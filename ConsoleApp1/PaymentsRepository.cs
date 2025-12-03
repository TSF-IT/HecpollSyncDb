using CsvHelper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System;
using System.Globalization;
using System.Threading;

internal sealed class PaymentSnapshot
{
    public int IdPayments { get; init; }
    public int TransactionsId { get; init; }
    public DateTime TransDateTime { get; init; }
    public int TransNumber { get; init; }
    public int TerminalsId { get; init; }
    public decimal? TransQuantity { get; init; }
    public decimal? TransAmount { get; init; }
    public decimal? TransAmountNet { get; init; }
    public decimal? TransAmountTax { get; init; }
    public decimal? TransTaxRate { get; init; }
    public string? TransArticleCode { get; init; }
    public string? TransArticleDescription { get; init; }
    public string? CurrencySymbol { get; init; }
    public string? CardPAN { get; init; }
    public string? VehicleLicensePlate { get; init; }
}

internal enum PaymentDryRunAction
{
    Insert,
    Update,
    NoOp
}

internal static class PaymentsRepository
{
    private static string _paymentsTableName = "dbo.PAYMENTS";

    public static async Task<int?> ResolveArticleIdFromMapAsync(
    SqlConnection conn,
    string? articleCode,
    string? articleDescription,
    decimal taxRate,
    CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(articleCode) || string.IsNullOrWhiteSpace(articleDescription))
            return null;

        const string sql = @"
SELECT TOP (1) ArticleId
FROM dbo.ArticleMapFromPayments
WHERE Code        = @code
  AND Description = @desc
  AND TaxRate     = @rate;";

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@code", articleCode);
        cmd.Parameters.AddWithValue("@desc", articleDescription);
        cmd.Parameters.AddWithValue("@rate", taxRate);

        var result = await cmd.ExecuteScalarAsync(ct);
        if (result == null || result == DBNull.Value)
            return null;

        return Convert.ToInt32(result, CultureInfo.InvariantCulture);
    }


    public static async Task<CardDriverVehicleMapRow?> GetCardVehicleMappingAsync(
    SqlConnection conn,
    string driverPanWithoutEquals,
    int transNumber,
    CancellationToken ct)
    {
        // On stocke DriverPan sans '=' dans la table, donc on normalise
        const string sql = @"
SELECT TOP (1)
       DriverPan,
       TransactionsID,
       TransNumber,
       TransDateTime,
       VehicleCardId,
       VehicleCardCustomer,
       VehicleCardNumber,
       VehicleCardPan,
       VehicleId,
       VehiclePlate,
       VehicleCustomersId,
       VehicleCustomerNumber,
       VehicleCustomerName,
       MandatorsId,
       MandatorNumber,
       MandatorDescription,
       VehicleContractId
FROM dbo.CardDriverVehicleMap
WHERE DriverPan   = @driverPan
  AND TransNumber = @transNumber;";

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@driverPan", driverPanWithoutEquals);
        cmd.Parameters.AddWithValue("@transNumber", transNumber);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return null;

        var driverPan = DbSafeReader.GetStringNullable(reader, 0);
        var transactionsId = DbSafeReader.GetIntNullable(reader, 1);
        var transNumberFromDb = DbSafeReader.GetIntNullable(reader, 2);
        var transDateTime = reader.GetDateTime(3);
        var vehicleCardId = DbSafeReader.GetIntNullable(reader, 4);
        var vehicleCardCustomer = DbSafeReader.GetStringNullable(reader, 5);
        var vehicleCardNumber = DbSafeReader.GetStringNullable(reader, 6);
        var vehicleCardPan = DbSafeReader.GetStringNullable(reader, 7);
        var vehicleId = DbSafeReader.GetIntNullable(reader, 8);
        var vehiclePlate = DbSafeReader.GetStringNullable(reader, 9);
        var vehicleCustomersId = DbSafeReader.GetIntNullable(reader, 10);
        var vehicleCustomerNumber = DbSafeReader.GetStringNullable(reader, 11);
        var vehicleCustomerName = DbSafeReader.GetStringNullable(reader, 12);
        var mandatorsId = DbSafeReader.GetIntNullable(reader, 13);
        var mandatorNumber = DbSafeReader.GetStringNullable(reader, 14);
        var mandatorDescription = DbSafeReader.GetStringNullable(reader, 15);
        var vehicleContractId = DbSafeReader.GetIntNullable(reader, 16);

        if (driverPan is null || transactionsId is null || transNumberFromDb is null || vehicleCardId is null
            || vehicleCardCustomer is null || vehicleCardNumber is null || vehicleCardPan is null)
        {
            Console.WriteLine("[DB-SAFE] CardDriverVehicleMap row incomplete.");
            return null;
        }

        return new CardDriverVehicleMapRow
        {
            DriverPan = driverPan,
            TransactionsId = transactionsId.Value,
            TransNumber = transNumberFromDb.Value,
            TransDateTime = transDateTime,
            VehicleCardId = vehicleCardId.Value,
            VehicleCardCustomer = vehicleCardCustomer,
            VehicleCardNumber = vehicleCardNumber,
            VehicleCardPan = vehicleCardPan,
            VehicleId = vehicleId,
            VehiclePlate = vehiclePlate,
            VehicleCustomersId = vehicleCustomersId,
            VehicleCustomerNumber = vehicleCustomerNumber,
            VehicleCustomerName = vehicleCustomerName,
            MandatorsId = mandatorsId,
            MandatorNumber = mandatorNumber,
            MandatorDescription = mandatorDescription,
            VehicleContractId = vehicleContractId,
        };
    }


    public static void Configure(IConfiguration configuration)
    {
        var name = configuration.GetSection("Import")["PaymentsTableName"];
        _paymentsTableName = string.IsNullOrWhiteSpace(name) ? "dbo.PAYMENTS" : name.Trim();
        Console.WriteLine($"[PAYMENTS-CONFIG] Using table '{_paymentsTableName}' as target for PAYMENTS.");
    }

    public static async Task<int?> GetTransactionIdAsync(
        SqlConnection conn,
        int transNumber,
        int terminalsId,
        DateTime transDateTime)
    {
        Console.WriteLine($"[PAYMENTS-LOOKUP] Searching transaction: TransNumber={transNumber}, TerminalsID={terminalsId}, Date={transDateTime}");

        const string sql = @"
SELECT TOP (1) ID_TRANSACTIONS
FROM dbo.TRANSACTIONS
WHERE TransNumber = @n
  AND TerminalsID = @t
  AND CONVERT(date, TransDateTime) = @d
ORDER BY TransDateTime DESC;";

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@n", transNumber);
        cmd.Parameters.AddWithValue("@t", terminalsId);
        cmd.Parameters.AddWithValue("@d", transDateTime.Date);

        var result = await cmd.ExecuteScalarAsync();
        if (result == null || result == DBNull.Value)
        {
            // Loose match on TransNumber + TerminalsID (ignore date) for debug
            const string sqlLoose = @"
SELECT TOP (1) ID_TRANSACTIONS, TransDateTime
FROM dbo.TRANSACTIONS
WHERE TransNumber = @n
  AND TerminalsID = @t
ORDER BY TransDateTime DESC;";

            await using (var cmdLoose = new SqlCommand(sqlLoose, conn))
            {
                cmdLoose.Parameters.AddWithValue("@n", transNumber);
                cmdLoose.Parameters.AddWithValue("@t", terminalsId);

                await using var reader = await cmdLoose.ExecuteReaderAsync();
                if (await reader.ReadAsync())
                {
                    var looseId = DbSafeReader.GetIntNullable(reader, 0);
                    var dbDate = reader.GetDateTime(1);
                    Console.WriteLine($"[PAYMENTS-LOOKUP] Loose match: TxID={looseId}, TxDate={dbDate}");
                }
                else
                {
                    Console.WriteLine($"[PAYMENTS-LOOKUP] NO MATCH for TransNumber={transNumber}, TerminalsID={terminalsId}, Date={transDateTime}");
                }
            }

            return null;
        }

        var idValue = result switch
        {
            int i => i,
            long l => (int)l,
            _ => Convert.ToInt32(result)
        };

        Console.WriteLine($"[PAYMENTS-LOOKUP] Found TxID={idValue}");
        return idValue;

    }

    public static async Task<PaymentDryRunAction> DryRunPaymentAsync(
        SqlConnection conn,
        int transactionsId,
        DateTime transDateTime,
        int transNumber,
        int terminalsId,
        string tenderCode,
        decimal? transQuantity,
        decimal? transSinglePriceInclSold,
        decimal? transAmount,
        decimal? transAmountNet,
        decimal? transAmountTax,
        decimal? transTaxRate,
        string? transArticleCode,
        string? transArticleDescription,
        int? transDeviceAddress,
        int? transSubDeviceAddress,
        string? currencySymbol,
        string? cardPan,
        string? cardCustomerNumber,
        string? cardNumber,
        int? cardTankNumber,
        int? cardsId,
        int? customersId,
        string? customerNumber,
        string? customerName,
        int? mandatorsId,
        string? mandatorNumber,
        string? mandatorDescription,
        int? vehiclesId,
        string? vehicleLicensePlate,
        CancellationToken cancellationToken)
    {
        Console.WriteLine(
            $"[PAYMENTS-DRYRUN] Candidate: TxId={transactionsId}, TransNumber={transNumber}, TerminalsID={terminalsId}, Tender={tenderCode}, Article={transArticleCode}, Device={transDeviceAddress}, SubDevice={transSubDeviceAddress}, Amount={transAmount}, Qty={transQuantity}, CardPAN={cardPan}, VehiclePlate={vehicleLicensePlate}");

        var existing = await GetExistingPaymentAsync(
            conn,
            transactionsId,
            transDateTime,
            transNumber,
            terminalsId,
            transArticleCode,
            transDeviceAddress,
            transSubDeviceAddress,
            transAmount,
            transQuantity,
            cancellationToken);

        if (existing is null)
        {
            Console.WriteLine("[PAYMENTS-DRYRUN] Action: INSERT (aucun paiement existant avec cette signature).");
            return PaymentDryRunAction.Insert;
        }

        Console.WriteLine(
            $"[PAYMENTS-DRYRUN-BEFORE] ID_PAYMENTS={existing.IdPayments}, Qty={existing.TransQuantity}, Amount={existing.TransAmount}, Net={existing.TransAmountNet}, Tax={existing.TransAmountTax}, Rate={existing.TransTaxRate}, ArticleDesc={existing.TransArticleDescription}, Currency={existing.CurrencySymbol}, PAN={existing.CardPAN}, Plate={existing.VehicleLicensePlate}");

        Console.WriteLine(
            $"[PAYMENTS-DRYRUN-AFTER ] Qty={transQuantity}, Amount={transAmount}, Net={transAmountNet}, Tax={transAmountTax}, Rate={transTaxRate}, ArticleDesc={transArticleDescription}, Currency={currencySymbol}, PAN={cardPan}, Plate={vehicleLicensePlate}");

        var wouldChange =
               existing.TransQuantity != transQuantity
            || existing.TransAmount != transAmount
            || existing.TransAmountNet != transAmountNet
            || existing.TransAmountTax != transAmountTax
            || existing.TransTaxRate != transTaxRate
            || existing.TransArticleDescription != transArticleDescription
            || existing.CurrencySymbol != currencySymbol
            || existing.CardPAN != cardPan;

        if (wouldChange)
        {
            Console.WriteLine($"[PAYMENTS-DRYRUN] Action: UPDATE sur ID_PAYMENTS={existing.IdPayments}.");
            return PaymentDryRunAction.Update;
        }
        else
        {
            Console.WriteLine($"[PAYMENTS-DRYRUN] Action: NO-OP (les données sont identiques, aucun UPDATE nécessaire) sur ID_PAYMENTS={existing.IdPayments}.");
            return PaymentDryRunAction.NoOp;
        }
    }

    private static async Task<PaymentSnapshot?> GetExistingPaymentAsync(
        SqlConnection conn,
        int transactionsId,
        DateTime transDateTime,
        int transNumber,
        int terminalsId,
        string? transArticleCode,
        int? transDeviceAddress,
        int? transSubDeviceAddress,
        decimal? transAmount,
        decimal? transQuantity,
        CancellationToken cancellationToken)
    {
        const string sqlTemplate = @"
SELECT TOP (1)
       ID_PAYMENTS,
       TransactionsID,
       TransDateTime,
       TransNumber,
       TerminalsID,
       TransQuantity,
       TransAmount,
       TransAmountNet,
       TransAmountTax,
       TransTaxRate,
       TransArticleCode,
       TransArticleDescription,
       CurrencySymbol,
       CardPAN,
       VehicleLicensePlate
FROM {0}
WHERE TransactionsID = @TransactionsID
  AND TransNumber = @TransNumber
  AND TerminalsID = @TerminalsID
  AND ABS(DATEDIFF(SECOND, TransDateTime, @TransDateTime)) <= 1
  AND ISNULL(TransArticleCode, '') = ISNULL(@TransArticleCode, '')
  AND ISNULL(TransDeviceAddress, -1) = ISNULL(@TransDeviceAddress, -1)
  AND ISNULL(TransSubDeviceAddress, -1) = ISNULL(@TransSubDeviceAddress, -1)
  AND ISNULL(CONVERT(decimal(18,4), TransAmount), 0) = ISNULL(CONVERT(decimal(18,4), @TransAmount), 0)
  AND ISNULL(CONVERT(decimal(18,4), TransQuantity), 0) = ISNULL(CONVERT(decimal(18,4), @TransQuantity), 0)
ORDER BY ID_PAYMENTS;";

        var sql = string.Format(sqlTemplate, _paymentsTableName);

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@TransactionsID", transactionsId);
        cmd.Parameters.AddWithValue("@TransNumber", transNumber);
        cmd.Parameters.AddWithValue("@TerminalsID", terminalsId);
        cmd.Parameters.AddWithValue("@TransDateTime", transDateTime);
        cmd.Parameters.AddWithValue("@TransArticleCode", (object?)transArticleCode ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@TransDeviceAddress", (object?)transDeviceAddress ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@TransSubDeviceAddress", (object?)transSubDeviceAddress ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@TransAmount", (object?)transAmount ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@TransQuantity", (object?)transQuantity ?? DBNull.Value);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return null;

        return new PaymentSnapshot
        {
            IdPayments = DbSafeReader.GetIntNullable(reader, 0) ?? 0,
            TransactionsId = DbSafeReader.GetIntNullable(reader, 1) ?? 0,
            TransDateTime = reader.GetDateTime(2),
            TransNumber = DbSafeReader.GetIntNullable(reader, 3) ?? 0,
            TerminalsId = DbSafeReader.GetIntNullable(reader, 4) ?? 0,
            TransQuantity = DbSafeReader.GetDecimalNullable(reader, 5),
            TransAmount = DbSafeReader.GetDecimalNullable(reader, 6),
            TransAmountNet = DbSafeReader.GetDecimalNullable(reader, 7),
            TransAmountTax = DbSafeReader.GetDecimalNullable(reader, 8),
            TransTaxRate = DbSafeReader.GetDecimalNullable(reader, 9),
            TransArticleCode = DbSafeReader.GetStringNullable(reader, 10),
            TransArticleDescription = DbSafeReader.GetStringNullable(reader, 11),
            CurrencySymbol = DbSafeReader.GetStringNullable(reader, 12),
            CardPAN = DbSafeReader.GetStringNullable(reader, 13),
            VehicleLicensePlate = DbSafeReader.GetStringNullable(reader, 14),
        };
    }

    public static async Task<int> UpsertPaymentAsync(
    SqlConnection conn,
    SqlTransaction? tx,
    int transactionsId,
    DateTime transDateTime,
    int transNumber,
    int terminalsId,
    string tenderCode,
    decimal? transQuantity,
    decimal? transSinglePriceInclSold,
    decimal? transAmount,
    decimal? transAmountNet,
    decimal? transAmountTax,
    decimal? transTaxRate,
    int articleId,
    string? transArticleCode,
    string? transArticleDescription,
    int? transDeviceAddress,
    int? transSubDeviceAddress,
    string? currencySymbol,
    string? cardPan,
    string? cardCustomerNumber,
    string? cardNumber,
    int? cardTankNumber,
    int? cardsId,
    int? customersId,
    string? customerNumber,
    string? customerName,
    int? mandatorsId,
    string? mandatorNumber,
    string? mandatorDescription,
    int? vehiclesId,
    string? vehicleLicensePlate)
    {
        Console.WriteLine(
            $"[PAYMENTS-MERGE] txId={transactionsId}, TransNumber={transNumber}, TerminalsID={terminalsId}, Tender={tenderCode}, Article={transArticleCode}, Device={transDeviceAddress}, SubDevice={transSubDeviceAddress}, Amount={transAmount}, Qty={transQuantity}");

        const string sqlTemplate = @"
MERGE {0} AS tgt
USING (VALUES(
        @TransactionsID,
        @TransDateTime,
        @TransNumber,
        @TerminalsID,
        @TenderCode,
        @TransArticleID,
        @TransArticleCode,
        @TransDeviceAddress,
        @TransSubDeviceAddress,
        @TransAmount,
        @TransQuantity))
      AS src(
        TransactionsID,
        TransDateTime,
        TransNumber,
        TerminalsID,
        TenderCode,
        TransArticleID,
        TransArticleCode,
        TransDeviceAddress,
        TransSubDeviceAddress,
        TransAmount,
        TransQuantity)
    ON (
        tgt.TransactionsID = src.TransactionsID
    )
WHEN MATCHED THEN
    UPDATE SET
        tgt.TransQuantity              = @TransQuantity,
        tgt.TransSinglePriceInclSold   = @TransSinglePriceInclSold,
        tgt.TransAmount                = @TransAmount,
        tgt.TransAmountNet             = @TransAmountNet,
        tgt.TransAmountTax             = @TransAmountTax,
        tgt.TransTaxRate               = @TransTaxRate,
        tgt.TransArticleID             = @TransArticleID,
        tgt.TransArticleCode           = @TransArticleCode,
        tgt.TransArticleDescription    = @TransArticleDescription,
        tgt.TransDeviceAddress         = @TransDeviceAddress,
        tgt.TransSubDeviceAddress      = @TransSubDeviceAddress,
        tgt.CurrencySymbol             = @CurrencySymbol,
        tgt.CardPAN                    = @CardPAN,
        tgt.CardsID                    = @CardsID,
        tgt.CardCustomerNumber         = @CardCustomerNumber,
        tgt.CardNumber                 = @CardNumber,
        tgt.CardTankNumber             = @CardTankNumber,
        tgt.CustomersID                = @CustomersID,
        tgt.CustomerNumber             = @CustomerNumber,
        tgt.CustomerName               = @CustomerName,
        tgt.MandatorsID                = @MandatorsID,
        tgt.MandatorNumber             = @MandatorNumber,
        tgt.MandatorDescription        = @MandatorDescription,
        tgt.VehiclesID                 = @VehiclesID,
        tgt.VehicleLicensePlate        = @VehicleLicensePlate
WHEN NOT MATCHED THEN
    INSERT (
            TransactionsID,
            TransDateTime,
            TransNumber,
            TerminalsID,
            TenderCode,
            TransQuantity,
            TransSinglePriceInclSold,
            TransAmount,
            TransAmountNet,
            TransAmountTax,
            TransTaxRate,
            TransArticleID,
            TransArticleCode,
            TransArticleDescription,
            TransDeviceAddress,
            TransSubDeviceAddress,
            CurrencySymbol,
            CardPAN,
            CardsID,
            CardCustomerNumber,
            CardNumber,
            CardTankNumber,
            CustomersID,
            CustomerNumber,
            CustomerName,
            MandatorsID,
            MandatorNumber,
            MandatorDescription,
            VehiclesID,
            VehicleLicensePlate,
            Number,
            ModifiedFlag,
            FleetImport)
    VALUES (
            @TransactionsID,
            @TransDateTime,
            @TransNumber,
            @TerminalsID,
            @TenderCode,
            @TransQuantity,
            @TransSinglePriceInclSold,
            @TransAmount,
            @TransAmountNet,
            @TransAmountTax,
            @TransTaxRate,
            @TransArticleID,
            @TransArticleCode,
            @TransArticleDescription,
            @TransDeviceAddress,
            @TransSubDeviceAddress,
            @CurrencySymbol,
            @CardPAN,
            @CardsID,
            @CardCustomerNumber,
            @CardNumber,
            @CardTankNumber,
            @CustomersID,
            @CustomerNumber,
            @CustomerName,
            @MandatorsID,
            @MandatorNumber,
            @MandatorDescription,
            @VehiclesID,
            @VehicleLicensePlate,
            DEFAULT,    -- Number
            DEFAULT,    -- ModifiedFlag
            DEFAULT);"; 

    var sql = string.Format(sqlTemplate, _paymentsTableName);

        await using var cmd = new SqlCommand(sql, conn);
        if (tx != null)
        {
            cmd.Transaction = tx;
        }

        cmd.Parameters.AddWithValue("@TransactionsID", transactionsId);
        cmd.Parameters.AddWithValue("@TransDateTime", transDateTime);
        cmd.Parameters.AddWithValue("@TransNumber", transNumber);
        cmd.Parameters.AddWithValue("@TerminalsID", terminalsId);
        cmd.Parameters.AddWithValue("@TenderCode", tenderCode);

        cmd.Parameters.AddWithValue("@TransArticleID", articleId);
        cmd.Parameters.AddWithValue("@TransArticleCode", (object?)transArticleCode ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@TransDeviceAddress", (object?)transDeviceAddress ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@TransSubDeviceAddress", (object?)transSubDeviceAddress ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@TransAmount", (object?)transAmount ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@TransQuantity", (object?)transQuantity ?? DBNull.Value);

        cmd.Parameters.AddWithValue("@TransSinglePriceInclSold", (object?)transSinglePriceInclSold ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@TransAmountNet", (object?)transAmountNet ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@TransAmountTax", (object?)transAmountTax ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@TransTaxRate", (object?)transTaxRate ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@TransArticleDescription", (object?)transArticleDescription ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@CurrencySymbol", (object?)currencySymbol ?? DBNull.Value);

        cmd.Parameters.AddWithValue("@CardPAN", (object?)cardPan ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@CardsID", (object?)cardsId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@CardCustomerNumber", (object?)cardCustomerNumber ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@CardNumber", (object?)cardNumber ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@CardTankNumber", (object?)cardTankNumber ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@CustomersID", (object?)customersId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@CustomerNumber", (object?)customerNumber ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@CustomerName", (object?)customerName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@MandatorsID", (object?)mandatorsId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@MandatorNumber", (object?)mandatorNumber ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@MandatorDescription", (object?)mandatorDescription ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@VehiclesID", (object?)vehiclesId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@VehicleLicensePlate", (object?)vehicleLicensePlate ?? DBNull.Value);

        var rows = await cmd.ExecuteNonQueryAsync();
        Console.WriteLine($"[PAYMENTS-MERGE] Rows affected = {rows}");
        return rows;
    }


    public static string ResolveTenderCode(IConfiguration cfg, CsvReader csv)
    {
        var mapping = cfg.GetSection("Import").GetSection("TenderMapping");
        var cardCode = mapping["Card"] ?? "UNKN";
        var cashCode = mapping["Cash"] ?? "UNKN";
        var voucherCode = mapping["Voucher"] ?? "UNKN";
        var unknownCode = mapping["Unknown"] ?? "UNKN";

        if (ReadBool(csv, "Payment_Card"))
            return cardCode;

        if (ReadBool(csv, "Payment_Cash"))
            return cashCode;

        if (ReadBool(csv, "Payment_Voucher"))
            return voucherCode;

        return unknownCode;
    }

    private static bool ReadBool(CsvReader csv, string fieldName)
    {
        try
        {
            var raw = csv.GetField(fieldName);
            if (string.IsNullOrWhiteSpace(raw))
                return false;

            if (bool.TryParse(raw, out var value))
                return value;

            return raw == "1";
        }
        catch
        {
            return false;
        }
    }
}

internal sealed class CardDriverVehicleMapRow
{
    public string DriverPan { get; init; } = default!;
    public int TransactionsId { get; init; }
    public int TransNumber { get; init; }
    public DateTime TransDateTime { get; init; }

    public int VehicleCardId { get; init; }
    public string VehicleCardCustomer { get; init; } = default!;
    public string VehicleCardNumber { get; init; } = default!;
    public string VehicleCardPan { get; init; } = default!;

    public int? VehicleId { get; init; }
    public string? VehiclePlate { get; init; }

    public int? VehicleCustomersId { get; init; }
    public string? VehicleCustomerNumber { get; init; }
    public string? VehicleCustomerName { get; init; }

    public int? MandatorsId { get; init; }
    public string? MandatorNumber { get; init; }
    public string? MandatorDescription { get; init; }

    public int? VehicleContractId { get; init; }
}
