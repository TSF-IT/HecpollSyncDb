using System.Data;
using Hecpoll.Sync;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

namespace Hecpoll.Sync;

/// <summary>
/// Logique d'accès / écriture des paiements et transactions shadow.
/// Toute la mécanique "je trouve la transaction, je la crée dans TRANSACTIONS_SHADOW,
/// j'enrichis la payment, etc." est centralisée ici.
/// </summary>
public static class PaymentsRepository
{
    private static IConfiguration? _configuration;

    public static void Configure(IConfiguration configuration)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
    }

    // -------------------------------------------------------------------------
    // API publique appelée depuis Program
    // -------------------------------------------------------------------------

    /// <summary>
    /// Mode dry-run : on ne modifie rien, mais on retourne une description de ce qui
    /// serait importé / mis à jour.
    /// </summary>
    public static async Task<DryRunResult> DryRunPaymentAsync(
        SqlConnection connection,
        LegacyPaymentCsvRow csvRow,
        bool backfillMode,
        CancellationToken cancellationToken = default)
    {
        var paymentType = DeterminePaymentType(csvRow);

        // On essaie de retrouver la transaction shadow
        var transactionId = await GetOrCreateTransactionShadowAsync(connection, csvRow, paymentType, dryRun: true, cancellationToken);

        // On enrichit depuis la base legacy si besoin
        var enrichment = await EnrichLegacyPaymentWithCardInfoAsync(connection, csvRow, cancellationToken);

        var description = $"[{paymentType}] TxId={transactionId?.ToString() ?? "null"} " +
                          $"TermID={csvRow.TerminalsID} Num={csvRow.TransNumber} " +
                          $"Quantité={csvRow.TransQuantity} Pan={csvRow.CardPAN ?? enrichment?.CardPan ?? "(?)"}";

        return new DryRunResult
        {
            ShouldImport = transactionId.HasValue,
            Description = description
        };
    }

    /// <summary>
    /// Mode réel : applique les changements dans TRANSACTIONS_SHADOW et PAYMENTS_SHADOW.
    /// </summary>
    public static async Task<bool> ApplyPaymentFromCsvAsync(
        SqlConnection connection,
        LegacyPaymentCsvRow csvRow,
        bool backfillMode,
        CancellationToken cancellationToken = default)
    {
        var tx = (SqlTransaction)await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);

        try
        {
            var paymentType = DeterminePaymentType(csvRow);

            var transactionId = await GetOrCreateTransactionShadowAsync(connection, csvRow, paymentType, dryRun: false, cancellationToken);
            if (!transactionId.HasValue)
            {
                await tx.RollbackAsync(cancellationToken);
                return false;
            }

            var enrichment = await EnrichLegacyPaymentWithCardInfoAsync(connection, csvRow, cancellationToken);

            await InsertOrUpdatePaymentShadowAsync(connection, tx, csvRow, transactionId.Value, paymentType, enrichment, backfillMode, cancellationToken);

            await tx.CommitAsync(cancellationToken);
            return true;
        }
        catch
        {
            await tx.RollbackAsync(cancellationToken);
            throw;
        }
    }

    // -------------------------------------------------------------------------
    // Déduction du type de paiement (mode legacy / SaaS, etc.)
    // -------------------------------------------------------------------------

    private static PaymentType DeterminePaymentType(LegacyPaymentCsvRow row)
    {
        // Si le CSV donne un hint (PaymentTypeHint), on l'utilise
        if (!string.IsNullOrWhiteSpace(row.PaymentTypeHint))
        {
            var hint = row.PaymentTypeHint.Trim().ToUpperInvariant();
            return hint switch
            {
                "LEGACY" => PaymentType.Legacy,
                "BACKFILL" => PaymentType.Backfill,
                "SAAS" => PaymentType.Saas,
                _ => PaymentType.Unknown
            };
        }

        // Fallback : heuristiques simples
        if (row.TransDateTime is { } dt && dt < new DateTime(2025, 9, 25))
            return PaymentType.Legacy;

        if (row.TransDateTime is { } dt2 && dt2 >= new DateTime(2025, 9, 25))
            return PaymentType.Saas;

        return PaymentType.Unknown;
    }

    // -------------------------------------------------------------------------
    // Transactions shadow – création / récupération
    // -------------------------------------------------------------------------

    /// <summary>
    /// Retrouve (ou crée) une ligne dans TRANSACTIONS_SHADOW correspondant à la transaction
    /// décrite par le CSV.
    /// </summary>
    public static async Task<int?> GetOrCreateTransactionShadowAsync(
        SqlConnection connection,
        LegacyPaymentCsvRow csvRow,
        PaymentType paymentType,
        bool dryRun,
        CancellationToken cancellationToken = default)
    {
        if (!csvRow.TransDateTime.HasValue || !csvRow.TransNumber.HasValue || !csvRow.TerminalsID.HasValue)
            return null;

        var transDateTime = csvRow.TransDateTime.Value;
        var transNumber = csvRow.TransNumber.Value;
        var terminalsId = csvRow.TerminalsID.Value;

        // 1) On cherche la transaction_shadow existante
        const string selectSql = @"
SELECT TransactionsID
FROM dbo.TRANSACTIONS_SHADOW
WHERE TransDateTime   = @TransDateTime
  AND TransNumber     = @TransNumber
  AND TerminalsID     = @TerminalsID;";

        await using (var selectCmd = new SqlCommand(selectSql, connection))
        {
            selectCmd.Parameters.AddWithValue("@TransDateTime", transDateTime);
            selectCmd.Parameters.AddWithValue("@TransNumber", transNumber);
            selectCmd.Parameters.AddWithValue("@TerminalsID", terminalsId);

            var existingId = await selectCmd.ExecuteScalarAsync(cancellationToken);
            if (existingId is int id)
                return id;
        }

        if (dryRun)
        {
            // En dry-run, on ne crée rien, mais on signale que ça n'existe pas
            return null;
        }

        // 2) Sinon, on va chercher dans TRANSACTIONS legacy, pour recopier la ligne
        const string legacySql = @"
SELECT TOP(1) *
FROM dbo.TRANSACTIONS
WHERE TransDateTime = @TransDateTime
  AND TransNumber   = @TransNumber
  AND TerminalsID   = @TerminalsID;";

        await using var legacyCmd = new SqlCommand(legacySql, connection);
        legacyCmd.Parameters.AddWithValue("@TransDateTime", transDateTime);
        legacyCmd.Parameters.AddWithValue("@TransNumber", transNumber);
        legacyCmd.Parameters.AddWithValue("@TerminalsID", terminalsId);

        await using var reader = await legacyCmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            // Pas de transaction legacy correspondante : on ne fait rien
            return null;
        }

        // On crée la transaction_shadow à partir de la transaction legacy
        const string insertSql = @"
INSERT INTO dbo.TRANSACTIONS_SHADOW
(
    TransDateTime,
    TransNumber,
    TerminalsID,
    TerminalsStationCode,
    TerminalNumber,
    TransStatus,
    TransWasExported,
    TransPollDateTime
    -- autres colonnes si nécessaire
)
OUTPUT INSERTED.TransactionsID
VALUES
(
    @TransDateTime,
    @TransNumber,
    @TerminalsID,
    @TerminalsStationCode,
    @TerminalNumber,
    @TransStatus,
    @TransWasExported,
    @TransPollDateTime
);";

        var termStationCode = reader["TerminalsStationCode"] as int? ?? 0;
        var terminalNumber = reader["TerminalNumber"] as int? ?? 0;
        var transStatus = reader["TransStatus"] as int? ?? 0;
        var transWasExported = reader["TransWasExported"] as string ?? "N";
        var transPollDateTime = reader["TransPollDateTime"] as DateTime? ?? transDateTime;

        await reader.CloseAsync();

        await using var insertCmd = new SqlCommand(insertSql, connection);
        insertCmd.Parameters.AddWithValue("@TransDateTime", transDateTime);
        insertCmd.Parameters.AddWithValue("@TransNumber", transNumber);
        insertCmd.Parameters.AddWithValue("@TerminalsID", terminalsId);
        insertCmd.Parameters.AddWithValue("@TerminalsStationCode", termStationCode);
        insertCmd.Parameters.AddWithValue("@TerminalNumber", terminalNumber);
        insertCmd.Parameters.AddWithValue("@TransStatus", transStatus);
        insertCmd.Parameters.AddWithValue("@TransWasExported", transWasExported);
        insertCmd.Parameters.AddWithValue("@TransPollDateTime", transPollDateTime);

        var newIdObj = await insertCmd.ExecuteScalarAsync(cancellationToken);
        return newIdObj as int?;
    }

    // -------------------------------------------------------------------------
    // Enrichissement des paiements (CARDS / VEHICLES / EMPLOYEES)
    // -------------------------------------------------------------------------

    public static async Task<PaymentEnrichment?> EnrichLegacyPaymentWithCardInfoAsync(
        SqlConnection connection,
        LegacyPaymentCsvRow row,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(row.CardPAN))
            return null;

        var cardInfo = await CardEnrichmentService.GetCardVehicleMappingAsync(connection, transaction: null, row.CardPAN!, cancellationToken);
        if (cardInfo == null)
            return null;

        return new PaymentEnrichment
        {
            CardPan = row.CardPAN,
            CardsId = cardInfo.CardsId,
            CardCustomerNumber = cardInfo.CardCustomerNumber,
            CardNumber = cardInfo.CardNumber,
            CardExtNumber = cardInfo.CardExtNumber,
            CardSystem = cardInfo.CardSystem,
            CardTankNumber = cardInfo.CardTankNumber,
            CardLimit = cardInfo.CardLimit,
            CardOnHand = cardInfo.CardOnHand,
            CardValidFrom = cardInfo.CardValidFrom,
            CardValidTo = cardInfo.CardValidTo,
            VehiclesId = cardInfo.VehiclesId,
            VehicleLicensePlate = cardInfo.VehicleLicensePlate,
            EmployeesId = cardInfo.EmployeesId,
            EmployeeNumber = cardInfo.EmployeeNumber,
            EmployeeName = cardInfo.EmployeeName
        };
    }

    // -------------------------------------------------------------------------
    // Insertion / mise à jour de PAYMENTS_SHADOW
    // -------------------------------------------------------------------------

    private static async Task InsertOrUpdatePaymentShadowAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        LegacyPaymentCsvRow row,
        int transactionsIdShadow,
        PaymentType paymentType,
        PaymentEnrichment? enrichment,
        bool backfillMode,
        CancellationToken cancellationToken)
    {
        // On regarde si un paiement shadow existe déjà pour cette transaction
        const string selectSql = @"
SELECT ID_PAYMENTS
FROM dbo.PAYMENTS_SHADOW
WHERE TransactionsID = @TransactionsID;";

        int? existingPaymentId = null;

        await using (var selectCmd = new SqlCommand(selectSql, connection, transaction))
        {
            selectCmd.Parameters.AddWithValue("@TransactionsID", transactionsIdShadow);
            var scalar = await selectCmd.ExecuteScalarAsync(cancellationToken);
            if (scalar is int id)
                existingPaymentId = id;
        }

        if (existingPaymentId.HasValue && !backfillMode)
        {
            // Déjà présent, et on n'est pas en backfill => on ne touche pas
            return;
        }

        // Construction des valeurs à insérer / mettre à jour
        var transDateTime = row.TransDateTime ?? DateTime.MinValue;
        var transNumber = row.TransNumber ?? 0;
        var terminalsId = row.TerminalsID ?? 0;

        var transQuantity = row.TransQuantity ?? 0m;
        var transSinglePrice = row.TransSinglePriceInclSold ?? 0m;
        var transAmount = row.TransAmount ?? 0m;
        var transAmountNet = row.TransAmountNet ?? 0m;
        var transAmountTax = row.TransAmountTax ?? 0m;
        var transTaxRate = row.TransTaxRate ?? 0m;

        var pan = row.CardPAN ?? enrichment?.CardPan;
        var cardLimit = enrichment?.CardLimit ?? 0m;
        var cardOnHand = enrichment?.CardOnHand ?? 0m;

        if (existingPaymentId.HasValue)
        {
            // UPDATE
            const string updateSql = @"
UPDATE dbo.PAYMENTS_SHADOW
SET TransDateTime                = @TransDateTime,
    TransNumber                  = @TransNumber,
    TerminalsID                  = @TerminalsID,
    TransQuantity                = @TransQuantity,
    TransSinglePriceInclSold     = @TransSinglePriceInclSold,
    TransAmount                  = @TransAmount,
    TransAmountNet               = @TransAmountNet,
    TransAmountTax               = @TransAmountTax,
    TransTaxRate                 = @TransTaxRate,
    CardPAN                      = @CardPAN,
    CardLimit                    = @CardLimit,
    CardOnHand                   = @CardOnHand,
    LastChangedDateTime          = SYSUTCDATETIME(),
    LastChangedByUser            = N'HecpollSyncDb'
WHERE ID_PAYMENTS = @Id;";

            await using var updateCmd = new SqlCommand(updateSql, connection, transaction);
            updateCmd.Parameters.AddWithValue("@Id", existingPaymentId.Value);
            updateCmd.Parameters.AddWithValue("@TransDateTime", transDateTime);
            updateCmd.Parameters.AddWithValue("@TransNumber", transNumber);
            updateCmd.Parameters.AddWithValue("@TerminalsID", terminalsId);
            updateCmd.Parameters.AddWithValue("@TransQuantity", transQuantity);
            updateCmd.Parameters.AddWithValue("@TransSinglePriceInclSold", transSinglePrice);
            updateCmd.Parameters.AddWithValue("@TransAmount", transAmount);
            updateCmd.Parameters.AddWithValue("@TransAmountNet", transAmountNet);
            updateCmd.Parameters.AddWithValue("@TransAmountTax", transAmountTax);
            updateCmd.Parameters.AddWithValue("@TransTaxRate", transTaxRate);
            updateCmd.Parameters.AddWithValue("@CardPAN", (object?)pan ?? DBNull.Value);
            updateCmd.Parameters.AddWithValue("@CardLimit", cardLimit);
            updateCmd.Parameters.AddWithValue("@CardOnHand", cardOnHand);

            await updateCmd.ExecuteNonQueryAsync(cancellationToken);
        }
        else
        {
            // INSERT
            const string insertSql = @"
INSERT INTO dbo.PAYMENTS_SHADOW
(
    TransactionsID,
    TransDateTime,
    TransNumber,
    TerminalsID,
    TransQuantity,
    TransSinglePriceInclSold,
    TransAmount,
    TransAmountNet,
    TransAmountTax,
    TransTaxRate,
    CardPAN,
    CardLimit,
    CardOnHand,
    LastChangedDateTime,
    LastChangedByUser
)
VALUES
(
    @TransactionsID,
    @TransDateTime,
    @TransNumber,
    @TerminalsID,
    @TransQuantity,
    @TransSinglePriceInclSold,
    @TransAmount,
    @TransAmountNet,
    @TransAmountTax,
    @TransTaxRate,
    @CardPAN,
    @CardLimit,
    @CardOnHand,
    SYSUTCDATETIME(),
    N'HecpollSyncDb'
);";

            await using var insertCmd = new SqlCommand(insertSql, connection, transaction);
            insertCmd.Parameters.AddWithValue("@TransactionsID", transactionsIdShadow);
            insertCmd.Parameters.AddWithValue("@TransDateTime", transDateTime);
            insertCmd.Parameters.AddWithValue("@TransNumber", transNumber);
            insertCmd.Parameters.AddWithValue("@TerminalsID", terminalsId);
            insertCmd.Parameters.AddWithValue("@TransQuantity", transQuantity);
            insertCmd.Parameters.AddWithValue("@TransSinglePriceInclSold", transSinglePrice);
            insertCmd.Parameters.AddWithValue("@TransAmount", transAmount);
            insertCmd.Parameters.AddWithValue("@TransAmountNet", transAmountNet);
            insertCmd.Parameters.AddWithValue("@TransAmountTax", transAmountTax);
            insertCmd.Parameters.AddWithValue("@TransTaxRate", transTaxRate);
            insertCmd.Parameters.AddWithValue("@CardPAN", (object?)pan ?? DBNull.Value);
            insertCmd.Parameters.AddWithValue("@CardLimit", cardLimit);
            insertCmd.Parameters.AddWithValue("@CardOnHand", cardOnHand);

            await insertCmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }
}

// -----------------------------------------------------------------------------
// Types utilitaires
// -----------------------------------------------------------------------------

public enum PaymentType
{
    Unknown = 0,
    Legacy = 1,
    Backfill = 2,
    Saas = 3
}

public sealed class DryRunResult
{
    public bool ShouldImport { get; set; }
    public string? Description { get; set; }
}

public sealed class PaymentEnrichment
{
    public string? CardPan { get; set; }
    public int? CardsId { get; set; }
    public string? CardCustomerNumber { get; set; }
    public string? CardNumber { get; set; }
    public string? CardExtNumber { get; set; }
    public string? CardSystem { get; set; }
    public string? CardTankNumber { get; set; }
    public decimal CardLimit { get; set; }
    public decimal CardOnHand { get; set; }
    public DateTime? CardValidFrom { get; set; }
    public DateTime? CardValidTo { get; set; }

    public int? VehiclesId { get; set; }
    public string? VehicleLicensePlate { get; set; }

    public int? EmployeesId { get; set; }
    public string? EmployeeNumber { get; set; }
    public string? EmployeeName { get; set; }
}
