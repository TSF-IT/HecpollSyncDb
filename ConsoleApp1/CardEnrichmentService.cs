using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

internal static class DbSafeReader
{
    public static int? GetIntNullable(SqlDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal)) return null;
        var value = reader.GetValue(ordinal);
        try
        {
            return Convert.ToInt32(value, System.Globalization.CultureInfo.InvariantCulture);
        }
        catch
        {
            Console.WriteLine($"[DB-SAFE] Impossible de convertir en int la valeur '{value}' (index {ordinal}, type {value?.GetType().Name}).");
            return null;
        }
    }

    public static decimal? GetDecimalNullable(SqlDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal)) return null;
        var value = reader.GetValue(ordinal);
        try
        {
            return Convert.ToDecimal(value, System.Globalization.CultureInfo.InvariantCulture);
        }
        catch
        {
            Console.WriteLine($"[DB-SAFE] Impossible de convertir en decimal la valeur '{value}' (index {ordinal}, type {value?.GetType().Name}).");
            return null;
        }
    }

    public static string? GetStringNullable(SqlDataReader reader, int ordinal)
    {
        return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
    }
}

internal sealed class CardEnrichment
{
    public int? CardsId { get; init; }
    public string? CardCustomerNumber { get; init; }
    public string? CardNumber { get; init; }
    public string? CardPanResolved { get; init; }
    public int? CardTankNumber { get; init; }

    public int? CustomersId { get; init; }
    public string? CustomerNumber { get; init; }
    public string? CustomerName { get; init; }

    public int? MandatorsId { get; init; }
    public string? MandatorNumber { get; init; }
    public string? MandatorDescription { get; init; }

    public int? VehiclesId { get; init; }
    public string? VehicleLicensePlate { get; init; }
}

internal static class CardEnrichmentService
{
    public static async Task<CardEnrichment?> EnrichAsync(
        SqlConnection conn,
        string? cardToken,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(cardToken))
            return null;

        const string sql = @"
SELECT TOP (1)
       c.ID_CARDS,
       c.Customer,              -- CardCustomerNumber
       c.Number,                -- CardNumber
       c.PAN,                   -- Card PAN/token
       c.TankNumber,            -- Card tank
       cu.ID_CUSTOMERS,
       cu.Number       AS CustomerNumber,
       COALESCE(cu.Company, '') + ' ' + COALESCE(cu.Lastname, '') AS CustomerName,
       m.ID_MANDATORS,
       m.Number       AS MandatorNumber,
       m.Description  AS MandatorDescription,
       v.ID_VEHICLES,
       v.LicensePlate
FROM dbo.CARDS c
LEFT JOIN dbo.CUSTOMERS cu ON cu.ID_CUSTOMERS = c.CustomersID
LEFT JOIN dbo.MANDATORS m   ON m.ID_MANDATORS   = cu.MandatorsID
LEFT JOIN dbo.VEHICLES v    ON v.ID_VEHICLES    = c.VehiclesID
WHERE c.PAN = @pan OR c.Number = @cardNumber
ORDER BY c.ID_CARDS;";

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@pan", (object)cardToken ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@cardNumber", (object)cardToken ?? DBNull.Value);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return null;

        var cardsId            = DbSafeReader.GetIntNullable(reader, 0);
        var cardCustomerNumber = DbSafeReader.GetStringNullable(reader, 1);
        var cardNumber         = DbSafeReader.GetStringNullable(reader, 2);
        var cardPanToken       = DbSafeReader.GetStringNullable(reader, 3);
        var cardTankNumber     = DbSafeReader.GetIntNullable(reader, 4);

        var customersId        = DbSafeReader.GetIntNullable(reader, 5);
        var customerNumber     = DbSafeReader.GetStringNullable(reader, 6);
        var customerName       = DbSafeReader.GetStringNullable(reader, 7);

        var mandatorsId        = DbSafeReader.GetIntNullable(reader, 8);
        var mandatorNumber     = DbSafeReader.GetStringNullable(reader, 9);
        var mandatorDesc       = DbSafeReader.GetStringNullable(reader, 10);

        var vehiclesId         = DbSafeReader.GetIntNullable(reader, 11);
        var licensePlate       = DbSafeReader.GetStringNullable(reader, 12);

        // Résolution du CardPAN : priorité à la plaque si connue, sinon token
        string? cardPanResolved = null;
        if (!string.IsNullOrWhiteSpace(licensePlate))
        {
            var compact = licensePlate.Replace(" ", string.Empty).ToUpperInvariant();
            cardPanResolved = compact.EndsWith("=") ? compact : compact + "=";
        }
        else if (!string.IsNullOrWhiteSpace(cardPanToken))
        {
            var compact = cardPanToken.Trim();
            cardPanResolved = compact.EndsWith("=") ? compact : compact + "=";
        }

        Console.WriteLine(
            $"[CARD-ENRICH] PAN={cardPanToken}, Token={cardToken}, ResolvedPAN={cardPanResolved}, Plate={licensePlate}, CardId={cardsId}, Cust={customerNumber}, Mand={mandatorNumber}");

        return new CardEnrichment
        {
            CardsId             = cardsId,
            CardCustomerNumber  = cardCustomerNumber,
            CardNumber          = cardNumber,
            CardPanResolved     = cardPanResolved,
            CardTankNumber      = cardTankNumber,
            CustomersId         = customersId,
            CustomerNumber      = customerNumber,
            CustomerName        = customerName,
            MandatorsId         = mandatorsId,
            MandatorNumber      = mandatorNumber,
            MandatorDescription = mandatorDesc,
            VehiclesId          = vehiclesId,
            VehicleLicensePlate = licensePlate
        };
    }

    // Enrichissement via la table de map CardDriverVehicleMap (scénario backfill)
    public static async Task<CardEnrichment?> EnrichFromCardMapAsync(
        SqlConnection conn,
        int transactionsId,
        int transNumber,
        CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT TOP (1)
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
       MandatorDescription
FROM dbo.CardDriverVehicleMap
WHERE TransactionsID = @txId
  AND TransNumber = @transNumber;";

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@txId", transactionsId);
        cmd.Parameters.AddWithValue("@transNumber", transNumber);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return null;

        return new CardEnrichment
        {
            CardsId             = DbSafeReader.GetIntNullable(reader, 0),
            CardCustomerNumber  = DbSafeReader.GetStringNullable(reader, 1),
            CardNumber          = DbSafeReader.GetStringNullable(reader, 2),
            CardPanResolved     = DbSafeReader.GetStringNullable(reader, 3),
            VehiclesId          = DbSafeReader.GetIntNullable(reader, 4),
            VehicleLicensePlate = DbSafeReader.GetStringNullable(reader, 5),
            CustomersId         = DbSafeReader.GetIntNullable(reader, 6),
            CustomerNumber      = DbSafeReader.GetStringNullable(reader, 7),
            CustomerName        = DbSafeReader.GetStringNullable(reader, 8),
            MandatorsId         = DbSafeReader.GetIntNullable(reader, 9),
            MandatorNumber      = DbSafeReader.GetStringNullable(reader, 10),
            MandatorDescription = DbSafeReader.GetStringNullable(reader, 11),
            CardTankNumber      = null
        };
    }
}
