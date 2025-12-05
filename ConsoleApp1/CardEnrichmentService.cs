using System.Collections.Concurrent;
using Microsoft.Data.SqlClient;

/// <summary>
/// Service d'enrichissement des cartes / véhicules / conducteurs.
/// L'idée est d'éviter de recalculer à chaque ligne et de centraliser la logique
/// sur la manière de retrouver les infos manquantes à partir des tables legacy.
/// </summary>
public static class CardEnrichmentService
{
    // Cache en mémoire pour éviter de requêter la base en boucle
    private static readonly ConcurrentDictionary<string, CardVehicleInfo> _cardVehicleCache = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Récupère (ou calcule) les infos véhicule associées à un PAN de carte.
    /// </summary>
    public static async Task<CardVehicleInfo?> GetCardVehicleMappingAsync(SqlConnection connection, SqlTransaction? transaction, string cardPan, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(cardPan))
            return null;

        if (_cardVehicleCache.TryGetValue(cardPan, out var cached))
            return cached;

        const string sql = @"
SELECT TOP (1)
       c.CardsID,
       c.CardCustomerNumber,
       c.CardNumber,
       c.CardExtNumber,
       c.CardSystem,
       c.CardTankNumber,
       c.CardLimit,
       c.CardOnHand,
       c.CardValidFrom,
       c.CardValidTo,
       v.VehiclesID,
       v.VehicleLicensePlate,
       e.EmployeesID,
       e.EmployeeNumber,
       e.EmployeeName
FROM dbo.CARDS c
LEFT JOIN dbo.VEHICLES v ON v.CardsID = c.CardsID
LEFT JOIN dbo.EMPLOYEES e ON e.CardsID = c.CardsID
WHERE c.CardPAN = @CardPAN
ORDER BY c.CardValidFrom DESC";

        await using var cmd = new SqlCommand(sql, connection, transaction)
        {
            CommandType = System.Data.CommandType.Text
        };
        cmd.Parameters.AddWithValue("@CardPAN", cardPan);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return null;

        var info = new CardVehicleInfo
        {
            CardsId = reader["CardsID"] as int?,
            CardCustomerNumber = reader["CardCustomerNumber"] as string,
            CardNumber = reader["CardNumber"] as string,
            CardExtNumber = reader["CardExtNumber"] as string,
            CardSystem = reader["CardSystem"] as string,
            CardTankNumber = reader["CardTankNumber"] as string,
            CardLimit = reader["CardLimit"] as decimal? ?? 0m,
            CardOnHand = reader["CardOnHand"] as decimal? ?? 0m,
            CardValidFrom = reader["CardValidFrom"] as DateTime?,
            CardValidTo = reader["CardValidTo"] as DateTime?,
            VehiclesId = reader["VehiclesID"] as int?,
            VehicleLicensePlate = reader["VehicleLicensePlate"] as string,
            EmployeesId = reader["EmployeesID"] as int?,
            EmployeeNumber = reader["EmployeeNumber"] as string,
            EmployeeName = reader["EmployeeName"] as string
        };

        _cardVehicleCache[cardPan] = info;
        return info;
    }

    /// <summary>
    /// Utilisé en fin de traitement pour voir un peu l'état du cache et comprendre
    /// combien de cartes / véhicules différents ont été rencontrés.
    /// </summary>
    public static void DumpCacheState()
    {
        Console.WriteLine("=== CardEnrichmentService – État du cache ===");
        Console.WriteLine($"Entrées cache carte/véhicule : {_cardVehicleCache.Count}");
    }
}

/// <summary>
/// Modèle des infos enrichies sur la carte / véhicule / conducteur.
/// </summary>
public sealed class CardVehicleInfo
{
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
