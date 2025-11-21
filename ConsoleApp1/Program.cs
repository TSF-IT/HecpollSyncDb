using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

internal class Program
{
    private sealed record TerminalInfo(int Id, int StationsId);

    private static async Task Main(string[] args)
    {
        Console.OutputEncoding = Encoding.UTF8;

        var config = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();

        var importSection = config.GetSection("Import");

        var watchRoot = importSection["WatchDirectory"]
                        ?? throw new InvalidOperationException("Import:WatchDirectory manquant dans appsettings.json");

        var inFolderName = importSection["InFolder"] ?? "";
        var processingFolderName = importSection["ProcessingFolder"] ?? "processing";
        var archiveFolderName = importSection["ArchiveFolder"] ?? "archive";
        var errorFolderName = importSection["ErrorFolder"] ?? "error";
        var pattern = importSection["FileSearchPattern"] ?? "*";
        var logRoot = importSection["LogDirectory"] ?? Path.Combine(AppContext.BaseDirectory, "logs");

        var pollingSeconds = 0;
        if (!int.TryParse(importSection["PollingIntervalSeconds"], NumberStyles.Integer, CultureInfo.InvariantCulture, out pollingSeconds))
        {
            pollingSeconds = 300;
        }

        // Mode dry-run : true = aucune écriture en base, on log seulement
        var dryRun = false;

        var connectionString = config.GetConnectionString("Hecpoll")
                               ?? throw new InvalidOperationException("Connection string 'Hecpoll' manquante dans appsettings.json.");

        var inFolder = Path.Combine(watchRoot, inFolderName);
        var processingFolder = Path.Combine(watchRoot, processingFolderName);
        var archiveFolder = Path.Combine(watchRoot, archiveFolderName);
        var errorFolder = Path.Combine(watchRoot, errorFolderName);

        Directory.CreateDirectory(inFolder);
        Directory.CreateDirectory(processingFolder);
        Directory.CreateDirectory(archiveFolder);
        Directory.CreateDirectory(errorFolder);
        Directory.CreateDirectory(logRoot);

        Console.WriteLine("=== Hecpoll Sync démarré ===");
        Console.WriteLine($"Répertoire racine : {watchRoot}");
        Console.WriteLine($"BDD cible : {connectionString}");
        Console.WriteLine($"Mode dry-run : {(dryRun ? "OUI (aucune écriture en base)" : "NON (insertion réelle)")}");

        while (true)
        {
            try
            {
                await ProcessPendingFilesAsync(
                    inFolder,
                    processingFolder,
                    archiveFolder,
                    errorFolder,
                    logRoot,
                    pattern,
                    connectionString,
                    dryRun);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERREUR GLOBALE] {ex}");
            }

            if (pollingSeconds <= 0)
            {
                // Mode one-shot (tâche planifiée)
                break;
            }

            await Task.Delay(TimeSpan.FromSeconds(pollingSeconds));
        }

        Console.WriteLine("=== Hecpoll Sync terminé ===");
    }

    private static async Task ProcessPendingFilesAsync(
        string inFolder,
        string processingFolder,
        string archiveFolder,
        string errorFolder,
        string logRoot,
        string pattern,
        string connectionString,
        bool dryRun)
    {
        var files = Directory.EnumerateFiles(inFolder, pattern).ToList();
        if (!files.Any())
        {
            Console.WriteLine("[INFO] Aucun fichier en attente.");
            return;
        }

        foreach (var file in files)
        {
            var fileName = Path.GetFileName(file);
            var processingPath = Path.Combine(processingFolder, fileName);
            var archivePath = Path.Combine(archiveFolder, fileName);
            var errorPath = Path.Combine(errorFolder, fileName);
            var logFileName = $"{fileName}_{DateTime.Now:yyyyMMdd_HHmmss}.log";
            var logPath = Path.Combine(logRoot, logFileName);

            Console.WriteLine($"[INFO] Début traitement fichier : {fileName}");
            Console.WriteLine($"[INFO] Log : {logPath}");

            using var log = new StreamWriter(logPath, append: false, Encoding.UTF8);
            log.WriteLine($"=== Hecpoll Sync - Fichier {fileName} ===");
            log.WriteLine($"Date de traitement : {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            log.WriteLine($"Mode dry-run : {(dryRun ? "OUI" : "NON")}");
            log.WriteLine();

            try
            {
                File.Move(file, processingPath, overwrite: true);

                var importedCount = await ImportCsvFileAsync(
                    processingPath,
                    connectionString,
                    dryRun,
                    log);

                var label = dryRun
                    ? "lignes qui seraient importées (dry-run)"
                    : "lignes importées";

                Console.WriteLine($"[OK] Fichier {fileName} : {importedCount} {label}.");
                log.WriteLine();
                log.WriteLine($"[OK] Fichier {fileName} : {importedCount} {label}.");

                File.Move(processingPath, archivePath, overwrite: true);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERREUR] Fichier {fileName} : {ex.Message}");
                Console.WriteLine(ex);

                log.WriteLine();
                log.WriteLine($"[ERREUR] Fichier {fileName} : {ex}");
                log.Flush();

                try
                {
                    if (File.Exists(processingPath))
                    {
                        File.Move(processingPath, errorPath, overwrite: true);
                    }
                    else if (File.Exists(file))
                    {
                        File.Move(file, errorPath, overwrite: true);
                    }
                }
                catch (Exception moveEx)
                {
                    Console.WriteLine($"[ERREUR] Impossible de déplacer le fichier en erreur : {moveEx.Message}");
                    log.WriteLine($"[ERREUR] Impossible de déplacer le fichier en erreur : {moveEx.Message}");
                }
            }
        }
    }

    private static async Task<int> ImportCsvFileAsync(
        string csvPath,
        string connectionString,
        bool dryRun,
        TextWriter log)
    {
        var imported = 0;
        var alreadyExisting = 0;
        var ignored = 0;
        var skippedTooOld = 0;

        var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            Delimiter = ";",
            HasHeaderRecord = true,
            BadDataFound = null,
            MissingFieldFound = null,
            TrimOptions = TrimOptions.Trim,
            Encoding = Encoding.UTF8
        };

        await using var stream = File.OpenRead(csvPath);
        using var reader = new StreamReader(stream, Encoding.UTF8);
        using var csv = new CsvReader(reader, csvConfig);

        await csv.ReadAsync();
        csv.ReadHeader();

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        // Dernière transaction déjà en base
        var lastImported = await GetLastImportedTransDateTimeAsync(connection);
        if (lastImported.HasValue)
        {
            log.WriteLine($"[INFO] Dernière transaction en base : {lastImported.Value:yyyy-MM-dd HH:mm:ss}");
        }
        else
        {
            log.WriteLine("[INFO] Aucune transaction existante en base (premier chargement).");
        }
        log.WriteLine();

        while (await csv.ReadAsync())
        {
            try
            {
                var transNumberStr = SafeGetField(csv, "Transaction_Number");
                if (string.IsNullOrWhiteSpace(transNumberStr) ||
                    !int.TryParse(transNumberStr, NumberStyles.Integer, CultureInfo.InvariantCulture, out var transNumber))
                {
                    log.WriteLine("[WARN] Ligne ignorée : Transaction_Number manquant ou invalide.");
                    ignored++;
                    continue;
                }

                var transDateTime = ParseTransactionDateTime(csv);
                if (transDateTime == null)
                {
                    log.WriteLine($"[WARN] Ligne ignorée : date de transaction invalide pour Transaction_Number={transNumber}.");
                    ignored++;
                    continue;
                }

                // Barrière de date : ne jamais insérer avant la dernière transaction en base
                if (lastImported.HasValue && transDateTime.Value < lastImported.Value)
                {
                    log.WriteLine(
                        $"[SKIP-OLD] Trans={transNumber}, Date={transDateTime.Value:yyyy-MM-dd HH:mm:ss} < Dernière en base {lastImported.Value:yyyy-MM-dd HH:mm:ss} -> ignorée.");
                    skippedTooOld++;
                    continue;
                }

                var quantity = ParseNullableDecimal(csv, "TransactionLineItem_Quantity_Value");
                var unitPriceSell = ParseNullableDecimal(csv, "TransactionLineItem_GrossSellUnitPrice_Amount");
                var unitPriceMarked = ParseNullableDecimal(csv, "TransactionLineItem_GrossMarkedUnitPrice_Amount");
                var lineAmount = ParseNullableDecimal(csv, "TransactionLineItem_GrossSellAmount_Amount");
                var taxRate = ParseNullableDecimal(csv, "TransactionLineItem_TaxRate_Value");

                if (quantity == null || unitPriceSell == null || unitPriceMarked == null || lineAmount == null || taxRate == null)
                {
                    log.WriteLine($"[WARN] Ligne ignorée : données monétaires manquantes pour Transaction_Number={transNumber}.");
                    ignored++;
                    continue;
                }

                var articleCode = SafeGetField(csv, "TransactionLineItem_Article_Code");
                var articleDescription = SafeGetField(csv, "TransactionLineItem_Article_Description");

                var dispenserNumber = ParseNullableInt(csv, "TransactionLineItem_DispenserNumber");
                var nozzleNumber = ParseNullableInt(csv, "TransactionLineItem_NozzleNumber");

                var transEndDateTimeFromCsv = ParseDateTimeOffset(csv, "Transaction_EndDateTime");

                var fiscalUnitNumber = SafeGetField(csv, "Transaction_AdditionalProperties_Fiscalization_UniqueDeviceNumber");
                var fiscalDeviceTransNumber = SafeGetField(csv, "Transaction_AdditionalProperties_Fiscalization_DeviceTransactionNumber");
                var fiscalDocType = SafeGetField(csv, "Transaction_AdditionalProperties_Fiscalization_DocumentType");
                var fiscalAmount = ParseNullableDecimal(csv, "Transaction_AdditionalProperties_Fiscalization_Amount");
                var fiscalDiscount = ParseNullableDecimal(csv, "Transaction_AdditionalProperties_Fiscalization_Discount");
                var fiscalTaxAmount = ParseNullableDecimal(csv, "Transaction_AdditionalProperties_Fiscalization_TaxAmount");

                // Station
                var stationCode = ParseNullableInt(csv, "Station_Code");
                var stationName = SafeGetField(csv, "Station_Name");

                var stationId = await ResolveStationIdAsync(connection, stationCode, stationName);
                if (stationId == null)
                {
                    log.WriteLine($"[WARN] Ligne ignorée : station introuvable pour Transaction_Number={transNumber} (Code={stationCode}, Name='{stationName}').");
                    ignored++;
                    continue;
                }

                // Terminal
                var terminalCode = ParseNullableInt(csv, "Terminal_Code");
                var terminalNumber = ParseNullableInt(csv, "Terminal_Number");
                var terminalDescription = SafeGetField(csv, "Terminal_Description");

                var terminalInfo = await ResolveTerminalAsync(
                    connection,
                    stationId.Value,
                    terminalCode,
                    terminalNumber,
                    terminalDescription);

                if (terminalInfo is null)
                {
                    log.WriteLine($"[WARN] Ligne ignorée : terminal introuvable pour Transaction_Number={transNumber} (StationID={stationId}, TerminalCode={terminalCode}).");
                    ignored++;
                    continue;
                }

                // Article
                var articleId = await ResolveArticleIdAsync(connection, articleCode, articleDescription);

                // Tank
                var tankNumber = await ResolveTankNumberAsync(connection, terminalInfo.StationsId, articleId);
                if (tankNumber is null)
                {
                    // Fallback : 1 par défaut si non trouvé
                    tankNumber = 1;
                }

                // Déduplication : (TransNumber, TerminalsID, date)
                var exists = await TransactionExistsAsync(connection, transNumber, terminalInfo.Id, transDateTime.Value);

                if (exists)
                {
                    alreadyExisting++;
                    continue;
                }

                var now = DateTime.Now;

                if (dryRun)
                {
                    log.WriteLine(
                        $"[NEW-DRYRUN] Trans={transNumber} Date={transDateTime.Value:yyyy-MM-dd HH:mm:ss}, " +
                        $"Term={terminalInfo.Id}, StationID={stationId}, ArticleID={articleId}, Qté={quantity.Value}, Montant={lineAmount.Value}, Tax={taxRate.Value}, Tank={tankNumber.Value}");
                }
                else
                {
                    await InsertTransactionAsync(
                        connection,
                        transDateTime.Value,
                        transNumber,
                        terminalInfo.Id,
                        quantity.Value,
                        unitPriceSell.Value,
                        unitPriceMarked.Value,
                        lineAmount.Value,
                        taxRate.Value,
                        articleId,
                        articleCode,
                        articleDescription,
                        dispenserNumber,
                        nozzleNumber,
                        tankNumber.Value,
                        fiscalUnitNumber,
                        fiscalDeviceTransNumber,
                        fiscalDocType,
                        fiscalAmount,
                        fiscalDiscount,
                        fiscalTaxAmount,
                        now,
                        now);

                    log.WriteLine(
                        $"[NEW-INSERT] Trans={transNumber} Date={transDateTime.Value:yyyy-MM-dd HH:mm:ss}, " +
                        $"Term={terminalInfo.Id}, StationID={stationId}, ArticleID={articleId}, Qté={quantity.Value}, Montant={lineAmount.Value}, Tax={taxRate.Value}, Tank={tankNumber.Value}");
                }

                imported++;
            }
            catch (Exception exRow)
            {
                log.WriteLine($"[WARN] Erreur lors du traitement d'une ligne : {exRow.Message}");
                ignored++;
            }
        }

        log.WriteLine();
        log.WriteLine($"[SUMMARY] Total lignes lues : {imported + alreadyExisting + ignored + skippedTooOld}");
        log.WriteLine($"[SUMMARY] Nouvelles transactions (>= dernière date et non présentes) : {imported}");
        log.WriteLine($"[SUMMARY] Déjà en base (même TransNumber/Terminal/date) : {alreadyExisting}");
        log.WriteLine($"[SUMMARY] Ignorées (erreurs / données incomplètes) : {ignored}");
        log.WriteLine($"[SUMMARY] Ignorées car plus anciennes que la dernière transaction en base : {skippedTooOld}");

        return imported;
    }

    // === Helpers parsing CSV ===================================

    private static DateTime? ParseTransactionDateTime(CsvReader csv)
    {
        var dt = ParseDateTimeOffset(csv, "Transaction_EndDateTime")
                 ?? ParseDateTimeOffset(csv, "Transaction_StartDateTime");

        return dt?.LocalDateTime;
    }

    private static DateTimeOffset? ParseDateTimeOffset(CsvReader csv, string fieldName)
    {
        var raw = SafeGetField(csv, fieldName);
        if (string.IsNullOrWhiteSpace(raw))
            return null;

        if (DateTimeOffset.TryParse(raw, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dto))
        {
            return dto;
        }

        if (DateTimeOffset.TryParse(raw, out dto))
        {
            return dto;
        }

        return null;
    }

    private static decimal? ParseNullableDecimal(CsvReader csv, string fieldName)
    {
        var raw = SafeGetField(csv, fieldName);
        if (string.IsNullOrWhiteSpace(raw))
            return null;

        if (decimal.TryParse(raw, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
        {
            return value;
        }

        // Essai culture FR (virgule) au cas où
        if (decimal.TryParse(raw, NumberStyles.Any, new CultureInfo("fr-FR"), out value))
        {
            return value;
        }

        return null;
    }

    private static int? ParseNullableInt(CsvReader csv, string fieldName)
    {
        var raw = SafeGetField(csv, fieldName);
        if (string.IsNullOrWhiteSpace(raw))
            return null;

        if (int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
        {
            return value;
        }

        return null;
    }

    private static string SafeGetField(CsvReader csv, string fieldName)
    {
        try
        {
            return csv.GetField(fieldName) ?? string.Empty;
        }
        catch
        {
            return string.Empty;
        }
    }

    // === Résolution Station / Terminal / Article / Tank =======

    private static async Task<int?> ResolveStationIdAsync(
        SqlConnection connection,
        int? stationCode,
        string? stationName)
    {
        if (stationCode is not null)
        {
            const string sqlByCode = @"
SELECT TOP (1) ID_STATIONS
FROM dbo.STATIONS
WHERE StationCode = @code;";

            await using (var cmd = new SqlCommand(sqlByCode, connection))
            {
                cmd.Parameters.AddWithValue("@code", stationCode.Value);
                var result = await cmd.ExecuteScalarAsync();
                if (result is int id)
                    return id;
            }
        }

        if (!string.IsNullOrWhiteSpace(stationName))
        {
            const string sqlByName = @"
SELECT TOP (1) ID_STATIONS
FROM dbo.STATIONS
WHERE StationName = @name;";

            await using (var cmd = new SqlCommand(sqlByName, connection))
            {
                cmd.Parameters.AddWithValue("@name", stationName);
                var result = await cmd.ExecuteScalarAsync();
                if (result is int id)
                    return id;
            }
        }

        return null;
    }

    private static async Task<TerminalInfo?> ResolveTerminalAsync(
        SqlConnection connection,
        int stationsId,
        int? terminalCode,
        int? terminalNumber,
        string? terminalDescription)
    {
        // 1) StationsID + Code
        if (terminalCode is not null)
        {
            const string sqlByCode = @"
SELECT TOP (1) ID_TERMINALS, StationsID
FROM dbo.TERMINALS
WHERE StationsID = @stationsId
  AND Code = @code;";

            await using (var cmd = new SqlCommand(sqlByCode, connection))
            {
                cmd.Parameters.AddWithValue("@stationsId", stationsId);
                cmd.Parameters.AddWithValue("@code", terminalCode.Value);

                using var reader = await cmd.ExecuteReaderAsync();
                if (await reader.ReadAsync())
                {
                    var id = reader.GetInt32(0);
                    var stId = reader.GetInt32(1);
                    return new TerminalInfo(id, stId);
                }
            }
        }

        // 2) StationsID + Number (fallback)
        if (terminalNumber is not null)
        {
            const string sqlByNumber = @"
SELECT TOP (1) ID_TERMINALS, StationsID
FROM dbo.TERMINALS
WHERE StationsID = @stationsId
  AND Number = @number;";

            await using (var cmd = new SqlCommand(sqlByNumber, connection))
            {
                cmd.Parameters.AddWithValue("@stationsId", stationsId);
                cmd.Parameters.AddWithValue("@number", terminalNumber.Value);

                using var reader = await cmd.ExecuteReaderAsync();
                if (await reader.ReadAsync())
                {
                    var id = reader.GetInt32(0);
                    var stId = reader.GetInt32(1);
                    return new TerminalInfo(id, stId);
                }
            }
        }

        // 3) StationsID + Description (ultime fallback)
        if (!string.IsNullOrWhiteSpace(terminalDescription))
        {
            const string sqlByDescription = @"
SELECT TOP (1) ID_TERMINALS, StationsID
FROM dbo.TERMINALS
WHERE StationsID = @stationsId
  AND Description = @description;";

            await using (var cmd = new SqlCommand(sqlByDescription, connection))
            {
                cmd.Parameters.AddWithValue("@stationsId", stationsId);
                cmd.Parameters.AddWithValue("@description", terminalDescription);

                using var reader = await cmd.ExecuteReaderAsync();
                if (await reader.ReadAsync())
                {
                    var id = reader.GetInt32(0);
                    var stId = reader.GetInt32(1);
                    return new TerminalInfo(id, stId);
                }
            }
        }

        return null;
    }

    private static async Task<int> ResolveArticleIdAsync(
        SqlConnection connection,
        string? articleCode,
        string? articleDescription)
    {
        if (string.IsNullOrWhiteSpace(articleCode))
            return 0;

        // Code + Description
        const string sql = @"
SELECT TOP (1) ID_ARTICLES
FROM dbo.ARTICLES
WHERE Code = @code AND Description = @description;";

        await using (var cmd = new SqlCommand(sql, connection))
        {
            cmd.Parameters.AddWithValue("@code", articleCode);
            cmd.Parameters.AddWithValue("@description", (object?)articleDescription ?? DBNull.Value);

            var result = await cmd.ExecuteScalarAsync();
            if (result is int id)
                return id;
        }

        // Fallback : Code seul
        const string sqlByCode = @"
SELECT TOP (1) ID_ARTICLES
FROM dbo.ARTICLES
WHERE Code = @code;";

        await using (var cmd2 = new SqlCommand(sqlByCode, connection))
        {
            cmd2.Parameters.AddWithValue("@code", articleCode);

            var result = await cmd2.ExecuteScalarAsync();
            return result is int id2 ? id2 : 0;
        }
    }

    private static async Task<int?> ResolveTankNumberAsync(
        SqlConnection connection,
        int stationsId,
        int articleId)
    {
        const string sql = @"
SELECT TOP (1) Number
FROM dbo.TANKS
WHERE StationsID = @stationsId
  AND ArticlesID = @articleId;";

        await using var cmd = new SqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@stationsId", stationsId);
        cmd.Parameters.AddWithValue("@articleId", articleId);

        var result = await cmd.ExecuteScalarAsync();
        if (result is int number)
            return number;

        return null;
    }

    // === Get last imported date ===============================

    private static async Task<DateTime?> GetLastImportedTransDateTimeAsync(SqlConnection connection)
    {
        const string sql = @"SELECT MAX(TransDateTime) FROM dbo.TRANSACTIONS;";
        await using var cmd = new SqlCommand(sql, connection);
        var result = await cmd.ExecuteScalarAsync();
        if (result == null || result == DBNull.Value)
            return null;
        return (DateTime)result;
    }

    // === Check existence ======================================

    private static async Task<bool> TransactionExistsAsync(
        SqlConnection connection,
        int transNumber,
        int terminalsId,
        DateTime transDateTime)
    {
        const string sql = @"
SELECT 1
FROM dbo.TRANSACTIONS
WHERE TransNumber = @transNumber
  AND TerminalsID = @terminalsId
  AND CONVERT(date, TransDateTime) = @transDate;";

        await using var cmd = new SqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@transNumber", transNumber);
        cmd.Parameters.AddWithValue("@terminalsId", terminalsId);
        cmd.Parameters.AddWithValue("@transDate", transDateTime.Date);

        var result = await cmd.ExecuteScalarAsync();
        return result != null;
    }

    // === Insert ===============================================
    private static async Task InsertTransactionAsync(
        SqlConnection connection,
        DateTime transDateTime,
        int transNumber,
        int terminalsId,
        decimal quantity,
        decimal singlePriceInclSold,
        decimal singlePriceInclActual,
        decimal amount,
        decimal taxrate,
        int articleId,
        string? articleCode,
        string? articleDescription,
        int? dispenserNumber,
        int? nozzleNumber,
        int tankNumber,
        string? fiscalUnitNumber,           // on ignore ces valeurs mais on garde la signature
        string? fiscalDeviceTransNumber,
        string? fiscalDocType,
        decimal? fiscalAmount,
        decimal? fiscalDiscount,
        decimal? fiscalTaxAmount,
        DateTime pollDateTime,
        DateTime insertDateTime)
    {
        const string sql = @"
INSERT INTO dbo.TRANSACTIONS
(
    TransDateTime,
    TransNumber,
    TerminalsID,
    Quantity,
    SinglePriceInclSold,
    SinglePriceInclActual,
    Amount,
    CurrencySymbol,
    Taxrate,
    ArticleID,
    ArticleCode,
    ArticleDescription,
    DeviceAddress,
    SubDeviceAddress,
    TankNumber,
    TransEndDateTime,
    PollDateTime,
    InsertDateTime,
    FiscalUnitNumber,
    FiscalReceiptNumber,
    FiscalDocType,
    FiscalAmount,
    FiscalDiscount,
    FiscalTaxAmount,
    ExportedCommon,
    ExportedCustomer,
    ModifiedFlag,
    FleetImport
)
VALUES
(
    @TransDateTime,
    @TransNumber,
    @TerminalsID,
    @Quantity,
    @SinglePriceInclSold,
    @SinglePriceInclActual,
    @Amount,
    @CurrencySymbol,
    @Taxrate,
    @ArticleID,
    @ArticleCode,
    @ArticleDescription,
    @DeviceAddress,
    @SubDeviceAddress,
    @TankNumber,
    @TransEndDateTime,
    @PollDateTime,
    @InsertDateTime,
    @FiscalUnitNumber,
    @FiscalReceiptNumber,
    @FiscalDocType,
    @FiscalAmount,
    @FiscalDiscount,
    @FiscalTaxAmount,
    @ExportedCommon,
    @ExportedCustomer,
    @ModifiedFlag,
    @FleetImport
);";

        await using var cmd = new SqlCommand(sql, connection);

        cmd.Parameters.AddWithValue("@TransDateTime", transDateTime);
        cmd.Parameters.AddWithValue("@TransNumber", transNumber);
        cmd.Parameters.AddWithValue("@TerminalsID", terminalsId);

        cmd.Parameters.AddWithValue("@Quantity", quantity);
        cmd.Parameters.AddWithValue("@SinglePriceInclSold", singlePriceInclSold);
        cmd.Parameters.AddWithValue("@SinglePriceInclActual", singlePriceInclActual);
        cmd.Parameters.AddWithValue("@Amount", amount);

        // CurrencySymbol : toujours NULL
        cmd.Parameters.AddWithValue("@CurrencySymbol", DBNull.Value);

        cmd.Parameters.AddWithValue("@Taxrate", taxrate);

        cmd.Parameters.AddWithValue("@ArticleID", articleId);
        cmd.Parameters.AddWithValue("@ArticleCode", (object?)articleCode ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@ArticleDescription", (object?)articleDescription ?? DBNull.Value);

        cmd.Parameters.AddWithValue("@DeviceAddress", (object?)dispenserNumber ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@SubDeviceAddress", (object?)nozzleNumber ?? DBNull.Value);

        cmd.Parameters.AddWithValue("@TankNumber", tankNumber);

        // TransEndDateTime : doit rester NULL
        cmd.Parameters.AddWithValue("@TransEndDateTime", DBNull.Value);

        cmd.Parameters.AddWithValue("@PollDateTime", pollDateTime);
        cmd.Parameters.AddWithValue("@InsertDateTime", insertDateTime);

        // === Partie fiscale : TOUT à NULL (on laisse FiscalErrorCode au DEFAULT 0) ===
        cmd.Parameters.AddWithValue("@FiscalUnitNumber", DBNull.Value);
        cmd.Parameters.AddWithValue("@FiscalReceiptNumber", DBNull.Value);
        cmd.Parameters.AddWithValue("@FiscalDocType", DBNull.Value);
        cmd.Parameters.AddWithValue("@FiscalAmount", DBNull.Value);
        cmd.Parameters.AddWithValue("@FiscalDiscount", DBNull.Value);
        cmd.Parameters.AddWithValue("@FiscalTaxAmount", DBNull.Value);

        // === Flags d'export / modification / flotte : toujours 'N' ===
        cmd.Parameters.AddWithValue("@ExportedCommon", "N");
        cmd.Parameters.AddWithValue("@ExportedCustomer", "N");
        cmd.Parameters.AddWithValue("@ModifiedFlag", "N");
        cmd.Parameters.AddWithValue("@FleetImport", "N");

        await cmd.ExecuteNonQueryAsync();
    }


}
