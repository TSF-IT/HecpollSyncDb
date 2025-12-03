using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
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
        var backfillPaymentsArg = args?.Contains("--backfill-payments", StringComparer.OrdinalIgnoreCase) ?? false;
        var backfillMode = backfillPaymentsArg;
        var configBuilder = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
        if (backfillPaymentsArg)
        {
            configBuilder.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Import:BackfillPayments:Enabled"] = "true"
            });
        }
        var config = configBuilder.Build();
        PaymentsRepository.Configure(config);
        var importSection = config.GetSection("Import");
        var watchRoot = importSection["WatchDirectory"]
                        ?? throw new InvalidOperationException("Import:WatchDirectory manquant dans appsettings.json");
        var inFolderName = importSection["InFolder"] ?? "";
        var processingFolderName = importSection["ProcessingFolder"] ?? "processing";
        var archiveFolderName = importSection["ArchiveFolder"] ?? "archive";
        var errorFolderName = importSection["ErrorFolder"] ?? "error";
        var pattern = importSection["FileSearchPattern"] ?? "*";
        var logRoot = importSection["LogDirectory"] ?? Path.Combine(AppContext.BaseDirectory, "logs");
        if (!int.TryParse(importSection["PollingIntervalSeconds"], NumberStyles.Integer, CultureInfo.InvariantCulture, out var pollingSeconds))
        {
            pollingSeconds = 300;
        }
        // Mode dry-run : true = aucune Ã©criture en base, on log seulement
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
        Console.WriteLine("=== Hecpoll Sync dÃ©marrÃ© ===");
        Console.WriteLine($"RÃ©pertoire racine : {watchRoot}");
        Console.WriteLine($"BDD cible : {connectionString}");
        Console.WriteLine($"Mode dry-run : {(dryRun ? "OUI (aucune Ã©criture en base)" : "NON (insertion rÃ©elle)")}");
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
                    dryRun,
                    config,
                    backfillMode);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERREUR GLOBALE] {ex}");
            }
            if (pollingSeconds <= 0)
            {
                // Mode one-shot (tÃ¢che planifiÃ©e)
                break;
            }
            await Task.Delay(TimeSpan.FromSeconds(pollingSeconds));
        }
        Console.WriteLine("=== Hecpoll Sync terminÃ© ===");
    }
    private static async Task ProcessPendingFilesAsync(
        string inFolder,
        string processingFolder,
        string archiveFolder,
        string errorFolder,
        string logRoot,
        string pattern,
        string connectionString,
        bool dryRun,
        IConfiguration config,
        bool backfillMode)
    {
        var totalTransactionsInserted = 0;
        var totalTransactionsExisting = 0;
        var totalPaymentsInserted = 0;
        var totalPaymentsUpdated = 0;
        var totalPaymentsSkippedNoTransaction = 0;
        var fileCount = 0;
        var files = Directory.EnumerateFiles(inFolder, pattern).ToList();
        if (backfillMode)
        {
            var archiveFiles = Directory.EnumerateFiles(archiveFolder, pattern);
            files = files.Concat(archiveFiles).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        }
        if (!files.Any())
        {
            Console.WriteLine("[INFO] Aucun fichier en attente.");
            Console.WriteLine("===== HecPoll Sync Summary =====");
            Console.WriteLine($"Transactions crÃ©Ã©es           : {totalTransactionsInserted}");
            Console.WriteLine($"Transactions ignorÃ©es         : {totalTransactionsExisting}");
            Console.WriteLine($"PAYMENTS insÃ©rÃ©s              : {totalPaymentsInserted}");
            Console.WriteLine($"PAYMENTS mis Ã  jour           : {totalPaymentsUpdated}");
            Console.WriteLine($"PAYMENTS ignorÃ©s (pas de TX)  : {totalPaymentsSkippedNoTransaction}");
            Console.WriteLine($"Fichiers traitÃ©s              : {fileCount}");
            Console.WriteLine($"Mode backfill                 : {backfillMode}");
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
            Console.WriteLine($"[INFO] DÃ©but traitement fichier : {fileName}");
            Console.WriteLine($"[INFO] Log : {logPath}");
            using var log = new StreamWriter(logPath, append: false, Encoding.UTF8);
            log.WriteLine($"=== Hecpoll Sync - Fichier {fileName} ===");
            log.WriteLine($"Date de traitement : {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            log.WriteLine($"Mode dry-run : {(dryRun ? "OUI" : "NON")}");
            log.WriteLine();
            try
            {
                File.Move(file, processingPath, overwrite: true);
                var importResult = await ImportCsvFileAsync(
                    processingPath,
                    connectionString,
                    dryRun,
                    log,
                    config,
                    backfillMode);
                totalTransactionsInserted += importResult.TransactionsInserted;
                totalTransactionsExisting += importResult.TransactionsExisting;
                totalPaymentsInserted += importResult.PaymentsInserted;
                totalPaymentsUpdated += importResult.PaymentsUpdated;
                totalPaymentsSkippedNoTransaction += importResult.PaymentsSkippedNoTransaction;
                fileCount++;
                var label = dryRun
                    ? "lignes qui seraient importÃ©es (dry-run)"
                    : "lignes importÃ©es";
                Console.WriteLine($"[OK] Fichier {fileName} : {importResult.TransactionsInserted} {label}.");
                log.WriteLine();
                log.WriteLine($"[OK] Fichier {fileName} : {importResult.TransactionsInserted} {label}.");
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
                    Console.WriteLine($"[ERREUR] Impossible de dÃ©placer le fichier en erreur : {moveEx.Message}");
                    log.WriteLine($"[ERREUR] Impossible de dÃ©placer le fichier en erreur : {moveEx.Message}");
                }
            }
            Console.WriteLine("===== HecPoll Sync Summary =====");
            Console.WriteLine($"Transactions crÃ©Ã©es           : {totalTransactionsInserted}");
            Console.WriteLine($"Transactions ignorÃ©es         : {totalTransactionsExisting}");
            Console.WriteLine($"PAYMENTS insÃ©rÃ©s              : {totalPaymentsInserted}");
            Console.WriteLine($"PAYMENTS mis Ã  jour           : {totalPaymentsUpdated}");
            Console.WriteLine($"PAYMENTS ignorÃ©s (pas de TX)  : {totalPaymentsSkippedNoTransaction}");
            Console.WriteLine($"Fichiers traitÃ©s              : {fileCount}");
            Console.WriteLine($"Mode backfill                 : {backfillMode}");
        }
    }
    private static async Task<ImportResult> ImportCsvFileAsync(
    string csvPath,
    string connectionString,
    bool dryRun,
    TextWriter log,
    IConfiguration configuration,
    bool backfillMode)
    {
        var imported = 0;
        var alreadyExisting = 0;
        var ignored = 0;
        var skippedTooOld = 0;
        var paymentsUpserts = 0;
        var paymentsInserted = 0;
        var paymentsUpdated = 0;
        var paymentsSkippedNoTransaction = 0;
        var paymentsDryInsert = 0;
        var paymentsDryUpdate = 0;
        var paymentsDryNoOp = 0;
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
        // DerniÃ¨re transaction dÃ©jÃ  en base
        var lastImported = await GetLastImportedTransDateTimeAsync(connection);
        if (lastImported.HasValue)
        {
            log.WriteLine($"[INFO] DerniÃ¨re transaction en base : {lastImported.Value:yyyy-MM-dd HH:mm:ss}");
        }
        else
        {
            log.WriteLine("[INFO] Aucune transaction existante en base (premier chargement).");
        }
        log.WriteLine();
        while (await csv.ReadAsync())
        {
            var transNumber = 0;
            DateTime? transDateTime = null;
            TerminalInfo? terminalInfo = null;
            try
            {
                // --- Transaction_Number ---
                var transNumberStr = SafeGetField(csv, "Transaction_Number");
                if (string.IsNullOrWhiteSpace(transNumberStr) ||
                    !int.TryParse(transNumberStr, NumberStyles.Integer, CultureInfo.InvariantCulture, out transNumber))
                {
                    log.WriteLine("[WARN] Ligne ignorÃ©e : Transaction_Number manquant ou invalide.");
                    ignored++;
                    continue;
                }
                // --- Date de transaction ---
                transDateTime = ParseTransactionDateTime(csv);
                if (transDateTime == null)
                {
                    log.WriteLine($"[WARN] Ligne ignorÃ©e : date de transaction invalide pour Transaction_Number={transNumber}.");
                    ignored++;
                    continue;
                }
                // --- Montants / taxes ---
                var quantity = ParseNullableDecimal(csv, "TransactionLineItem_Quantity_Value");
                var unitPriceSell = ParseNullableDecimal(csv, "TransactionLineItem_GrossSellUnitPrice_Amount");
                var unitPriceMarked = ParseNullableDecimal(csv, "TransactionLineItem_GrossMarkedUnitPrice_Amount");
                var lineAmount = ParseNullableDecimal(csv, "TransactionLineItem_GrossSellAmount_Amount");
                var taxRate = ParseNullableDecimal(csv, "TransactionLineItem_TaxRate_Value");
                if (quantity == null || lineAmount == null || taxRate == null)
                {
                    log.WriteLine($"[WARN] Ligne ignorÃ©e : donnÃ©es essentielles manquantes (QtÃ©/Montant/Taxe) pour Transaction_Number={transNumber}.");
                    ignored++;
                    continue;
                }
                if (unitPriceSell == null)
                {
                    // On reconstruit un prix unitaire approximatif si possible
                    unitPriceSell = quantity.Value != 0 ? lineAmount / quantity : 0m;
                }
                if (unitPriceMarked == null)
                {
                    unitPriceMarked = unitPriceSell;
                }
                // --- Article ---
                var articleCode = SafeGetField(csv, "TransactionLineItem_Article_Code");
                var articleDescription = SafeGetField(csv, "TransactionLineItem_Article_Description");
                var dispenserNumber = ParseNullableInt(csv, "TransactionLineItem_DispenserNumber");
                var nozzleNumber = ParseNullableInt(csv, "TransactionLineItem_NozzleNumber");
                var transEndDateTimeFromCsv = ParseDateTimeOffset(csv, "Transaction_EndDateTime");
                // --- FiscalitÃ© ---
                var fiscalUnitNumber = SafeGetField(csv, "Transaction_AdditionalProperties_Fiscalization_UniqueDeviceNumber");
                var fiscalDeviceTransNumber = SafeGetField(csv, "Transaction_AdditionalProperties_Fiscalization_DeviceTransactionNumber");
                var fiscalDocType = SafeGetField(csv, "Transaction_AdditionalProperties_Fiscalization_DocumentType");
                var fiscalAmount = ParseNullableDecimal(csv, "Transaction_AdditionalProperties_Fiscalization_Amount");
                var fiscalDiscount = ParseNullableDecimal(csv, "Transaction_AdditionalProperties_Fiscalization_Discount");
                var fiscalTaxAmount = ParseNullableDecimal(csv, "Transaction_AdditionalProperties_Fiscalization_TaxAmount");
                // --- Station ---
                var stationCode = ParseNullableInt(csv, "Station_Code");
                var stationName = SafeGetField(csv, "Station_Name");
                var stationId = await ResolveStationIdAsync(connection, stationCode, stationName);
                if (stationId == null)
                {
                    log.WriteLine($"[WARN] Ligne ignorÃ©e : station introuvable pour Transaction_Number={transNumber} (Code={stationCode}, Name='{stationName}').");
                    ignored++;
                    continue;
                }
                // --- Terminal ---
                var terminalCode = ParseNullableInt(csv, "Terminal_Code");
                var terminalNumber = ParseNullableInt(csv, "Terminal_Number");
                var terminalDescription = SafeGetField(csv, "Terminal_Description");
                terminalInfo = await ResolveTerminalAsync(
                    connection,
                    stationId.Value,
                    terminalCode,
                    terminalNumber,
                    terminalDescription);
                if (terminalInfo is null)
                {
                    log.WriteLine($"[WARN] Ligne ignorÃ©e : terminal introuvable pour Transaction_Number={transNumber} (StationID={stationId}, TerminalCode={terminalCode}).");
                    ignored++;
                    continue;
                }
                // --- ArticleID : d'abord via ArticleMapFromPayments, sinon fallback ---
                int articleId;
                var mappedArticleId = await PaymentsRepository.ResolveArticleIdFromMapAsync(
                    connection,
                    articleCode,
                    articleDescription,
                    taxRate.Value,
                    CancellationToken.None);
                if (mappedArticleId.HasValue && mappedArticleId.Value > 0)
                {
                    articleId = mappedArticleId.Value;
                }
                else
                {
                    articleId = await ResolveArticleIdAsync(connection, articleCode, articleDescription);
                }
                // --- Tank ---
                var tankNumber = await ResolveTankNumberAsync(connection, terminalInfo.StationsId, articleId);
                if (tankNumber is null)
                {
                    tankNumber = 1; // Fallback
                }
                Console.WriteLine($"[PAYMENTS-DEBUG] CSV line: TransNumber={transNumber}, Terminal={terminalInfo.Id}, Date={transDateTime}");
                var txId = await PaymentsRepository.GetTransactionIdAsync(
                    connection,
                    transNumber,
                    terminalInfo.Id,
                    transDateTime.Value);
                var transactionExisted = txId.HasValue;
                if (transactionExisted)
                {
                    alreadyExisting++;
                }
                // --- BarriÃ¨re + backfill ---
                if (!transactionExisted)
                {
                    if (backfillMode)
                    {
                        paymentsSkippedNoTransaction++;
                        Console.WriteLine($"[PAYMENTS-DEBUG] SKIP payment: reason=NoTransactionOrTooOld, TransNumber={transNumber}, Terminal={terminalInfo.Id}, Date={transDateTime}");
                        Console.WriteLine($"[PAYMENTS-SKIPPED] TX introuvable pour TransNumber={transNumber} Terminal={terminalInfo.Id}");
                        continue;
                    }
                    if (lastImported.HasValue && transDateTime.Value < lastImported.Value)
                    {
                        log.WriteLine(
                            $"[SKIP-OLD] Trans={transNumber}, Date={transDateTime.Value:yyyy-MM-dd HH:mm:ss} < DerniÃ¨re en base {lastImported.Value:yyyy-MM-dd HH:mm:ss} -> ignorÃ©e.");
                        skippedTooOld++;
                        Console.WriteLine($"[PAYMENTS-DEBUG] SKIP payment: reason=NoTransactionOrTooOld, TransNumber={transNumber}, Terminal={terminalInfo.Id}, Date={transDateTime}");
                        continue;
                    }
                }
                if (dryRun)
                {
                    if (!transactionExisted)
                    {
                        log.WriteLine(
                            $"[NEW-DRYRUN] Trans={transNumber} Date={transDateTime.Value:yyyy-MM-dd HH:mm:ss}, " +
                            $"Term={terminalInfo.Id}, StationID={stationId}, ArticleID={articleId}, QtÃ©={quantity.Value}, Montant={lineAmount.Value}, Tax={taxRate.Value}, Tank={tankNumber.Value}");
                        imported++;
                    }
                    else
                    {
                        log.WriteLine(
                            $"[PAYMENTS-DRYRUN] Trans={transNumber} Date={transDateTime.Value:yyyy-MM-dd HH:mm:ss}, Term={terminalInfo.Id}, ArticleID={articleId}, Montant={lineAmount.Value}");
                    }
                    continue;
                }
                // --- Insertion Ã©ventuelle de TRANSACTIONS ---
                if (!transactionExisted)
                {
                    var now = DateTime.Now;
                    var transactionId = await InsertTransactionAsync(
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
                        $"Term={terminalInfo.Id}, StationID={stationId}, ArticleID={articleId}, QtÃ©={quantity.Value}, Montant={lineAmount.Value}, Tax={taxRate.Value}, Tank={tankNumber.Value}, ID={transactionId}");
                    txId = transactionId;
                    imported++;
                }
                Console.WriteLine($"[PAYMENTS-DEBUG] About to upsert payment: txId={txId}, TransNumber={transNumber}, Terminal={terminalInfo.Id}, Date={transDateTime}, Amount={lineAmount}, Qty={quantity}");
                var amountNet = (lineAmount != null && taxRate != null)
                    ? Math.Round(lineAmount.Value / (1 + taxRate.Value / 100m), 2)
                    : (decimal?)null;
                var amountTax = (lineAmount != null && amountNet != null)
                    ? lineAmount.Value - amountNet.Value
                    : (decimal?)null;
                // === Carte / PAN / Enrichissement ===
                // PAN brut du SaaS (carte "conducteur")
                var cardPanCsv    = SafeGetField(csv, "CardOne_Pan");
                var cardNumberCsv = SafeGetField(csv, "CardOne_Number");
                var cardHolder    = SafeGetField(csv, "CardOne_Holder");
                // Enrichissement via la map historique si la transaction existe deja en base
                CardEnrichment? mapEnrichment = null;
                if (txId.HasValue)
                {
                    mapEnrichment = await CardEnrichmentService.EnrichFromCardMapAsync(
                        connection,
                        txId.Value,
                        transNumber,
                        CancellationToken.None);
                }
                // Prepare les variables finales
                string? cardPan = null;
                int?    cardsId = null;
                string? cardCustomerNumber = null;
                string? cardNumber = null;
                int?    cardTankNumber = null;
                int?    customersId = null;
                string? customerNumber = null;
                string? customerName = null;
                int?    mandatorsId = null;
                string? mandatorNumber = null;
                string? mandatorDescription = null;
                int?    vehiclesId = null;
                string? vehicleLicensePlate = null;
                if (mapEnrichment is not null)
                {
                    // On privilegie la verite de la base on-prem
                    cardsId             = mapEnrichment.CardsId;
                    cardCustomerNumber  = mapEnrichment.CardCustomerNumber;
                    cardNumber          = mapEnrichment.CardNumber;
                    cardTankNumber      = mapEnrichment.CardTankNumber;
                    customersId         = mapEnrichment.CustomersId;
                    customerNumber      = mapEnrichment.CustomerNumber;
                    customerName        = mapEnrichment.CustomerName;
                    mandatorsId         = mapEnrichment.MandatorsId;
                    mandatorNumber      = mapEnrichment.MandatorNumber;
                    mandatorDescription = mapEnrichment.MandatorDescription;
                    vehiclesId          = mapEnrichment.VehiclesId;
                    vehicleLicensePlate = mapEnrichment.VehicleLicensePlate;
                    var panFromMap = mapEnrichment.CardPanResolved;
                    if (!string.IsNullOrWhiteSpace(panFromMap))
                    {
                        var compact = panFromMap.Trim();
                        cardPan = compact.EndsWith("=", StringComparison.Ordinal) ? compact : compact + "=";
                    }
                }
                else
                {
                    // Fallback : enrichissement a partir du CSV / CARDS pour les transactions pure SaaS
                    var cardTokenForEnrichment = !string.IsNullOrWhiteSpace(cardPanCsv) ? cardPanCsv : cardNumberCsv;
                    var csvEnrichment = await CardEnrichmentService.EnrichAsync(
                        connection,
                        cardTokenForEnrichment,
                        CancellationToken.None);
                    if (!string.IsNullOrWhiteSpace(cardPanCsv))
                    {
                        var compactPan = cardPanCsv.Trim();
                        cardPan = compactPan.EndsWith("=", StringComparison.Ordinal) ? compactPan : compactPan + "=";
                    }
                    if (csvEnrichment is not null)
                    {
                        cardsId             = csvEnrichment.CardsId;
                        cardCustomerNumber  = csvEnrichment.CardCustomerNumber;
                        cardNumber          = csvEnrichment.CardNumber ?? cardNumberCsv;
                        cardTankNumber      = csvEnrichment.CardTankNumber;
                        customersId         = csvEnrichment.CustomersId;
                        customerNumber      = csvEnrichment.CustomerNumber;
                        customerName        = csvEnrichment.CustomerName;
                        mandatorsId         = csvEnrichment.MandatorsId;
                        mandatorNumber      = csvEnrichment.MandatorNumber;
                        mandatorDescription = csvEnrichment.MandatorDescription;
                        vehiclesId          = csvEnrichment.VehiclesId;
                        vehicleLicensePlate = csvEnrichment.VehicleLicensePlate ?? vehicleLicensePlate;
                        if (!string.IsNullOrWhiteSpace(csvEnrichment.CardPanResolved))
                            cardPan = csvEnrichment.CardPanResolved;
                    }
                }
                // Si toujours aucune plaque, prendre une eventuelle info du CSV
                if (string.IsNullOrWhiteSpace(vehicleLicensePlate))
                {
                    var plateRaw = SafeGetField(csv, "VehicleLicensePlate");
                    if (string.IsNullOrWhiteSpace(plateRaw))
                        plateRaw = SafeGetField(csv, "Transaction_CustomerInputs_AdditionalEntry");
                    if (!string.IsNullOrWhiteSpace(plateRaw))
                        vehicleLicensePlate = plateRaw.Trim();
                }
                var currencySymbolRaw = SafeGetField(csv, "TransactionLineItem_GrossSellAmount_CurrencyISOCode");
                var currencySymbol = string.IsNullOrWhiteSpace(currencySymbolRaw) ? null : currencySymbolRaw;
                // PAN normalise (sans '=') pour lookup dans CardDriverVehicleMap
                string? driverPan = null;
                if (!string.IsNullOrWhiteSpace(cardPanCsv))
                {
                    driverPan = cardPanCsv.Trim();
                    if (driverPan.EndsWith("=", StringComparison.Ordinal))
                        driverPan = driverPan.Substring(0, driverPan.Length - 1);
                }
                // --- Mapping vehicule via CardDriverVehicleMap (si on a un driverPan) ---
                if (!string.IsNullOrWhiteSpace(driverPan))
                {
                    var map = await PaymentsRepository.GetCardVehicleMappingAsync(
                        connection,
                        driverPan,
                        transNumber,
                        CancellationToken.None);
                    if (map != null)
                    {
                        // Carte véhicule comme principale
                        cardsId = map.VehicleCardId;
                        cardCustomerNumber = map.VehicleCardCustomer;
                        cardNumber = map.VehicleCardNumber;
                        cardPan = map.VehicleCardPan;        // ex: GQ348ND=
                        vehiclesId = map.VehicleId;
                        vehicleLicensePlate = map.VehiclePlate;
                        customersId = map.VehicleCustomersId;
                        customerNumber = map.VehicleCustomerNumber;
                        customerName = map.VehicleCustomerName;
                        mandatorsId = map.MandatorsId;
                        mandatorNumber = map.MandatorNumber;
                        mandatorDescription = map.MandatorDescription;
                        // Tu peux, si besoin, exploiter map.VehicleContractId plus tard
                    }
                }
                var tenderCode = PaymentsRepository.ResolveTenderCode(configuration, csv);
                // ================================================================
                if (dryRun)
                {
                    var action = await PaymentsRepository.DryRunPaymentAsync(
                        connection,
                        transactionsId: txId!.Value,
                        transDateTime: transDateTime.Value,
                        transNumber: transNumber,
                        terminalsId: terminalInfo.Id,
                        tenderCode: tenderCode,
                        transQuantity: quantity,
                        transSinglePriceInclSold: unitPriceSell,
                        transAmount: lineAmount,
                        transAmountNet: amountNet,
                        transAmountTax: amountTax,
                        transTaxRate: taxRate,
                        transArticleCode: articleCode,
                        transArticleDescription: articleDescription,
                        transDeviceAddress: dispenserNumber,
                        transSubDeviceAddress: nozzleNumber,
                        currencySymbol: currencySymbol,
                        cardPan: cardPan,
                        cardCustomerNumber: cardCustomerNumber,
                        cardNumber: cardNumber,
                        cardTankNumber: cardTankNumber,
                        cardsId: cardsId,
                        customersId: customersId,
                        customerNumber: customerNumber,
                        customerName: customerName,
                        mandatorsId: mandatorsId,
                        mandatorNumber: mandatorNumber,
                        mandatorDescription: mandatorDescription,
                        vehiclesId: vehiclesId,
                        vehicleLicensePlate: vehicleLicensePlate,
                        cancellationToken: CancellationToken.None);
                    switch (action)
                    {
                        case PaymentDryRunAction.Insert:
                            paymentsDryInsert++;
                            break;
                        case PaymentDryRunAction.Update:
                            paymentsDryUpdate++;
                            break;
                        case PaymentDryRunAction.NoOp:
                            paymentsDryNoOp++;
                            break;
                    }
                }
                else
                {
                    var affected = await PaymentsRepository.UpsertPaymentAsync(
                        connection,
                        null,
                        txId!.Value,
                        transDateTime.Value,
                        transNumber,
                        terminalInfo.Id,
                        tenderCode,
                        quantity,
                        unitPriceSell,
                        lineAmount,
                        amountNet,
                        amountTax,
                        taxRate,
                        articleId,              // << ajoute ici
                        articleCode,
                        articleDescription,
                        dispenserNumber,
                        nozzleNumber,
                        currencySymbol,
                        cardPan,
                        cardCustomerNumber,
                        cardNumber,
                        cardTankNumber,
                        cardsId,
                        customersId,
                        customerNumber,
                        customerName,
                        mandatorsId,
                        mandatorNumber,
                        mandatorDescription,
                        vehiclesId,
                        vehicleLicensePlate);
                    if (affected > 0)
                    {
                        paymentsInserted++;
                        Console.WriteLine($"[PAYMENTS-UPSERT] Rows={affected}, Trans={transNumber}, Term={terminalInfo.Id}, Article={articleCode}, Amount={lineAmount}");
                    }
                    if (transactionExisted)
                    {
                        paymentsUpserts++;
                        log.WriteLine(
                            $"[PAYMENTS] upsert pour Trans={transNumber}, Term={terminalInfo.Id}, Article={articleCode}, Amount={lineAmount.Value}");
                    }
                }
            }
            catch (Exception exRow)
            {
                Console.WriteLine($"[ERROR] Exception lors du traitement de la ligne CSV: TransNumber={SafeGetField(csv, "Transaction_Number")}, Terminal={(terminalInfo != null ? terminalInfo.Id : null)}, Date={transDateTime}, Message={exRow.Message}");
                Console.WriteLine(exRow.ToString());
                log.WriteLine($"[WARN] Erreur lors du traitement d'une ligne : {exRow}");
                ignored++;
            }
        }
        log.WriteLine();
        log.WriteLine($"[SUMMARY] Total lignes lues : {imported + alreadyExisting + ignored + skippedTooOld}");
        log.WriteLine($"[SUMMARY] Nouvelles transactions (>= derniÃ¨re date et non prÃ©sentes) : {imported}");
        log.WriteLine($"[SUMMARY] DÃ©jÃ  en base (mÃªme TransNumber/Terminal/date) : {alreadyExisting}");
        log.WriteLine($"[SUMMARY] Upserts PAYMENTS sur transactions existantes : {paymentsUpserts}");
        log.WriteLine($"[SUMMARY] PAYMENTS insÃ©rÃ©s : {paymentsInserted}");
        log.WriteLine($"[SUMMARY] PAYMENTS mis Ã  jour : {paymentsUpdated}");
        log.WriteLine($"[SUMMARY] PAYMENTS ignorÃ©s faute de transaction : {paymentsSkippedNoTransaction}");
        log.WriteLine($"[SUMMARY] IgnorÃ©es (erreurs / donnÃ©es incomplÃ¨tes) : {ignored}");
        log.WriteLine($"[SUMMARY] IgnorÃ©es car plus anciennes que la derniÃ¨re transaction en base : {skippedTooOld}");
        Console.WriteLine($"Mode dry-run                 : {dryRun}");
        Console.WriteLine($"[DRYRUN PAYMENTS] INSERT  : {paymentsDryInsert}");
        Console.WriteLine($"[DRYRUN PAYMENTS] UPDATE  : {paymentsDryUpdate}");
        Console.WriteLine($"[DRYRUN PAYMENTS] NO-OP   : {paymentsDryNoOp}");
        log.WriteLine();
        log.WriteLine("[BACKFILL PAYMENTS]");
        log.WriteLine($"- InsÃ©rÃ©s : {paymentsInserted}");
        log.WriteLine($"- Mis Ã  jour : {paymentsUpdated}");
        log.WriteLine($"- IgnorÃ©s (transaction introuvable) : {paymentsSkippedNoTransaction}");
        return new ImportResult(imported, alreadyExisting, paymentsInserted, paymentsUpdated, paymentsSkippedNoTransaction);
    }
    private sealed record ImportResult(int TransactionsInserted, int TransactionsExisting, int PaymentsInserted, int PaymentsUpdated, int PaymentsSkippedNoTransaction);
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
        // Essai culture FR (virgule) au cas oÃ¹
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
    // === RÃ©solution Station / Terminal / Article / Tank =======
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
                    var id = DbSafeReader.GetIntNullable(reader, 0);
                    var stId = DbSafeReader.GetIntNullable(reader, 1);
                    if (id.HasValue && stId.HasValue)
                        return new TerminalInfo(id.Value, stId.Value);
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
                    var id = DbSafeReader.GetIntNullable(reader, 0);
                    var stId = DbSafeReader.GetIntNullable(reader, 1);
                    if (id.HasValue && stId.HasValue)
                        return new TerminalInfo(id.Value, stId.Value);
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
                    var id = DbSafeReader.GetIntNullable(reader, 0);
                    var stId = DbSafeReader.GetIntNullable(reader, 1);
                    if (id.HasValue && stId.HasValue)
                        return new TerminalInfo(id.Value, stId.Value);
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
    // === Insert ===============================================
    private static async Task<int> InsertTransactionAsync(
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
);
SELECT CAST(SCOPE_IDENTITY() AS int);";
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
        // === Partie fiscale : TOUT Ã  NULL (on laisse FiscalErrorCode au DEFAULT 0) ===
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
        var result = await cmd.ExecuteScalarAsync();
        if (result == null || result == DBNull.Value)
            return 0;
        return Convert.ToInt32(result);
    }
}
