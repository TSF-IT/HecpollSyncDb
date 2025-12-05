using System.Data;
using System.Globalization;
using System.Text;
using System.Linq;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Data.SqlClient;

internal static class CsvToShadowImporter
{
    private const string TransactionsShadowTable = "[dbo].[TRANSACTIONS_SHADOW]";
    private const string PaymentsShadowTable = "[dbo].[PAYMENTS_SHADOW]";

    private sealed record TerminalInfo(
        int Id,
        int StationsId,
        string? Code,
        string? Number,
        string? TerminalNumber
    );

    private sealed class ReferenceData
    {
        // STATIONS.StationCode -> STATIONS.ID_STATIONS
        public Dictionary<string, int> StationByCode { get; } =
            new(StringComparer.OrdinalIgnoreCase);

        // TERMINALS groupés par StationsID
        public Dictionary<int, List<TerminalInfo>> TerminalsByStationId { get; } =
            new();

        // TANKS : (StationsID, ArticlesID) -> TankNumber (TANKS.Number)
        public Dictionary<(int stationId, int articleId), int> TankNumberByStationAndArticleId { get; } =
            new();

        // TANKS : (StationsID, ArticlesID) -> ID_TANKS
        public Dictionary<(int stationId, int articleId), int> TanksIdByStationAndArticleId { get; } =
            new();

        // STATIONS -> MANDATORS (via STATIONS.MandatorsID)
        // stationId -> (ID_MANDATORS, Mandator.Number, Mandator.Description)
        public Dictionary<int, (int mandatorId, string? number, string? description)> MandatorByStationId { get; } =
            new();

        // CUSTOMERS : Number -> (ID_CUSTOMERS, displayName)
        public Dictionary<string, (int id, string? name)> CustomersByNumber { get; } =
            new(StringComparer.OrdinalIgnoreCase);

        // CONTRACTS : Number -> ID_CONTRACTS
        public Dictionary<string, int> ContractsByNumber { get; } =
            new(StringComparer.OrdinalIgnoreCase);

        // VEHICLES : LicensePlate -> ID_VEHICLES
        public Dictionary<string, int> VehiclesByPlate { get; } =
            new(StringComparer.OrdinalIgnoreCase);

        // CARDS : Number -> ID_CARDS
        public Dictionary<string, int> CardsByNumber { get; } =
            new(StringComparer.OrdinalIgnoreCase);
    }

    public static async Task ImportAsync(SqlConnection connection, string ecpolCsvPath)
    {
        Logger.Info("Import", "Début de l'import du CSV ECPOL vers les tables shadow.",
            new { Fichier = ecpolCsvPath });

        using var transaction = connection.BeginTransaction();

        // 1. Chargement des données de référence (STATIONS, TERMINALS, TANKS)
        var referenceData = await LoadReferenceDataAsync(connection, transaction);

        // 2. Création des DataTable à partir du schéma SQL (SELECT TOP 0)
        var transactionsTable = CreateEmptyTableFromSchema(connection, transaction, TransactionsShadowTable);
        var paymentsTable = CreateEmptyTableFromSchema(connection, transaction, PaymentsShadowTable);

        // 3. Lecture du CSV ECPOL
        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            Delimiter = ";",
            HasHeaderRecord = true,
            Encoding = Encoding.UTF8,
            BadDataFound = args =>
            {
                var row = args.Context?.Parser?.Row ?? 0;
                Logger.Warning("CSV", "Donnée invalide dans le CSV ECPOL.",
                    new { Row = row, args.RawRecord });
            },
            MissingFieldFound = null
        };

        int logicalId = 0;
        int lignes = 0;

        using (var reader = new StreamReader(ecpolCsvPath, Encoding.UTF8))
        using (var csv = new CsvReader(reader, config))
        {
            // 1) Lire la première ligne (l'entête)
            if (!await csv.ReadAsync())
            {
                throw new InvalidOperationException("Le fichier CSV ECPOL est vide ou illisible.");
            }

            // 2) Indiquer à CsvHelper que cette ligne est l'entête
            csv.ReadHeader();

            Logger.Info("Import", "Entête du CSV ECPOL lue.",
                new { Colonnes = string.Join(";", csv.HeaderRecord ?? Array.Empty<string>()) });

            // 3) Maintenant on peut lire les lignes de données
            while (await csv.ReadAsync())
            {
                lignes++;
                logicalId++;

                var rowNumber = csv.Context?.Parser?.Row ?? 0;

                var txRow = transactionsTable.NewRow();
                var payRow = paymentsTable.NewRow();

                MapTransactionRow(csv, referenceData, txRow, logicalId, rowNumber);
                MapPaymentRow(csv, referenceData, payRow, logicalId, rowNumber);

                transactionsTable.Rows.Add(txRow);
                paymentsTable.Rows.Add(payRow);
            }
        }

        Logger.Info("Import", "Lecture du CSV ECPOL terminée.",
            new { Lignes = lignes });

        // 4. TRUNCATE des tables shadow
        await TruncateTableAsync(connection, transaction, TransactionsShadowTable);
        await TruncateTableAsync(connection, transaction, PaymentsShadowTable);

        // 5. Bulk copy des deux DataTable
        BulkInsert(transaction, transactionsTable, TransactionsShadowTable);
        BulkInsert(transaction, paymentsTable, PaymentsShadowTable);

        transaction.Commit();

        Logger.Info("Import", "Import ECPOL → *_SHADOW terminé.",
            new { LignesTransactions = transactionsTable.Rows.Count, LignesPayments = paymentsTable.Rows.Count });
    }

    #region Reference data

    private static async Task<ReferenceData> LoadReferenceDataAsync(SqlConnection connection, SqlTransaction transaction)
    {
        var data = new ReferenceData();

        // STATIONS + MANDATORS
        using (var cmd = new SqlCommand(
                   "SELECT s.ID_STATIONS, s.StationCode, s.MandatorsID, m.Number, m.Description " +
                   "FROM dbo.STATIONS s " +
                   "LEFT JOIN dbo.MANDATORS m ON m.ID_MANDATORS = s.MandatorsID;",
                   connection, transaction))
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                var stationId = reader.GetInt32(0);
                var stationCode = reader.GetString(1);
                data.StationByCode[stationCode] = stationId;

                if (!reader.IsDBNull(2))
                {
                    var mandatorId = reader.GetInt32(2);
                    var mandNumber = reader.IsDBNull(3) ? null : reader.GetString(3); // MANDATORS.Number
                    var mandDesc = reader.IsDBNull(4) ? null : reader.GetString(4); // MANDATORS.Description

                    data.MandatorByStationId[stationId] = (mandatorId, mandNumber, mandDesc);
                }
            }
        }

        // TERMINALS
        using (var cmd = new SqlCommand(
                   "SELECT ID_TERMINALS, StationsID, Code, Number, TerminalNumber FROM dbo.TERMINALS;",
                   connection, transaction))
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                var id = reader.GetInt32(0);
                var stationsId = reader.GetInt32(1);
                var code = reader.IsDBNull(2) ? null : reader.GetString(2);
                var number = reader.IsDBNull(3) ? null : reader.GetString(3);
                var termNumber = reader.IsDBNull(4) ? null : reader.GetString(4);

                var info = new TerminalInfo(id, stationsId, code, number, termNumber);

                if (!data.TerminalsByStationId.TryGetValue(stationsId, out var list))
                {
                    list = new List<TerminalInfo>();
                    data.TerminalsByStationId[stationsId] = list;
                }

                list.Add(info);
            }
        }

        // TANKS : (StationsID, ArticlesID) -> TankNumber + ID_TANKS
        using (var cmd = new SqlCommand(
                   "SELECT ID_TANKS, StationsID, ArticlesID, Number FROM dbo.TANKS;",
                   connection, transaction))
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                var tanksId = reader.GetInt32(0); // ID_TANKS
                var stationId = reader.GetInt32(1); // StationsID
                var articleId = reader.GetInt32(2); // ArticlesID
                var tankNumber = reader.GetInt32(3); // Number

                var key = (stationId, articleId);
                data.TankNumberByStationAndArticleId[key] = tankNumber;
                data.TanksIdByStationAndArticleId[key] = tanksId;
            }
        }

        // CUSTOMERS : Number -> (ID_CUSTOMERS, displayName)
        using (var cmd = new SqlCommand(
                   "SELECT ID_CUSTOMERS, Number, Company, Firstname, Lastname FROM dbo.CUSTOMERS;",
                   connection, transaction))
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                var id = reader.GetInt32(0);
                var number = reader.GetString(1); // CUSTOMERS.Number
                var company = reader.IsDBNull(2) ? null : reader.GetString(2);
                var firstname = reader.IsDBNull(3) ? null : reader.GetString(3);
                var lastname = reader.IsDBNull(4) ? null : reader.GetString(4);

                string? displayName = null;
                if (!string.IsNullOrWhiteSpace(company))
                {
                    displayName = company;
                }
                else if (!string.IsNullOrWhiteSpace(firstname) || !string.IsNullOrWhiteSpace(lastname))
                {
                    displayName = $"{firstname} {lastname}".Trim();
                }

                data.CustomersByNumber[number] = (id, displayName);
            }
        }

        // CONTRACTS : Number -> ID_CONTRACTS
        using (var cmd = new SqlCommand(
                   "SELECT ID_CONTRACTS, Number FROM dbo.CONTRACTS;",
                   connection, transaction))
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                var id = reader.GetInt32(0);
                var number = reader.GetString(1); // CONTRACTS.Number

                data.ContractsByNumber[number] = id;
            }
        }

        // VEHICLES : LicensePlate -> ID_VEHICLES
        using (var cmd = new SqlCommand(
                   "SELECT ID_VEHICLES, LicensePlate FROM dbo.VEHICLES;",
                   connection, transaction))
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                var id = reader.GetInt32(0);
                var plate = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);

                var key = NormalizePlate(plate);
                if (string.IsNullOrEmpty(key))
                    continue;

                data.VehiclesByPlate[key] = id;
            }
        }

        // CARDS : Number -> ID_CARDS (normalisé)
        using (var cmd = new SqlCommand(
                   "SELECT ID_CARDS, Number FROM dbo.CARDS;",
                   connection, transaction))
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                var id = reader.GetInt32(0);
                var number = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);

                var key = NormalizeCardNumber(number);
                if (string.IsNullOrEmpty(key))
                    continue;

                data.CardsByNumber[key] = id;
            }
        }

        Logger.Info("ReferenceData", "Tables de référence chargées.",
    new
    {
        Stations = data.StationByCode.Count,
        StationsAvecTerminaux = data.TerminalsByStationId.Count,
        CombinaisonsTanks = data.TankNumberByStationAndArticleId.Count,
        Customers = data.CustomersByNumber.Count,
        Contracts = data.ContractsByNumber.Count,
        Vehicles = data.VehiclesByPlate.Count,
        Cards = data.CardsByNumber.Count
    });


        return data;
    }

    #endregion

    #region Schema helpers

    private static DataTable CreateEmptyTableFromSchema(SqlConnection connection, SqlTransaction transaction, string tableName)
    {
        var dt = new DataTable();

        using var cmd = new SqlCommand($"SELECT TOP 0 * FROM {tableName};", connection, transaction);
        using var adapter = new Microsoft.Data.SqlClient.SqlDataAdapter(cmd);
        adapter.Fill(dt); // remplit uniquement le schéma

        return dt;
    }

    private static async Task TruncateTableAsync(SqlConnection connection, SqlTransaction transaction, string tableName)
    {
        Logger.Info("Truncate", $"TRUNCATE TABLE {tableName}.");
        var sql = $"TRUNCATE TABLE {tableName};";
        await using var cmd = new SqlCommand(sql, connection, transaction);
        await cmd.ExecuteNonQueryAsync();
    }

    private static void BulkInsert(SqlTransaction transaction, DataTable table, string tableName)
    {
        var connection = transaction.Connection
                         ?? throw new InvalidOperationException("Transaction sans connexion associée.");

        using var bulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, transaction)
        {
            DestinationTableName = tableName
        };

        foreach (DataColumn col in table.Columns)
        {
            bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);
        }

        Logger.Info("BulkCopy", $"Début du bulk copy vers {tableName}.",
            new { Lignes = table.Rows.Count });

        bulk.WriteToServer(table);

        Logger.Info("BulkCopy", $"Bulk copy terminé vers {tableName}.",
            new { Lignes = table.Rows.Count });
    }

    #endregion

    #region Mapping helpers

    private static void MapTransactionRow(
        CsvReader csv,
        ReferenceData referenceData,
        DataRow row,
        int logicalId,
        long rowNumber)
    {
        // ID technique interne à la moulinette
        row["ID_TRANSACTIONS"] = logicalId;

        // Dates / heures
        var transStartStr = GetField(csv, "Transaction_StartDateTime");
        var transEndStr = GetField(csv, "Transaction_EndDateTime");

        var transStart = ParseDateTimeOffset(transStartStr, "Transaction_StartDateTime", rowNumber);
        var transEnd = ParseDateTimeOffsetNullable(transEndStr, "Transaction_EndDateTime", rowNumber);

        row["TransDateTime"] = transStart.UtcDateTime.ToLocalTime();
        row["TransEndDateTime"] = transEnd?.UtcDateTime.ToLocalTime() ?? (object)DBNull.Value;

        // Numéro de transaction
        var transNumber = ParseInt(GetField(csv, "Transaction_Number"), "Transaction_Number", rowNumber);
        row["TransNumber"] = transNumber;

        // Station / Terminal
        var stationCode = GetField(csv, "Station_Code");
        var terminalCode = GetField(csv, "Terminal_Code");
        var terminalNumberStr = GetField(csv, "Terminal_Number");

        int terminalsId = ResolveTerminalsId(referenceData, stationCode, terminalCode, terminalNumberStr, rowNumber);
        row["TerminalsID"] = terminalsId;

        // Type de transaction : X (identique à ce qu'on observe dans l'historique)
        row["TransType"] = "X";

        // Quantité / prix / montants
        var qty = ParseDouble(GetField(csv, "TransactionLineItem_Quantity_Value"), "TransactionLineItem_Quantity_Value", rowNumber);
        var sellUnit = ParseDouble(GetField(csv, "TransactionLineItem_GrossSellUnitPrice_Amount"), "TransactionLineItem_GrossSellUnitPrice_Amount", rowNumber);
        var markedUnit = ParseDouble(GetField(csv, "TransactionLineItem_GrossMarkedUnitPrice_Amount"), "TransactionLineItem_GrossMarkedUnitPrice_Amount", rowNumber);
        var sellAmount = ParseDouble(GetField(csv, "TransactionLineItem_GrossSellAmount_Amount"), "TransactionLineItem_GrossSellAmount_Amount", rowNumber);
        var markedAmount = ParseDouble(GetField(csv, "TransactionLineItem_GrossMarkedAmount_Amount"), "TransactionLineItem_GrossMarkedAmount_Amount", rowNumber);

        row["Quantity"] = qty;
        row["SinglePriceInclSold"] = sellUnit;
        row["SinglePriceInclActual"] = markedUnit;
        row["Amount"] = sellAmount;

        var currency = GetField(csv, "TransactionLineItem_GrossSellAmount_CurrencyISOCode");
        row["CurrencySymbol"] = string.IsNullOrWhiteSpace(currency) ? DBNull.Value : currency;

        var taxRate = ParseDouble(GetField(csv, "TransactionLineItem_TaxRate_Value"), "TransactionLineItem_TaxRate_Value", rowNumber);
        row["Taxrate"] = taxRate;

        var discount = markedAmount - sellAmount;
        row["DiscountValue"] = discount > 0 ? discount : (object)DBNull.Value;

        // Article
        var articleNumberStr = GetField(csv, "TransactionLineItem_Article_Number");
        var articleCode = GetField(csv, "TransactionLineItem_Article_Code");
        var articleDescription = GetField(csv, "TransactionLineItem_Article_Description");

        var articleId = ParseInt(articleNumberStr, "TransactionLineItem_Article_Number", rowNumber);
        row["ArticleID"] = articleId;
        row["ArticleCode"] = string.IsNullOrWhiteSpace(articleCode) ? DBNull.Value : articleCode;
        row["ArticleDescription"] = string.IsNullOrWhiteSpace(articleDescription) ? DBNull.Value : articleDescription;

        // Distributeur / pistolet / cuve
        var dispenserStr = GetField(csv, "TransactionLineItem_DispenserNumber");
        var nozzleStr = GetField(csv, "TransactionLineItem_NozzleNumber");

        row["DeviceAddress"] = ParseIntNullable(dispenserStr, "TransactionLineItem_DispenserNumber", rowNumber) ?? (object)DBNull.Value;
        row["SubDeviceAddress"] = ParseIntNullable(nozzleStr, "TransactionLineItem_NozzleNumber", rowNumber) ?? (object)DBNull.Value;

        // TankNumber via lookup (StationsID + ArticlesID)
        int? tankNumber = ResolveTankNumber(referenceData, stationCode, articleId, rowNumber);
        row["TankNumber"] = tankNumber.HasValue ? (object)tankNumber.Value : DBNull.Value;

        // Status, WasExported, etc.
        row["TransStatus"] = DBNull.Value;
        row["WasExported"] = "N";

        // FileID : on marque explicitement les lignes issues du flux ECPOL
        row["FileID"] = -1;

        // Dates système / poll : on fixe un horodatage unique pour l'import
        var now = DateTime.Now;
        row["PollDateTime"] = now;          // date de "poll" côté moulinette
        row["InsertDateTime"] = now;        // date d'insertion dans la shadow

        // TypeOfTransaction, Fiscal*
        row["TypeOfTransaction"] = DBNull.Value;
        MapFiscalColumns(csv, row, rowNumber);

        row["LastChangedDateTime"] = now;
        row["LastChangedByUser"] = "hecpoll-sync";

        // ExportedCommon / ExportedCustomer / ModifiedFlag / FleetImport
        var exportedCommon = GetField(csv, "Transaction_IsExportedCommon");
        var exportedCustomer = GetField(csv, "Transaction_IsExportedCustomer");

        row["ExportedCommon"] = exportedCommon.Equals("True", StringComparison.OrdinalIgnoreCase) ? "Y" : "N";
        row["ExportedCustomer"] = exportedCustomer.Equals("True", StringComparison.OrdinalIgnoreCase) ? "Y" : "N";
        row["ModifiedFlag"] = "N";
        row["FleetImport"] = "Y";
    }

    // Normalise une plaque pour lookup : on ne garde que les caractères alphanumériques, en majuscules.
    private static string NormalizePlate(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;

        var sb = new StringBuilder(value.Length);
        foreach (var c in value.Trim())
        {
            if (char.IsLetterOrDigit(c))
                sb.Append(char.ToUpperInvariant(c));
        }

        return sb.ToString();
    }

    // Normalise un numéro de carte pour lookup : trim + majuscules
    private static string NormalizeCardNumber(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;

        return value.Trim().ToUpperInvariant();
    }

    private static void MapPaymentRow(
        CsvReader csv,
        ReferenceData referenceData,
        DataRow row,
        int logicalId,
        long rowNumber)
    {
        var contractNumberEcpol = GetField(csv, "Contract_Number");

        // ID technique
        row["ID_PAYMENTS"] = logicalId;

        // Lien vers TRANSACTIONS_SHADOW
        row["TransactionsID"] = logicalId;

        // Date / heure / numéro
        var transStartStr = GetField(csv, "Transaction_StartDateTime");
        var transStart = ParseDateTimeOffset(transStartStr, "Transaction_StartDateTime", rowNumber);
        row["TransDateTime"] = transStart.UtcDateTime.ToLocalTime();

        var transNumber = ParseInt(GetField(csv, "Transaction_Number"), "Transaction_Number", rowNumber);
        row["TransNumber"] = transNumber;

        // Article
        var articleNumberStr = GetField(csv, "TransactionLineItem_Article_Number");
        var articleCode = GetField(csv, "TransactionLineItem_Article_Code");
        var articleDescription = GetField(csv, "TransactionLineItem_Article_Description");

        var articleId = ParseInt(articleNumberStr, "TransactionLineItem_Article_Number", rowNumber);
        row["TransArticleID"] = articleId;
        row["TransArticleCode"] = string.IsNullOrWhiteSpace(articleCode) ? DBNull.Value : articleCode;
        row["TransArticleDescription"] = string.IsNullOrWhiteSpace(articleDescription) ? DBNull.Value : articleDescription;

        // Montants au niveau transaction
        row["TransQuantity"] = ParseDouble(GetField(csv, "TransactionLineItem_Quantity_Value"), "TransactionLineItem_Quantity_Value", rowNumber);
        row["TransSinglePriceInclSold"] = ParseDecimal(GetField(csv, "TransactionLineItem_GrossSellUnitPrice_Amount"), "TransactionLineItem_GrossSellUnitPrice_Amount", rowNumber);
        row["TransAmount"] = ParseDecimal(GetField(csv, "TransactionLineItem_GrossSellAmount_Amount"), "TransactionLineItem_GrossSellAmount_Amount", rowNumber);
        row["TransAmountNet"] = ParseDecimal(GetField(csv, "Transaction_NetSellTotalPrice_Amount"), "Transaction_NetSellTotalPrice_Amount", rowNumber);
        row["TransAmountTax"] = ParseDecimal(GetField(csv, "Transaction_SellTaxAmount_Amount"), "Transaction_SellTaxAmount_Amount", rowNumber);
        row["TransTaxRate"] = ParseDouble(GetField(csv, "TransactionLineItem_TaxRate_Value"), "TransactionLineItem_TaxRate_Value", rowNumber);

        // Distributeur / station / terminal
        var dispenserStr = GetField(csv, "TransactionLineItem_DispenserNumber");
        var nozzleStr = GetField(csv, "TransactionLineItem_NozzleNumber");
        row["TransDeviceAddress"] = ParseIntNullable(dispenserStr, "TransactionLineItem_DispenserNumber", rowNumber) ?? (object)DBNull.Value;
        row["TransSubDeviceAddress"] = ParseIntNullable(nozzleStr, "TransactionLineItem_NozzleNumber", rowNumber) ?? (object)DBNull.Value;

        var stationCode = GetField(csv, "Station_Code");
        var terminalCode = GetField(csv, "Terminal_Code");
        var terminalNumberStr = GetField(csv, "Terminal_Number");
        int terminalsId = ResolveTerminalsId(referenceData, stationCode, terminalCode, terminalNumberStr, rowNumber);

        // MandatorsID / MandatorNumber / MandatorDescription via STATIONS.MandatorsID + MANDATORS.Number/Description
        int? mandatorsId = null;
        string? mandatorNumber = null;
        string? mandatorDescription = null;

        if (referenceData.StationByCode.TryGetValue(stationCode, out var stationIdForMandator) &&
            referenceData.MandatorByStationId.TryGetValue(stationIdForMandator, out var mandInfo))
        {
            mandatorsId = mandInfo.mandatorId;
            mandatorNumber = mandInfo.number;
            mandatorDescription = mandInfo.description;
        }
        else
        {
            Logger.Warning("LookupMandators",
                "Aucun mandant trouvé pour cette station.",
                new { StationCode = stationCode, Row = rowNumber });
        }

        row["MandatorsID"] = mandatorsId.HasValue ? (object)mandatorsId.Value : DBNull.Value;
        row["MandatorNumber"] = string.IsNullOrWhiteSpace(mandatorNumber) ? DBNull.Value : mandatorNumber;
        row["MandatorDescription"] = string.IsNullOrWhiteSpace(mandatorDescription) ? DBNull.Value : mandatorDescription;


        row["TerminalsID"] = terminalsId;
        row["TerminalStationCode"] = string.IsNullOrWhiteSpace(stationCode) ? DBNull.Value : stationCode;
        row["TerminalNumber"] = string.IsNullOrWhiteSpace(terminalNumberStr) ? DBNull.Value : terminalNumberStr;

        // On ne renseigne pas MandatorNumber / MandatorDescription tant qu'on ne connaît pas le schéma exact de MANDATORS
        row["MandatorNumber"] = DBNull.Value;
        row["MandatorDescription"] = DBNull.Value;

        // ContractsID via CONTRACTS.Number
        int? contractsId = null;
        if (!string.IsNullOrWhiteSpace(contractNumberEcpol) &&
            referenceData.ContractsByNumber.TryGetValue(contractNumberEcpol, out var cid))
        {
            contractsId = cid;
        }
        else if (!string.IsNullOrWhiteSpace(contractNumberEcpol))
        {
            Logger.Warning("LookupContracts",
                "Aucun contrat trouvé pour ce Contract_Number (ECPOL) dans CONTRACTS.Number.",
                new { ContractNumber = contractNumberEcpol, Row = rowNumber });
        }
        row["ContractsID"] = contractsId.HasValue ? (object)contractsId.Value : DBNull.Value;
        row["ContractNumber"] = string.IsNullOrWhiteSpace(contractNumberEcpol) ? DBNull.Value : contractNumberEcpol;

        // Carte 1
        var cardOnePan = GetField(csv, "CardOne_Pan");
        var cardOneNumber = GetField(csv, "CardOne_Number");
        var cardOneHolder = GetField(csv, "CardOne_Holder");

        row["CardPAN"] = string.IsNullOrWhiteSpace(cardOnePan) ? DBNull.Value : cardOnePan;
        row["CardNumber"] = string.IsNullOrWhiteSpace(cardOneNumber) ? DBNull.Value : cardOneNumber;
        row["CardCustomerNumber"] = DBNull.Value;
        row["CardExtNumber"] = DBNull.Value;
        row["CardSystem"] = DBNull.Value;
        row["CardTankNumber"] = DBNull.Value;
        row["CardLimit"] = DBNull.Value;
        row["CardOnHand"] = DBNull.Value;
        row["CardValidFrom"] = DBNull.Value;
        row["CardValidTo"] = DBNull.Value;
        row["CardHolder"] = string.IsNullOrWhiteSpace(cardOneHolder) ? DBNull.Value : cardOneHolder;

        // Carte 2 (si présente)
        var cardTwoPan = GetField(csv, "CardTwo_Pan");
        var cardTwoNumber = GetField(csv, "CardTwo_Number");
        var cardTwoHolder = GetField(csv, "CardTwo_Holder");

        // CardsID (carte 1) via CARDS.Number
        int? cardsId = null;
        var cardOneKey = NormalizeCardNumber(cardOneNumber);

        if (!string.IsNullOrEmpty(cardOneKey) &&
            referenceData.CardsByNumber.TryGetValue(cardOneKey, out var cId1))
        {
            cardsId = cId1;
        }
        else if (!string.IsNullOrWhiteSpace(cardOneNumber))
        {
            Logger.Warning("LookupCards",
                "Aucune carte trouvée pour CardOne_Number (ECPOL) dans CARDS.Number.",
                new { CardNumber = cardOneNumber, CardKey = cardOneKey, Row = rowNumber });
        }
        row["CardsID"] = cardsId.HasValue ? (object)cardsId.Value : DBNull.Value;

        // CardsID2 (carte 2) via CARDS.Number
        int? cardsId2 = null;
        var cardTwoKey = NormalizeCardNumber(cardTwoNumber);

        if (!string.IsNullOrEmpty(cardTwoKey) &&
            referenceData.CardsByNumber.TryGetValue(cardTwoKey, out var cId2))
        {
            cardsId2 = cId2;
        }
        else if (!string.IsNullOrWhiteSpace(cardTwoNumber))
        {
            Logger.Warning("LookupCards",
                "Aucune carte trouvée pour CardTwo_Number (ECPOL) dans CARDS.Number.",
                new { CardNumber = cardTwoNumber, CardKey = cardTwoKey, Row = rowNumber });
        }
        row["CardsID2"] = cardsId2.HasValue ? (object)cardsId2.Value : DBNull.Value;

        row["CardPAN2"] = string.IsNullOrWhiteSpace(cardTwoPan) ? DBNull.Value : cardTwoPan;
        row["CardNumber2"] = string.IsNullOrWhiteSpace(cardTwoNumber) ? DBNull.Value : cardTwoNumber;
        row["CardHolder2"] = string.IsNullOrWhiteSpace(cardTwoHolder) ? DBNull.Value : cardTwoHolder;

        // Client / conducteur / véhicule
        var customerNumber = GetField(csv, "Customer_Number");
        var customerFirstName = GetField(csv, "Customer_FirstName");
        var customerLastName = GetField(csv, "Customer_LastName");
        var customerCompany = GetField(csv, "Customer_Company");

        row["CustomerNumber"] = string.IsNullOrWhiteSpace(customerNumber) ? DBNull.Value : customerNumber;
        row["CustomerName"] = !string.IsNullOrWhiteSpace(customerCompany)
            ? customerCompany
            : string.IsNullOrWhiteSpace(customerLastName + customerFirstName)
                ? DBNull.Value
                : $"{customerFirstName} {customerLastName}".Trim();

        // CustomersID via CUSTOMERS.Number
        int? customersId = null;
        if (!string.IsNullOrWhiteSpace(customerNumber) &&
            referenceData.CustomersByNumber.TryGetValue(customerNumber, out var custInfo))
        {
            customersId = custInfo.id;
        }
        else if (!string.IsNullOrWhiteSpace(customerNumber))
        {
            Logger.Warning("LookupCustomers",
                "Aucun client trouvé pour ce Customer_Number (ECPOL) dans CUSTOMERS.Number.",
                new { CustomerNumber = customerNumber, Row = rowNumber });
        }
        row["CustomersID"] = customersId.HasValue ? (object)customersId.Value : DBNull.Value;

        var driverNumber = GetField(csv, "Driver_Number");
        var driverFirstName = GetField(csv, "Driver_FirstName");
        var driverLastName = GetField(csv, "Driver_LastName");

        row["EmployeeNumber"] = string.IsNullOrWhiteSpace(driverNumber) ? DBNull.Value : driverNumber;
        row["EmployeeName"] = string.IsNullOrWhiteSpace(driverLastName + driverFirstName)
            ? DBNull.Value
            : $"{driverFirstName} {driverLastName}".Trim();

        var vehicleNumber = GetField(csv, "Vehicle_Number");
        var vehicleDescription = GetField(csv, "Vehicle_Description");
        var vehicleLicensePlate = GetField(csv, "Vehicle_LicensePlate");

        row["VehicleLicensePlate"] = string.IsNullOrWhiteSpace(vehicleLicensePlate) ? DBNull.Value : vehicleLicensePlate;

        // VehiclesID via VEHICLES.LicensePlate
        int? vehiclesId = null;
        var plateKey = NormalizePlate(vehicleLicensePlate);

        if (!string.IsNullOrEmpty(plateKey) &&
            referenceData.VehiclesByPlate.TryGetValue(plateKey, out var vehId))
        {
            vehiclesId = vehId;
        }
        else if (!string.IsNullOrWhiteSpace(vehicleLicensePlate))
        {
            Logger.Warning("LookupVehicles",
                "Aucun véhicule trouvé pour cette plaque (ECPOL) dans VEHICLES.LicensePlate.",
                new { VehicleLicensePlate = vehicleLicensePlate, PlateKey = plateKey, Row = rowNumber });
        }
        row["VehiclesID"] = vehiclesId.HasValue ? (object)vehiclesId.Value : DBNull.Value;

        // Mileage (dans le CSV, on reçoit parfois des valeurs décimales du type "40066.00")
        var mileageStr = GetField(csv, "Mileage");
        row["Mileage"] = ParseMileage(mileageStr, rowNumber) ?? (object)DBNull.Value;

        // TenderCode (0 = carte, selon cross-check avec PAYMENTS existants)
        var paymentCard = GetField(csv, "Payment_Card");
        var paymentCash = GetField(csv, "Payment_Cash");
        var paymentVoucher = GetField(csv, "Payment_Voucher");

        var tenderCode = ResolveTenderCode(paymentCard, paymentCash, paymentVoucher, rowNumber);
        row["TenderCode"] = tenderCode;

        // Number : numéro de ligne de paiement – 1 seule ligne par transaction actuellement
        row["Number"] = 1;

        // Devise / group code, etc.
        row["CurrencySymbol"] = DBNull.Value;
        row["NbrOfNotes"] = DBNull.Value;
        row["NotesAmount"] = DBNull.Value;
        row["IDGroupCode"] = DBNull.Value;
        row["AdditionalEntry"] = DBNull.Value;

        // ModifiedFlag / FleetImport
        row["ModifiedFlag"] = "N";
        row["FleetImport"] = "Y";

        // TanksID via (Station_Code, ArticleID) -> TANKS.ID_TANKS
        int? tanksId = null;
        if (referenceData.StationByCode.TryGetValue(stationCode, out var stationIdForTanks))
        {
            if (referenceData.TanksIdByStationAndArticleId.TryGetValue((stationIdForTanks, articleId), out var tid))
            {
                tanksId = tid;
            }
            else
            {
                Logger.Warning("LookupTanksID",
                    "Aucune cuve (ID_TANKS) trouvée pour cette station et cet article (PAYMENTS).",
                    new { StationCode = stationCode, StationId = stationIdForTanks, ArticleId = articleId, Row = rowNumber });
            }
        }
        else
        {
            Logger.Warning("LookupTanksID",
                "Station introuvable pour le calcul de TanksID (PAYMENTS).",
                new { StationCode = stationCode, ArticleId = articleId, Row = rowNumber });
        }

        row["TanksID"] = tanksId.HasValue ? (object)tanksId.Value : DBNull.Value;

        // Distributeur / Nozzle
        row["DispenserNumber"] = string.IsNullOrWhiteSpace(dispenserStr) ? DBNull.Value : dispenserStr;
        row["DispenserDescription"] = DBNull.Value;
        row["NozzleNumber"] = string.IsNullOrWhiteSpace(nozzleStr) ? DBNull.Value : nozzleStr;
        row["NozzleDescription"] = DBNull.Value;

    }

    private static int? ParseMileage(string value, long rowNumber)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Equals("NULL", StringComparison.OrdinalIgnoreCase))
            return null;

        // 1) Essai direct en entier (valeurs déjà "propres")
        if (int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var intResult))
        {
            return intResult;
        }

        // 2) Si le CSV nous envoie un "40066.00" ou similaire, on parse en double et on arrondit
        if (double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var doubleResult))
        {
            // Arrondi au km le plus proche (40066.50 -> 40067)
            return (int)Math.Round(doubleResult, MidpointRounding.AwayFromZero);
        }

        var msg = $"Impossible de parser le kilométrage '{value}' pour Mileage (ligne {rowNumber}).";
        Logger.Error("ParseMileage", msg);
        throw new InvalidOperationException(msg);
    }

    private static int ResolveTerminalsId(
    ReferenceData referenceData,
    string stationCode,
    string terminalCode,
    string terminalNumberStr,
    long rowNumber)
    {
        if (!referenceData.StationByCode.TryGetValue(stationCode, out var stationId))
        {
            var msg = $"Station introuvable pour Station_Code='{stationCode}' (ligne {rowNumber}).";
            Logger.Error("LookupStations", msg, data: new { StationCode = stationCode, Row = rowNumber });
            throw new InvalidOperationException(msg);
        }

        if (!referenceData.TerminalsByStationId.TryGetValue(stationId, out var terminals) || terminals.Count == 0)
        {
            var msg = $"Aucun terminal connu pour la station ID_STATIONS={stationId} (ligne {rowNumber}).";
            Logger.Error("LookupTerminals", msg, data: new { StationId = stationId, StationCode = stationCode, Row = rowNumber });
            throw new InvalidOperationException(msg);
        }

        TerminalInfo? match = null;

        // 1. Essayer sur Code (mapping direct ECPOL -> TERMINALS.Code)
        if (!string.IsNullOrWhiteSpace(terminalCode))
        {
            match = terminals.FirstOrDefault(t =>
                string.Equals(t.Code, terminalCode, StringComparison.OrdinalIgnoreCase));
        }

        // 2. Essayer sur Number (TERMINALS.Number)
        if (match is null && !string.IsNullOrWhiteSpace(terminalNumberStr))
        {
            match = terminals.FirstOrDefault(t =>
                string.Equals(t.Number, terminalNumberStr, StringComparison.OrdinalIgnoreCase));
        }

        // 3. Essayer sur TerminalNumber (TERMINALS.TerminalNumber)
        if (match is null && !string.IsNullOrWhiteSpace(terminalNumberStr))
        {
            match = terminals.FirstOrDefault(t =>
                string.Equals(t.TerminalNumber, terminalNumberStr, StringComparison.OrdinalIgnoreCase));
        }

        if (match is not null)
        {
            return match.Id;
        }

        // 4. Fallback : aucun match exact, on prend le premier terminal de la station
        var fallback = terminals.OrderBy(t => t.Id).First();

        Logger.Warning("LookupTerminals",
            "Aucun terminal ne correspond exactement aux informations ECPOL, utilisation du premier terminal de la station.",
            new
            {
                StationCode = stationCode,
                StationId = stationId,
                TerminalCodeEcpol = terminalCode,
                TerminalNumberEcpol = terminalNumberStr,
                FallbackTerminalId = fallback.Id,
                FallbackTerminalCode = fallback.Code,
                FallbackTerminalNumber = fallback.Number,
                Row = rowNumber
            });

        return fallback.Id;
    }

    private static int? ResolveTankNumber(
        ReferenceData referenceData,
        string stationCode,
        int articleId,
        long rowNumber)
    {
        // STATIONS.StationCode -> ID_STATIONS
        if (!referenceData.StationByCode.TryGetValue(stationCode, out var stationId))
        {
            Logger.Warning("LookupTanks", "Station introuvable pour le calcul de TankNumber.",
                new { StationCode = stationCode, ArticleId = articleId, Row = rowNumber });
            return null;
        }

        if (articleId <= 0)
        {
            Logger.Warning("LookupTanks", "ArticleID invalide pour le calcul de TankNumber.",
                new { StationCode = stationCode, StationId = stationId, ArticleId = articleId, Row = rowNumber });
            return null;
        }

        // TANKS : (StationsID, ArticlesID) -> Number
        if (referenceData.TankNumberByStationAndArticleId.TryGetValue((stationId, articleId), out var tankNumber))
        {
            return tankNumber;
        }

        Logger.Warning("LookupTanks", "Aucune cuve trouvée pour cette station et cet article.",
            new { StationCode = stationCode, StationId = stationId, ArticleId = articleId, Row = rowNumber });

        return null;
    }

    private static void MapFiscalColumns(CsvReader csv, DataRow row, long rowNumber)
    {
        var docType = GetField(csv, "Transaction_AdditionalProperties_Fiscalization_DocumentType");
        var amountStr = GetField(csv, "Transaction_AdditionalProperties_Fiscalization_Amount");
        var discountStr = GetField(csv, "Transaction_AdditionalProperties_Fiscalization_Discount");
        var taxAmountStr = GetField(csv, "Transaction_AdditionalProperties_Fiscalization_TaxAmount");

        row["FiscalDocType"] = string.IsNullOrWhiteSpace(docType) ? DBNull.Value : docType;
        row["FiscalAmount"] = ParseDoubleNullable(amountStr, "Transaction_AdditionalProperties_Fiscalization_Amount", rowNumber) ?? (object)DBNull.Value;
        row["FiscalDiscount"] = ParseDoubleNullable(discountStr, "Transaction_AdditionalProperties_Fiscalization_Discount", rowNumber) ?? (object)DBNull.Value;
        row["FiscalTaxAmount"] = ParseDoubleNullable(taxAmountStr, "Transaction_AdditionalProperties_Fiscalization_TaxAmount", rowNumber) ?? (object)DBNull.Value;
    }

    private static string ResolveTenderCode(string paymentCard, string paymentCash, string paymentVoucher, long rowNumber)
    {
        var isCard = paymentCard.Equals("True", StringComparison.OrdinalIgnoreCase);
        var isCash = paymentCash.Equals("True", StringComparison.OrdinalIgnoreCase);
        var isVoucher = paymentVoucher.Equals("True", StringComparison.OrdinalIgnoreCase);

        // Cas observé dans tes fichiers : True/False/False => TenderCode = "0"
        if (isCard && !isCash && !isVoucher)
        {
            return "0";
        }

        var msg = $"Combinaison de moyen de paiement non gérée par la moulinette (Card={paymentCard}, Cash={paymentCash}, Voucher={paymentVoucher}).";
        Logger.Error("TenderCode", msg, data: new { paymentCard, paymentCash, paymentVoucher, Row = rowNumber });
        throw new InvalidOperationException(msg);
    }

    #endregion

    #region Field helpers

    private static string GetField(CsvReader csv, string name)
    {
        return csv.GetField(name) ?? string.Empty;
    }

    private static DateTimeOffset ParseDateTimeOffset(string value, string fieldName, long rowNumber)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            var msg = $"Valeur vide pour le champ {fieldName} (ligne {rowNumber}).";
            Logger.Error("ParseDateTime", msg);
            throw new InvalidOperationException(msg);
        }

        // Exemple : 2020-06-16T09:15:00.0000000+02:00
        if (!DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dto))
        {
            var msg = $"Impossible de parser la date '{value}' pour {fieldName} (ligne {rowNumber}).";
            Logger.Error("ParseDateTime", msg);
            throw new InvalidOperationException(msg);
        }

        return dto;
    }

    private static DateTimeOffset? ParseDateTimeOffsetNullable(string value, string fieldName, long rowNumber)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        return ParseDateTimeOffset(value, fieldName, rowNumber);
    }

    private static int ParseInt(string value, string fieldName, long rowNumber)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            var msg = $"Valeur entière vide pour {fieldName} (ligne {rowNumber}).";
            Logger.Error("ParseInt", msg);
            throw new InvalidOperationException(msg);
        }

        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var result))
        {
            var msg = $"Impossible de parser l'entier '{value}' pour {fieldName} (ligne {rowNumber}).";
            Logger.Error("ParseInt", msg);
            throw new InvalidOperationException(msg);
        }

        return result;
    }

    private static int? ParseIntNullable(string value, string fieldName, long rowNumber)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Equals("NULL", StringComparison.OrdinalIgnoreCase))
            return null;

        return ParseInt(value, fieldName, rowNumber);
    }

    private static double ParseDouble(string value, string fieldName, long rowNumber)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Equals("NULL", StringComparison.OrdinalIgnoreCase))
            return 0d;

        // ECPOL nous donne des nombres avec point décimal
        if (!double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var result))
        {
            var msg = $"Impossible de parser le double '{value}' pour {fieldName} (ligne {rowNumber}).";
            Logger.Error("ParseDouble", msg);
            throw new InvalidOperationException(msg);
        }

        return result;
    }

    private static double? ParseDoubleNullable(string value, string fieldName, long rowNumber)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Equals("NULL", StringComparison.OrdinalIgnoreCase))
            return null;

        return ParseDouble(value, fieldName, rowNumber);
    }

    private static decimal ParseDecimal(string value, string fieldName, long rowNumber)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Equals("NULL", StringComparison.OrdinalIgnoreCase))
            return 0m;

        if (!decimal.TryParse(value, NumberStyles.Number, CultureInfo.InvariantCulture, out var result))
        {
            var msg = $"Impossible de parser le decimal '{value}' pour {fieldName} (ligne {rowNumber}).";
            Logger.Error("ParseDecimal", msg);
            throw new InvalidOperationException(msg);
        }

        return result;
    }

    #endregion
}
