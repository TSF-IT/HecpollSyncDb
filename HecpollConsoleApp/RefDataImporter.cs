using CsvHelper;
using CsvHelper.Configuration;
using ExcelDataReader;
using Microsoft.Data.SqlClient;
using System.Globalization;
using System.Text;

namespace Hecpoll.Sync;

internal static class RefDataImporter
{
    // ---------- PUBLIC API ----------

    public static async Task ImportCustomersAsync(SqlConnection connection, string csvPath, CancellationToken cancellationToken = default)
    {
        Logger.Info("RefData-Customers", "Début import CUSTOMERS depuis CSV HECPOLL SAAS.",
            new { Fichier = csvPath });

        await using SqlTransaction transaction = (SqlTransaction)await connection.BeginTransactionAsync(cancellationToken);

        var existingCustomers = await LoadExistingCustomersAsync(connection, transaction, cancellationToken); // Number -> (ID, Name)

        var config = CreateCsvConfig();

        int updated = 0;
        int unknown = 0;
        int skipped = 0;

        using (var reader = new StreamReader(csvPath, Encoding.UTF8))
        using (var csv = new CsvReader(reader, config))
        {
            if (!csv.Read())
                throw new InvalidOperationException("Le CSV Customers est vide ou illisible.");
            csv.ReadHeader();

            while (await csv.ReadAsync())
            {
                cancellationToken.ThrowIfCancellationRequested();
                var number = (csv.GetField("Customer_Number") ?? string.Empty).Trim();
                if (string.IsNullOrWhiteSpace(number))
                {
                    skipped++;
                    continue;
                }

                var company = (csv.GetField("Customer_Company") ?? string.Empty).Trim();
                var firstName = (csv.GetField("Customer_FirstName") ?? string.Empty).Trim();
                var lastName = (csv.GetField("Customer_LastName") ?? string.Empty).Trim();
                var email = (csv.GetField("Customer_Contact_EmailAddress") ?? string.Empty).Trim();

                if (!existingCustomers.TryGetValue(number, out var existing))
                {
                    Logger.Warning("RefData-Customers",
                        "Client CSV non trouvé dans CUSTOMERS.Number (aucune mise à jour effectuée).",
                        new { CustomerNumber = number, Company = company });
                    unknown++;
                    continue;
                }

                await UpdateCustomerAsync(connection, transaction, existing.Id, company, firstName, lastName, email, cancellationToken);
                updated++;
            }
        }

        await transaction.CommitAsync(cancellationToken);

        Logger.Info("RefData-Customers", "Import CUSTOMERS terminé.",
            new { MisAJour = updated, Inconnus = unknown, Ignorees = skipped });
    }

    public static async Task ImportContractsAsync(SqlConnection connection, string csvPath, CancellationToken cancellationToken = default)
    {
        Logger.Info("RefData-Contracts", "Début import CONTRACTS depuis CSV HECPOLL SAAS.",
            new { Fichier = csvPath });

        await using SqlTransaction transaction = (SqlTransaction)await connection.BeginTransactionAsync(cancellationToken);

        var existingContracts = await LoadExistingContractsAsync(connection, transaction, cancellationToken); // Number -> ID
        var customersByNumber = await LoadExistingCustomersAsync(connection, transaction, cancellationToken);

        var config = CreateCsvConfig();

        int updated = 0;
        int inserted = 0;
        int unknown = 0;
        int skipped = 0;

        using (var reader = new StreamReader(csvPath, Encoding.UTF8))
        using (var csv = new CsvReader(reader, config))
        {
            if (!csv.Read())
                throw new InvalidOperationException("Le CSV Contracts est vide ou illisible.");
            csv.ReadHeader();

            while (await csv.ReadAsync())
            {
                cancellationToken.ThrowIfCancellationRequested();
                var number = (csv.GetField("Contract_Number") ?? string.Empty).Trim();
                if (string.IsNullOrWhiteSpace(number))
                {
                    skipped++;
                    continue;
                }

                var description = (csv.GetField("Contract_Description") ?? string.Empty).Trim();
                var customerNumber = (csv.GetField("Customer_Number") ?? string.Empty).Trim();

                int? customersId = null;
                if (!string.IsNullOrWhiteSpace(customerNumber) &&
                    customersByNumber.TryGetValue(customerNumber, out var cust))
                {
                    customersId = cust.Id;
                }

                if (!existingContracts.TryGetValue(number, out var contractId))
                {
                    // Nouveau contrat -> tentative d'insert uniquement si on résout le client
                    if (customersId == null)
                    {
                        Logger.Warning("RefData-Contracts",
                            "Impossible d'insérer ce contrat : Customer_Number inconnu dans CUSTOMERS.Number.",
                            new { ContractNumber = number, CustomerNumber = customerNumber });
                        unknown++;
                        continue;
                    }

                    await InsertContractAsync(connection, transaction, number, description, customersId, cancellationToken);

                    Logger.Info("RefData-Contracts",
                        "Insertion d'un nouveau contrat.",
                        new { Number = number, Description = description, CustomersID = customersId });

                    inserted++;
                    continue;
                }

                // Contrat existant -> update ciblé
                if (customersId == null && !string.IsNullOrWhiteSpace(customerNumber))
                {
                    Logger.Warning("RefData-Contracts",
                        "Customer_Number du contrat non trouvé dans CUSTOMERS.Number (CustomersID inchangé).",
                        new { ContractNumber = number, CustomerNumber = customerNumber });
                }

                await UpdateContractAsync(connection, transaction, contractId.Id, description, customersId, cancellationToken);
                updated++;
            }
        }

        await transaction.CommitAsync(cancellationToken);

        Logger.Info("RefData-Contracts", "Import CONTRACTS terminé.",
            new { MisAJour = updated, Inseres = inserted, Inconnus = unknown, Ignorees = skipped });
    }

    public static async Task ImportEmployeesFromDriversAsync(SqlConnection connection, string csvPath, CancellationToken cancellationToken = default)
    {
        Logger.Info("RefData-Employees", "Début import EMPLOYEES depuis CSV Drivers HECPOLL SAAS.",
            new { Fichier = csvPath });

        await using SqlTransaction transaction = (SqlTransaction)await connection.BeginTransactionAsync(cancellationToken);

        var existingEmployees = await LoadExistingEmployeesAsync(connection, transaction, cancellationToken); // Number -> ID
        var customersByNumber = await LoadExistingCustomersAsync(connection, transaction, cancellationToken);

        var config = CreateCsvConfig();

        int updated = 0;
        int inserted = 0;
        int skipped = 0;

        using (var reader = new StreamReader(csvPath, Encoding.UTF8))
        using (var csv = new CsvReader(reader, config))
        {
            if (!csv.Read())
                throw new InvalidOperationException("Le CSV Drivers est vide ou illisible.");
            csv.ReadHeader();

            while (await csv.ReadAsync())
            {
                cancellationToken.ThrowIfCancellationRequested();
                var number = (csv.GetField("Driver_Number") ?? string.Empty).Trim();
                if (string.IsNullOrWhiteSpace(number))
                {
                    skipped++;
                    continue;
                }

                var firstName = (csv.GetField("Driver_FirstName") ?? string.Empty).Trim();
                var lastName = (csv.GetField("Driver_LastName") ?? string.Empty).Trim();
                var street = (csv.GetField("Driver_Street") ?? string.Empty).Trim();
                var houseNo = (csv.GetField("Driver_HouseNumber") ?? string.Empty).Trim();
                var zip = (csv.GetField("Driver_ZipCode") ?? string.Empty).Trim();
                var city = (csv.GetField("Driver_City") ?? string.Empty).Trim();
                var email = (csv.GetField("Driver_EmailAddress") ?? string.Empty).Trim();
                var custNum = (csv.GetField("Customer_Number") ?? string.Empty).Trim();

                string fullStreet = string.IsNullOrEmpty(houseNo) ? street : $"{street} {houseNo}".Trim();

                int? customersId = null;
                if (!string.IsNullOrWhiteSpace(custNum) &&
                    customersByNumber.TryGetValue(custNum, out var cust))
                {
                    customersId = cust.Id;
                }
                else if (!string.IsNullOrWhiteSpace(custNum))
                {
                    Logger.Warning("RefData-Employees",
                        "Impossible de résoudre CustomersID pour ce driver (Customer_Number inconnu).",
                        new { DriverNumber = number, CustomerNumber = custNum });
                }

                if (!existingEmployees.TryGetValue(number, out var empRef))
                {
                    // Nouveau driver -> insertion
                    await InsertEmployeeAsync(connection, transaction, number, firstName, lastName,
                        fullStreet, zip, city, email, customersId, cancellationToken);

                    Logger.Info("RefData-Employees",
                        "Insertion d'un nouvel employé (driver).",
                        new { Number = number, Firstname = firstName, Lastname = lastName, CustomersID = customersId });

                    inserted++;
                    continue;
                }

                // Employé existant -> update ciblé
                await UpdateEmployeeAsync(connection, transaction, empRef.Id, firstName, lastName,
                    fullStreet, zip, city, email, customersId, cancellationToken);
                updated++;
            }
        }

        await transaction.CommitAsync(cancellationToken);

        Logger.Info("RefData-Employees", "Import EMPLOYEES terminé.",
            new { MisAJour = updated, Inseres = inserted, Ignorees = skipped });
    }

    public static async Task ImportCardsFromExcelAsync(SqlConnection connection, string excelPath, CancellationToken cancellationToken = default)
    {
        Logger.Info("RefData-Cards", "Début import CARDS depuis Excel HECPOLL SAAS.",
            new { Fichier = excelPath });

        await using SqlTransaction transaction = (SqlTransaction)await connection.BeginTransactionAsync(cancellationToken);

        // Chargement des cartes existantes
        var existingCards = await LoadExistingCardsAsync(connection, transaction, cancellationToken);

        // Important pour ExcelDataReader (support encodages)
        System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);

        using var stream = File.Open(excelPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var reader = ExcelReaderFactory.CreateReader(stream);

        var conf = new ExcelDataSetConfiguration
        {
            ConfigureDataTable = _ => new ExcelDataTableConfiguration
            {
                UseHeaderRow = true // première ligne = en-têtes (Card_Number, Card_Pan, etc.)
            }
        };

        var dataSet = reader.AsDataSet(conf);
        if (dataSet.Tables.Count == 0)
            throw new InvalidOperationException("Le fichier Cards ne contient aucune feuille.");

        var table = dataSet.Tables[0]; // feuille 1

        int updated = 0;
        int staged = 0;
        int skipped = 0;

        foreach (System.Data.DataRow row in table.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var cardNumber = (row["Card_Number"] as string ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(cardNumber))
            {
                skipped++;
                continue;
            }

            var pan = (row["Card_Pan"] as string ?? string.Empty).Trim();
            var holder = (row["Card_Holder"] as string ?? string.Empty).Trim();

            if (!existingCards.TryGetValue(cardNumber, out var existing))
            {
                // Nouveau Card_Number : on le stocke en staging
                await InsertPendingCardAsync(connection, transaction, cardNumber, pan, holder, Path.GetFileName(excelPath), cancellationToken);

                Logger.Info("RefData-Cards",
                    "Insertion d'une carte en staging (CARDS_SAAS_PENDING).",
                    new { CardNumber = cardNumber, PAN = pan, Holder = holder });

                staged++;
                continue;
            }

            await UpdateCardAsync(connection, transaction, existing.Id, pan, holder, cancellationToken);
            updated++;
        }

        await transaction.CommitAsync(cancellationToken);

        Logger.Info("RefData-Cards", "Import CARDS terminé.",
            new { MisAJour = updated, Staged = staged, Ignorees = skipped });
    }

    // ---------- CSV config commun ----------

    private static CsvConfiguration CreateCsvConfig()
    {
        return new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            Delimiter = ";",
            HasHeaderRecord = true,
            Encoding = Encoding.UTF8,
            MissingFieldFound = null,
            BadDataFound = null
        };
    }

    // ---------- Load helpers ----------

    private sealed record CustomerRef(int Id, string? Name);
    private sealed record ContractRef(int Id);
    private sealed record EmployeeRef(int Id);
    private sealed record CardRef(int Id, string? Pan, string? Holder);

    private static async Task<Dictionary<string, CustomerRef>> LoadExistingCustomersAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        var dict = new Dictionary<string, CustomerRef>(StringComparer.OrdinalIgnoreCase);
        const string sql = @"SELECT ID_CUSTOMERS, Number, Company, Firstname, Lastname FROM dbo.CUSTOMERS;";
        await using var cmd = new SqlCommand(sql, connection, transaction);
        await using var rd = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await rd.ReadAsync(cancellationToken))
        {
            var id = rd.GetInt32(0);
            var number = rd.GetString(1);
            string? name = null;
            if (!rd.IsDBNull(2))
                name = rd.GetString(2);
            else
            {
                var fn = rd.IsDBNull(3) ? null : rd.GetString(3);
                var ln = rd.IsDBNull(4) ? null : rd.GetString(4);
                if (!string.IsNullOrWhiteSpace(fn) || !string.IsNullOrWhiteSpace(ln))
                    name = $"{fn} {ln}".Trim();
            }
            dict[number] = new CustomerRef(id, name);
        }
        return dict;
    }

    private static async Task<Dictionary<string, ContractRef>> LoadExistingContractsAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        var dict = new Dictionary<string, ContractRef>(StringComparer.OrdinalIgnoreCase);
        const string sql = @"SELECT ID_CONTRACTS, Number FROM dbo.CONTRACTS;";
        await using var cmd = new SqlCommand(sql, connection, transaction);
        await using var rd = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await rd.ReadAsync(cancellationToken))
        {
            var id = rd.GetInt32(0);
            var number = rd.GetString(1);
            dict[number] = new ContractRef(id);
        }
        return dict;
    }

    private static async Task<Dictionary<string, EmployeeRef>> LoadExistingEmployeesAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        var dict = new Dictionary<string, EmployeeRef>(StringComparer.OrdinalIgnoreCase);
        const string sql = @"SELECT ID_EMPLOYEES, Number FROM dbo.EMPLOYEES;";
        await using var cmd = new SqlCommand(sql, connection, transaction);
        await using var rd = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await rd.ReadAsync(cancellationToken))
        {
            var id = rd.GetInt32(0);
            var number = rd.GetString(1);
            dict[number] = new EmployeeRef(id);
        }
        return dict;
    }

    private static async Task<Dictionary<string, CardRef>> LoadExistingCardsAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        var dict = new Dictionary<string, CardRef>(StringComparer.OrdinalIgnoreCase);
        const string sql = @"SELECT ID_CARDS, Number, PAN, Cardholder FROM dbo.CARDS;";
        await using var cmd = new SqlCommand(sql, connection, transaction);
        await using var rd = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await rd.ReadAsync(cancellationToken))
        {
            var id = rd.GetInt32(0);
            var number = rd.GetString(1);
            var pan = rd.IsDBNull(2) ? null : rd.GetString(2);
            var holder = rd.IsDBNull(3) ? null : rd.GetString(3);
            dict[number] = new CardRef(id, pan, holder);
        }
        return dict;
    }

    // ---------- INSERT helpers ----------

    private static async Task InsertContractAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string number,
        string description,
        int? customersId,
        CancellationToken cancellationToken)
    {
        const string sql = @"
INSERT INTO dbo.CONTRACTS
(Number, Description, CustomersID,
 PeriodTypesID, PeriodDuration, PeriodStart,
 KindOfLimit, LimitPerFilling, Limit, Balance, OnHand,
 DateValidFrom, DateValidTo,
 AuthDay_Mo, AuthDay_Tu, AuthDay_We, AuthDay_Th, AuthDay_Fr, AuthDay_Sa, AuthDay_Su,
 RechargeLastAmount, RechargeLastDateTime,
 LastChangedDateTime, LastChangedByUser)
VALUES
(@Number, @Description, @CustomersID,
 NULL, NULL, NULL,
 'Q', NULL, NULL, NULL, NULL,
 NULL, NULL,
 'Y','Y','Y','Y','Y','Y','Y',
 NULL, NULL,
 GETDATE(), SUSER_SNAME());";

        await using var cmd = new SqlCommand(sql, connection, transaction);
        cmd.Parameters.AddWithValue("@Number", number);
        cmd.Parameters.AddWithValue("@Description", (object?)description ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@CustomersID", customersId ?? (object)DBNull.Value);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task InsertEmployeeAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string number,
        string firstName,
        string lastName,
        string street,
        string zip,
        string city,
        string email,
        int? customersId,
        CancellationToken cancellationToken)
    {
        const string sql = @"
INSERT INTO dbo.EMPLOYEES
(EmployeeGroupsID, CustomersID, Number, PersonalNumber,
 Firstname, Lastname, Addressline, Street, City, ZipCode,
 POBox, POBoxZIPCode, CountriesID, CostCenterID, Birthday, EMail,
 LastChangedDateTime, LastChangedByUser)
VALUES
(NULL, @CustomersID, @Number, NULL,
 @Firstname, @Lastname, NULL, @Street, @City, @ZipCode,
 NULL, NULL, NULL, NULL, NULL, @EMail,
 GETDATE(), SUSER_SNAME());";

        await using var cmd = new SqlCommand(sql, connection, transaction);
        cmd.Parameters.AddWithValue("@CustomersID", (object?)customersId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Number", number);
        cmd.Parameters.AddWithValue("@Firstname", (object?)firstName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Lastname", (object?)lastName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Street", (object?)street ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@City", (object?)city ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@ZipCode", (object?)zip ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@EMail", (object?)email ?? DBNull.Value);

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task InsertPendingCardAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string cardNumber,
        string pan,
        string holder,
        string sourceFile,
        CancellationToken cancellationToken)
    {
        const string sql = @"
INSERT INTO dbo.CARDS_SAAS_PENDING
(CardNumber, PAN, Cardholder, SourceFile)
VALUES
(@CardNumber, @PAN, @Cardholder, @SourceFile);";

        await using var cmd = new SqlCommand(sql, connection, transaction);
        cmd.Parameters.AddWithValue("@CardNumber", cardNumber);
        cmd.Parameters.AddWithValue("@PAN", (object?)pan ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Cardholder", (object?)holder ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@SourceFile", (object?)sourceFile ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    // ---------- Update helpers ----------

    private static async Task UpdateCustomerAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        int id,
        string company,
        string firstName,
        string lastName,
        string email,
        CancellationToken cancellationToken)
    {
        // 1) Lire les valeurs actuelles
        const string selectSql = @"
SELECT Number, Company, Firstname, Lastname, EMail
FROM dbo.CUSTOMERS
WHERE ID_CUSTOMERS = @Id;";

        string numberDb;
        string? companyDb, firstDb, lastDb, emailDb;

        await using (var selectCmd = new SqlCommand(selectSql, connection, transaction))
        {
            selectCmd.Parameters.AddWithValue("@Id", id);
            await using var rd = await selectCmd.ExecuteReaderAsync(cancellationToken);
            if (!await rd.ReadAsync(cancellationToken))
            {
                Logger.Warning("RefData-Customers",
                    "Impossible de trouver le client à mettre à jour (ID inconnu).",
                    new { Id = id });
                return;
            }

            numberDb = rd.GetString(0);
            companyDb = rd.IsDBNull(1) ? null : rd.GetString(1);
            firstDb = rd.IsDBNull(2) ? null : rd.GetString(2);
            lastDb = rd.IsDBNull(3) ? null : rd.GetString(3);
            emailDb = rd.IsDBNull(4) ? null : rd.GetString(4);
        }

        // 2) Normaliser (trim) pour comparaison
        static string Norm(string? s) => (s ?? string.Empty).Trim();

        var newCompany = company?.Trim() ?? string.Empty;
        var newFirst = firstName?.Trim() ?? string.Empty;
        var newLast = lastName?.Trim() ?? string.Empty;
        var newEmail = email?.Trim() ?? string.Empty;

        bool changeCompany = Norm(companyDb) != newCompany;
        bool changeFirst = Norm(firstDb) != newFirst;
        bool changeLast = Norm(lastDb) != newLast;
        bool changeEmail = Norm(emailDb) != newEmail;

        if (!changeCompany && !changeFirst && !changeLast && !changeEmail)
        {
            // Aucun changement -> on ne fait rien
            Logger.Info("RefData-Customers",
                "Client déjà à jour, aucune mise à jour effectuée.",
                new { Id = id, Number = numberDb });
            return;
        }

        // 3) Préparer le détail des changements pour le log
        var changes = new List<object>();
        if (changeCompany)
            changes.Add(new { Field = "Company", Old = companyDb, New = newCompany });
        if (changeFirst)
            changes.Add(new { Field = "Firstname", Old = firstDb, New = newFirst });
        if (changeLast)
            changes.Add(new { Field = "Lastname", Old = lastDb, New = newLast });
        if (changeEmail)
            changes.Add(new { Field = "EMail", Old = emailDb, New = newEmail });

        Logger.Info("RefData-Customers",
            "Mise à jour du client.",
            new { Id = id, Number = numberDb, Changes = changes });

        // 4) Exécuter le UPDATE avec les valeurs normalisées
        const string updateSql = @"
UPDATE dbo.CUSTOMERS
SET Company   = @Company,
    Firstname = @Firstname,
    Lastname  = @Lastname,
    EMail     = @EMail,
    LastChangedDateTime = GETDATE(),
    LastChangedByUser   = SUSER_SNAME()
WHERE ID_CUSTOMERS = @Id;";

        await using var updateCmd = new SqlCommand(updateSql, connection, transaction);
        updateCmd.Parameters.AddWithValue("@Id", id);
        updateCmd.Parameters.AddWithValue("@Company", (object?)newCompany ?? DBNull.Value);
        updateCmd.Parameters.AddWithValue("@Firstname", (object?)newFirst ?? DBNull.Value);
        updateCmd.Parameters.AddWithValue("@Lastname", (object?)newLast ?? DBNull.Value);
        updateCmd.Parameters.AddWithValue("@EMail", (object?)newEmail ?? DBNull.Value);

        await updateCmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task UpdateContractAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        int id,
        string description,
        int? customersId,
        CancellationToken cancellationToken)
    {
        // 1) Lire les valeurs actuelles
        const string selectSql = @"
SELECT Number, Description, CustomersID
FROM dbo.CONTRACTS
WHERE ID_CONTRACTS = @Id;";

        string numberDb;
        string? descDb;
        int? custIdDb;

        await using (var selectCmd = new SqlCommand(selectSql, connection, transaction))
        {
            selectCmd.Parameters.AddWithValue("@Id", id);
            await using var rd = await selectCmd.ExecuteReaderAsync(cancellationToken);
            if (!await rd.ReadAsync(cancellationToken))
            {
                Logger.Warning("RefData-Contracts",
                    "Impossible de trouver le contrat à mettre à jour (ID inconnu).",
                    new { Id = id });
                return;
            }

            numberDb = rd.GetString(0);
            descDb = rd.IsDBNull(1) ? null : rd.GetString(1);
            custIdDb = rd.IsDBNull(2) ? (int?)null : rd.GetInt32(2);
        }

        // 2) Comparaison après fermeture du reader
        static string Norm(string? s) => (s ?? string.Empty).Trim();

        var newDesc = description?.Trim() ?? string.Empty;
        bool changeDesc = Norm(descDb) != newDesc;
        bool changeCustomer = customersId.HasValue && customersId.Value != custIdDb;

        if (!changeDesc && !changeCustomer)
        {
            Logger.Info("RefData-Contracts",
                "Contrat déjà à jour, aucune mise à jour effectuée.",
                new { Id = id, Number = numberDb });
            return;
        }

        var changes = new List<object>();
        if (changeDesc)
            changes.Add(new { Field = "Description", Old = descDb, New = newDesc });
        if (changeCustomer)
            changes.Add(new { Field = "CustomersID", Old = custIdDb, New = customersId });

        Logger.Info("RefData-Contracts",
            "Mise à jour du contrat.",
            new { Id = id, Number = numberDb, Changes = changes });

        // 3) UPDATE proprement dit
        const string updateSql = @"
UPDATE dbo.CONTRACTS
SET Description   = @Description,
    CustomersID   = ISNULL(@CustomersID, CustomersID),
    LastChangedDateTime = GETDATE(),
    LastChangedByUser   = SUSER_SNAME()
WHERE ID_CONTRACTS = @Id;";

        await using var updateCmd = new SqlCommand(updateSql, connection, transaction);
        updateCmd.Parameters.AddWithValue("@Id", id);
        updateCmd.Parameters.AddWithValue("@Description", (object?)newDesc ?? DBNull.Value);
        updateCmd.Parameters.AddWithValue("@CustomersID", (object?)customersId ?? DBNull.Value);

        await updateCmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task UpdateEmployeeAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        int id,
        string firstName,
        string lastName,
        string street,
        string zip,
        string city,
        string email,
        int? customersId,
        CancellationToken cancellationToken)
    {
        // 1) Lire les valeurs actuelles
        const string selectSql = @"
SELECT Number, Firstname, Lastname, Street, ZipCode, City, EMail, CustomersID
FROM dbo.EMPLOYEES
WHERE ID_EMPLOYEES = @Id;";

        string numberDb;
        string? firstDb, lastDb, streetDb, zipDb, cityDb, emailDb;
        int? custIdDb;

        await using (var selectCmd = new SqlCommand(selectSql, connection, transaction))
        {
            selectCmd.Parameters.AddWithValue("@Id", id);
            await using var rd = await selectCmd.ExecuteReaderAsync(cancellationToken);
            if (!await rd.ReadAsync(cancellationToken))
            {
                Logger.Warning("RefData-Employees",
                    "Impossible de trouver l'employé à mettre à jour (ID inconnu).",
                    new { Id = id });
                return;
            }

            numberDb = rd.GetString(0);
            firstDb = rd.IsDBNull(1) ? null : rd.GetString(1);
            lastDb = rd.IsDBNull(2) ? null : rd.GetString(2);
            streetDb = rd.IsDBNull(3) ? null : rd.GetString(3);
            zipDb = rd.IsDBNull(4) ? null : rd.GetString(4);
            cityDb = rd.IsDBNull(5) ? null : rd.GetString(5);
            emailDb = rd.IsDBNull(6) ? null : rd.GetString(6);
            custIdDb = rd.IsDBNull(7) ? (int?)null : rd.GetInt32(7);
        }

        static string Norm(string? s) => (s ?? string.Empty).Trim();

        var newFirst = firstName?.Trim() ?? string.Empty;
        var newLast = lastName?.Trim() ?? string.Empty;
        var newStreet = street?.Trim() ?? string.Empty;
        var newZip = zip?.Trim() ?? string.Empty;
        var newCity = city?.Trim() ?? string.Empty;
        var newEmail = email?.Trim() ?? string.Empty;

        bool changeFirst = Norm(firstDb) != newFirst;
        bool changeLast = Norm(lastDb) != newLast;
        bool changeStreet = Norm(streetDb) != newStreet;
        bool changeZip = Norm(zipDb) != newZip;
        bool changeCity = Norm(cityDb) != newCity;
        bool changeEmail = Norm(emailDb) != newEmail;
        bool changeCustomer = customersId.HasValue && customersId.Value != custIdDb;

        if (!changeFirst && !changeLast && !changeStreet && !changeZip && !changeCity && !changeEmail && !changeCustomer)
        {
            Logger.Info("RefData-Employees",
                "Employé déjà à jour, aucune mise à jour effectuée.",
                new { Id = id, Number = numberDb });
            return;
        }

        var changes = new List<object>();
        if (changeFirst) changes.Add(new { Field = "Firstname", Old = firstDb, New = newFirst });
        if (changeLast) changes.Add(new { Field = "Lastname", Old = lastDb, New = newLast });
        if (changeStreet) changes.Add(new { Field = "Street", Old = streetDb, New = newStreet });
        if (changeZip) changes.Add(new { Field = "ZipCode", Old = zipDb, New = newZip });
        if (changeCity) changes.Add(new { Field = "City", Old = cityDb, New = newCity });
        if (changeEmail) changes.Add(new { Field = "EMail", Old = emailDb, New = newEmail });
        if (changeCustomer) changes.Add(new { Field = "CustomersID", Old = custIdDb, New = customersId });

        Logger.Info("RefData-Employees",
            "Mise à jour de l'employé.",
            new { Id = id, Number = numberDb, Changes = changes });

        const string updateSql = @"
UPDATE dbo.EMPLOYEES
SET Firstname   = @Firstname,
    Lastname    = @Lastname,
    Street      = @Street,
    ZipCode     = @ZipCode,
    City        = @City,
    EMail       = @EMail,
    CustomersID = ISNULL(@CustomersID, CustomersID),
    LastChangedDateTime = GETDATE(),
    LastChangedByUser   = SUSER_SNAME()
WHERE ID_EMPLOYEES = @Id;";

        await using var updateCmd = new SqlCommand(updateSql, connection, transaction);
        updateCmd.Parameters.AddWithValue("@Id", id);
        updateCmd.Parameters.AddWithValue("@Firstname", (object?)newFirst ?? DBNull.Value);
        updateCmd.Parameters.AddWithValue("@Lastname", (object?)newLast ?? DBNull.Value);
        updateCmd.Parameters.AddWithValue("@Street", (object?)newStreet ?? DBNull.Value);
        updateCmd.Parameters.AddWithValue("@ZipCode", (object?)newZip ?? DBNull.Value);
        updateCmd.Parameters.AddWithValue("@City", (object?)newCity ?? DBNull.Value);
        updateCmd.Parameters.AddWithValue("@EMail", (object?)newEmail ?? DBNull.Value);
        updateCmd.Parameters.AddWithValue("@CustomersID", (object?)customersId ?? DBNull.Value);

        await updateCmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task UpdateCardAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        int id,
        string pan,
        string holder,
        CancellationToken cancellationToken)
    {
        // 1) Lire les valeurs actuelles
        const string selectSql = @"
SELECT Number, PAN, Cardholder
FROM dbo.CARDS
WHERE ID_CARDS = @Id;";

        string numberDb;
        string? panDb;
        string? holderDb;

        await using (var selectCmd = new SqlCommand(selectSql, connection, transaction))
        {
            selectCmd.Parameters.AddWithValue("@Id", id);
            await using var rd = await selectCmd.ExecuteReaderAsync(cancellationToken);
            if (!await rd.ReadAsync(cancellationToken))
            {
                Logger.Warning("RefData-Cards",
                    "Impossible de trouver la carte à mettre à jour (ID inconnu).",
                    new { Id = id });
                return;
            }

            numberDb = rd.GetString(0);
            panDb = rd.IsDBNull(1) ? null : rd.GetString(1);
            holderDb = rd.IsDBNull(2) ? null : rd.GetString(2);
        }

        static string Norm(string? s) => (s ?? string.Empty).Trim();

        var newPan = pan?.Trim() ?? string.Empty;
        var newHolder = holder?.Trim() ?? string.Empty;

        bool changePan = Norm(panDb) != newPan;
        bool changeHolder = Norm(holderDb) != newHolder;

        if (!changePan && !changeHolder)
        {
            Logger.Info("RefData-Cards",
                "Carte déjà à jour, aucune mise à jour effectuée.",
                new { Id = id, Number = numberDb });
            return;
        }

        var changes = new List<object>();
        if (changePan)
            changes.Add(new { Field = "PAN", Old = panDb, New = newPan });
        if (changeHolder)
            changes.Add(new { Field = "Cardholder", Old = holderDb, New = newHolder });

        Logger.Info("RefData-Cards",
            "Mise à jour de la carte.",
            new { Id = id, Number = numberDb, Changes = changes });

        const string updateSql = @"
UPDATE dbo.CARDS
SET PAN        = @PAN,
    Cardholder = @Cardholder,
    LastChangedDateTime = GETDATE(),
    LastChangedByUser   = SUSER_SNAME()
WHERE ID_CARDS = @Id;";

        await using var updateCmd = new SqlCommand(updateSql, connection, transaction);
        updateCmd.Parameters.AddWithValue("@Id", id);
        updateCmd.Parameters.AddWithValue("@PAN", (object?)newPan ?? DBNull.Value);
        updateCmd.Parameters.AddWithValue("@Cardholder", (object?)newHolder ?? DBNull.Value);

        await updateCmd.ExecuteNonQueryAsync(cancellationToken);
    }

    // ---------- small parse utilities ----------

    private static DateTime? ParseDateNullable(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;
        if (DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt))
            return dt;
        return null;
    }

    private static decimal? ParseDecimalNullable(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;
        if (decimal.TryParse(value, NumberStyles.Number, CultureInfo.InvariantCulture, out var d))
            return d;
        return null;
    }
}


