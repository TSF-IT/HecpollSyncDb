using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

namespace Hecpoll.Sync;

internal static class Program
{
    private static readonly string[] SupportedPrefixes =
    {
        "Customers_",
        "Contracts_",
        "Drivers_",
        "Cards_",
        "Transaction_"
    };

    static async Task<int> Main(string[] args)
    {
        Logger.Init();
        System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);

        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        var configuration = BuildConfiguration();
        var options = HecpollOptions.FromConfiguration(configuration, args);

        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            Logger.Error("Startup", "Aucune chaîne de connexion fournie (ni argument, ni appsettings.json).");
            WriteMissingConnectionStringHelp();
            return 1;
        }

        if (!Directory.Exists(options.BaseDirectory))
        {
            Logger.Error("Startup", $"Le répertoire de base n'existe pas : {options.BaseDirectory}");
            Console.Error.WriteLine($"Répertoire introuvable : {options.BaseDirectory}");
            return 1;
        }

        CreateWorkingDirectories(options);

        var detectedFiles = DiscoverFiles(options.BaseDirectory);
        if (detectedFiles.Count == 0)
        {
            Logger.Info("Startup", "Aucun fichier HECPOLL à traiter.");
            Console.WriteLine("Rien à traiter.");
            return 0;
        }

        Logger.Info("Startup", "Fichiers détectés.", new { Fichiers = detectedFiles.Select(f => f.Name).ToArray() });

        try
        {
            var exitCode = 0;

            await using var connection = new SqlConnection(options.ConnectionString);
            await connection.OpenAsync(cts.Token);

            exitCode = await ProcessCategoryAsync(
                connection,
                options,
                "Customers_",
                (conn, path, token) => RefDataImporter.ImportCustomersAsync(conn, path, token),
                exitCode,
                cts.Token);

            exitCode = await ProcessCategoryAsync(
                connection,
                options,
                "Contracts_",
                (conn, path, token) => RefDataImporter.ImportContractsAsync(conn, path, token),
                exitCode,
                cts.Token);

            exitCode = await ProcessCategoryAsync(
                connection,
                options,
                "Cards_",
                (conn, path, token) => RefDataImporter.ImportCardsFromExcelAsync(conn, path, token),
                exitCode,
                cts.Token);

            exitCode = await ProcessCategoryAsync(
                connection,
                options,
                "Drivers_",
                (conn, path, token) => RefDataImporter.ImportEmployeesFromDriversAsync(conn, path, token),
                exitCode,
                cts.Token);

            exitCode = await ProcessCategoryAsync(
                connection,
                options,
                "Transaction_",
                (conn, path, token) => CsvToShadowImporter.ImportAsync(conn, path, token),
                exitCode,
                cts.Token);

            Logger.Info("Shutdown", "Processus global HECPOLL terminé.", new { CodeRetour = exitCode });
            return exitCode;
        }
        catch (OperationCanceledException)
        {
            Logger.Warning("Shutdown", "Arrêt demandé, interruption du traitement.");
            return 3;
        }
        catch (Exception ex)
        {
            Logger.Error("Global", "Erreur critique lors de l'exécution du processus HECPOLL.", ex);
            Console.Error.WriteLine("Erreur critique :");
            Console.Error.WriteLine(ex);
            return 2;
        }
    }

    private static IConfigurationRoot BuildConfiguration()
    {
        return new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: false)
            .AddEnvironmentVariables()
            .Build();
    }

    private static IReadOnlyList<FileInfo> DiscoverFiles(string baseDir)
    {
        return Directory.GetFiles(baseDir)
            .Select(f => new FileInfo(f))
            .Where(f => SupportedPrefixes.Any(prefix => f.Name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)))
            .OrderBy(f => f.Name)
            .ToList();
    }

    private static void CreateWorkingDirectories(HecpollOptions options)
    {
        Directory.CreateDirectory(options.ArchiveDirectory);
        Directory.CreateDirectory(options.ErrorDirectory);
        Directory.CreateDirectory(options.ProcessingDirectory);
    }

    private static async Task<int> ProcessCategoryAsync(
        SqlConnection connection,
        HecpollOptions options,
        string prefix,
        Func<SqlConnection, string, CancellationToken, Task> importFunc,
        int currentExitCode,
        CancellationToken cancellationToken)
    {
        var files = Directory.GetFiles(options.BaseDirectory, prefix + "*")
                             .Select(f => new FileInfo(f))
                             .OrderBy(f => f.Name)
                             .ToList();

        if (files.Count == 0)
        {
            Logger.Info("Files", $"Aucun fichier {prefix} à traiter.");
            return currentExitCode;
        }

        foreach (var fi in files)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var fileName = fi.Name;
            var sourcePath = fi.FullName;
            var processingPath = Path.Combine(options.ProcessingDirectory, fileName);
            var archivePath = Path.Combine(options.ArchiveDirectory, fileName);
            var errorPath = Path.Combine(options.ErrorDirectory, fileName);

            try
            {
                MoveWithOverwrite(sourcePath, processingPath);

                Logger.Info("File", $"Début de traitement du fichier {fileName}.",
                    new { Fichier = processingPath });

                await importFunc(connection, processingPath, cancellationToken);

                Logger.Info("File", $"Traitement terminé pour le fichier {fileName}.",
                    new { Fichier = processingPath });

                MoveWithOverwrite(processingPath, archivePath);
            }
            catch (Exception exFile)
            {
                Logger.Error("File", $"Erreur lors du traitement du fichier {fileName}.", exFile,
                    new { Fichier = fileName });

                Console.Error.WriteLine($"Erreur lors du traitement de {fileName} : {exFile.Message}");

                TryMoveToError(processingPath, sourcePath, errorPath);

                if (currentExitCode == 0)
                    currentExitCode = 2;
            }
        }

        return currentExitCode;
    }

    private static void MoveWithOverwrite(string sourcePath, string targetPath)
    {
        if (File.Exists(targetPath))
        {
            File.Delete(targetPath);
        }

        File.Move(sourcePath, targetPath);
    }

    private static void TryMoveToError(string processingPath, string sourcePath, string errorPath)
    {
        try
        {
            if (File.Exists(errorPath))
                File.Delete(errorPath);

            if (File.Exists(processingPath))
                File.Move(processingPath, errorPath);
            else if (File.Exists(sourcePath))
                File.Move(sourcePath, errorPath);
        }
        catch (Exception exMove)
        {
            Logger.Error("File", "Impossible de déplacer le fichier en error.", exMove,
                new { Fichier = Path.GetFileName(errorPath) });
        }
    }

    private static void WriteMissingConnectionStringHelp()
    {
        Console.Error.WriteLine("""
            Chaîne de connexion manquante.

            Options :
            - Dans appsettings.json :
                "ConnectionStrings": {
                  "Hecpoll": "Server=...;Database=HECPOLL;User Id=...;Password=...;TrustServerCertificate=True;"
                }

            - Ou en argument :
                Hecpoll.Sync.exe "Server=...;Database=HECPOLL;User Id=...;Password=...;TrustServerCertificate=True;"
            """);
    }
}
