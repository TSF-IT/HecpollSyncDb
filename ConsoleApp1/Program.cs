using Microsoft.Data.SqlClient;

internal static class Program
{
    // Usage:
    // Hecpoll.Sync "<connectionString>" "<transactionEcpolCsvPath>"
    static async Task<int> Main(string[] args)
    {
        Logger.Init();

        if (args.Length < 2)
        {
            const string phase = "Startup";
            const string msg = "Arguments insuffisants fournis à l'application.";

            Logger.Error(phase, msg);
            Console.Error.WriteLine("""
                Utilisation:
                  Hecpoll.Sync "<connectionString>" "<transactionEcpolCsvPath>"

                Exemple:
                  Hecpoll.Sync "Server=SRV-INVENTAIRE-DB;Database=HECPOLL;User Id=xxx;Password=yyy;TrustServerCertificate=True" "C:\Data\Ecpol\Transaction_0312202522000"
                """);
            return 1;
        }

        var connectionString = args[0];
        var ecpolCsvPath = args[1];

        SqlConnectionStringBuilder? csb = null;
        try
        {
            csb = new SqlConnectionStringBuilder(connectionString);
        }
        catch
        {
            // Si la chaîne est invalide, on loguera juste sans détails.
        }

        Logger.Info("Startup", "Démarrage du traitement HecPoll (flux ECPOL → *_SHADOW).",
            new
            {
                FichierEcpol = ecpolCsvPath,
                Serveur = csb?.DataSource,
                Base = csb?.InitialCatalog
            });

        try
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            Logger.Info("Connexion", "Connexion SQL Server ouverte avec succès.");

            await CsvToShadowImporter.ImportAsync(connection, ecpolCsvPath);

            Logger.Info("Shutdown", "Traitement terminé sans erreur.");
            Console.WriteLine("Import terminé avec succès.");
            return 0;
        }
        catch (Exception ex)
        {
            Logger.Error("Global", "Erreur critique lors de l'exécution de la moulinette.", ex);
            Console.Error.WriteLine("Erreur critique lors de l'import :");
            Console.Error.WriteLine(ex);
            return 2;
        }
    }
}
