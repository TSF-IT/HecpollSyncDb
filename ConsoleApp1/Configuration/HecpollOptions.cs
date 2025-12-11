using System.IO;
using Microsoft.Extensions.Configuration;

namespace Hecpoll.Sync;

/// <summary>
/// Options simples pour piloter l'exécution du batch Hecpoll.
/// </summary>
internal sealed class HecpollOptions
{
    /// <summary>
    /// Chaîne de connexion SQL Server utilisée pour l'ensemble des imports.
    /// </summary>
    public required string ConnectionString { get; init; }

    /// <summary>
    /// Répertoire racine où les fichiers sont déposés par le système amont.
    /// </summary>
    public required string BaseDirectory { get; init; }

    public string ArchiveDirectory => Path.Combine(BaseDirectory, "archive");
    public string ErrorDirectory => Path.Combine(BaseDirectory, "error");
    public string ProcessingDirectory => Path.Combine(BaseDirectory, "processing");

    public static HecpollOptions FromConfiguration(IConfiguration configuration, string[] args)
    {
        var connectionString = args.Length >= 1
            ? args[0]
            : configuration.GetConnectionString("Hecpoll");

        var baseDir = args.Length >= 2
            ? args[1]
            : configuration["Hecpoll:FilesDirectory"] ?? @"C:\hecpoll_files";

        return new HecpollOptions
        {
            ConnectionString = connectionString ?? string.Empty,
            BaseDirectory = baseDir
        };
    }
}
