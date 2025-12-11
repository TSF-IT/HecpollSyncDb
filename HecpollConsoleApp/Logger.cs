using System.Text.Json;
using System.Text.Json.Serialization;

namespace Hecpoll.Sync;

internal static class Logger
{
    private static readonly object _lock = new();
    private static string _logFilePath;

    static Logger()
    {
        var baseDir = AppContext.BaseDirectory;
        var logDir = Path.Combine(baseDir, "logs");
        Directory.CreateDirectory(logDir);

        var fileName = $"hecpoll-sync_{DateTime.Now:yyyyMMdd_HHmmss}.log";
        _logFilePath = Path.Combine(logDir, fileName);
    }

    public static void Init(string? customPath = null)
    {
        if (!string.IsNullOrWhiteSpace(customPath))
        {
            _logFilePath = customPath;
            var dir = Path.GetDirectoryName(_logFilePath);
            if (!string.IsNullOrEmpty(dir))
            {
                Directory.CreateDirectory(dir);
            }
        }
    }

    public static void Info(string phase, string message, object? data = null)
        => Log("INFO", phase, message, data, null);

    public static void Warning(string phase, string message, object? data = null)
        => Log("WARN", phase, message, data, null);

    public static void Error(string phase, string message, Exception? ex = null, object? data = null)
        => Log("ERROR", phase, message, data, ex);

    private static void Log(string level, string phase, string message, object? data, Exception? ex)
    {
        var entry = new LogEntry
        {
            Timestamp = DateTimeOffset.Now,
            Level = level,
            Phase = phase,
            Message = message,
            Data = data,
            Exception = ex?.ToString()
        };

        var json = JsonSerializer.Serialize(entry, new JsonSerializerOptions
        {
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        });

        lock (_lock)
        {
            File.AppendAllText(_logFilePath, json + Environment.NewLine);
        }

        // Echo console (utile en lancement manuel)
        Console.WriteLine($"[{entry.Timestamp:yyyy-MM-dd HH:mm:ss}] {level} {phase} - {message}");
    }

    private sealed class LogEntry
    {
        [JsonPropertyName("timestamp")]
        public DateTimeOffset Timestamp { get; set; }

        [JsonPropertyName("niveau")]
        public string Level { get; set; } = default!;

        [JsonPropertyName("phase")]
        public string Phase { get; set; } = default!;

        [JsonPropertyName("message")]
        public string Message { get; set; } = default!;

        [JsonPropertyName("donnees")]
        public object? Data { get; set; }

        [JsonPropertyName("exception")]
        public string? Exception { get; set; }
    }
}
