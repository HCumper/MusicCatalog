namespace MusicCatalog.E2ETests;

public sealed record E2ETestSettings(
    string BaseUrl,
    string ConnectionString,
    string Title,
    string Artist,
    string Genre,
    string Codec,
    bool Headless,
    float SlowMoMilliseconds)
{
    private static bool ReadHeadless() =>
        string.Equals(
            Environment.GetEnvironmentVariable("MUSICCATALOG_E2E_HEADLESS"),
            "true",
            StringComparison.OrdinalIgnoreCase);

    private static float ReadSlowMoMilliseconds() =>
        float.TryParse(
            Environment.GetEnvironmentVariable("MUSICCATALOG_E2E_SLOWMO"),
            out var slowMoMilliseconds)
            ? slowMoMilliseconds
            : 250;

    public static E2ETestSettings Load() =>
        new(
            Environment.GetEnvironmentVariable("MUSICCATALOG_E2E_BASEURL") ?? "http://localhost:5000",
            Environment.GetEnvironmentVariable("MUSICCATALOG_E2E_CONNECTIONSTRING") ?? string.Empty,
            Environment.GetEnvironmentVariable("MUSICCATALOG_E2E_TITLE") ?? string.Empty,
            Environment.GetEnvironmentVariable("MUSICCATALOG_E2E_ARTIST") ?? string.Empty,
            Environment.GetEnvironmentVariable("MUSICCATALOG_E2E_GENRE") ?? string.Empty,
            Environment.GetEnvironmentVariable("MUSICCATALOG_E2E_CODEC") ?? string.Empty,
            ReadHeadless(),
            ReadSlowMoMilliseconds());
}
