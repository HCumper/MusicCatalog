# MusicCatalog E2E Tests

These tests drive the real MusicCatalog web UI with Playwright and compare the grid results with rows queried directly from PostgreSQL.
They also cover reload completion, paging, clear filters, and title/artist typeahead behavior.

## Run

Start the app first:

```powershell
dotnet run --project C:\Source\MusicCatalog\MusicCatalog\src\MusicCatalog.Server
```

Install Playwright browsers once after building the test project:

```powershell
dotnet build C:\Source\MusicCatalog\MusicCatalog\tests\MusicCatalog.E2ETests
pwsh C:\Source\MusicCatalog\MusicCatalog\tests\MusicCatalog.E2ETests\bin\Debug\net8.0\playwright.ps1 install chromium
```

Set the test database connection string without committing credentials:

```powershell
$env:MUSICCATALOG_E2E_BASEURL = "http://localhost:5000"
$env:MUSICCATALOG_E2E_CONNECTIONSTRING = "Host=localhost;Port=5432;Database=musiccatalog;Username=postgres;Password=your_password"
dotnet test C:\Source\MusicCatalog\MusicCatalog\tests\MusicCatalog.E2ETests
```

Optional filters:

```powershell
$env:MUSICCATALOG_E2E_TITLE = "mozart"
$env:MUSICCATALOG_E2E_ARTIST = "brendel"
$env:MUSICCATALOG_E2E_GENRE = "Classical"
$env:MUSICCATALOG_E2E_CODEC = "MP3"
$env:MUSICCATALOG_E2E_HEADLESS = "true"
$env:MUSICCATALOG_E2E_SLOWMO = "100"
```

If `MUSICCATALOG_E2E_CONNECTIONSTRING` is not set, the tests are skipped.

The browser is visible by default. Set `MUSICCATALOG_E2E_HEADLESS=true` to hide it.
Set `MUSICCATALOG_E2E_SLOWMO` to a larger number, such as `500`, to slow each browser action down while watching the tests.

## Covered Workflows

- Search result count and first-page grid rows match PostgreSQL.
- Paging `Next` shows the same rows as PostgreSQL `offset 100`.
- `Clear Filters` resets title, artist, genre, codec, result count, and grid state.
- Title and artist typeahead do not query/populate options for one character, then populate after two characters.
- `Reload Source` shows the completion dialog and writes import metadata.
