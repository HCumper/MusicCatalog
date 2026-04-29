# MusicCatalog

MusicCatalog is a Bolero/F# web app for loading an Mp3tag HTML export into PostgreSQL and searching the resulting music catalog.

## Project Layout

- `MusicCatalog/src/MusicCatalog.Client` - Bolero WebAssembly client UI.
- `MusicCatalog/src/MusicCatalog.Server` - ASP.NET Core/Bolero server and PostgreSQL access.
- `MusicCatalog/src/MusicCatalog.Server/data/mp3tag.html` - source export loaded by the app.
- `MusicCatalog/db` - database bootstrap SQL and notes.

## Configuration

Do not commit real database credentials. Copy the example config:

```powershell
Copy-Item `
  MusicCatalog/src/MusicCatalog.Server/appsettings.Development.example.json `
  MusicCatalog/src/MusicCatalog.Server/appsettings.Development.json
```

Then update the local `MusicCatalogDb` connection string in `appsettings.Development.json`, or use .NET user secrets:

```powershell
cd MusicCatalog/src/MusicCatalog.Server
dotnet user-secrets init
dotnet user-secrets set "ConnectionStrings:MusicCatalogDb" "Host=localhost;Port=5432;Database=musiccatalog;Username=postgres;Password=your_password"
```

## Database

Create the local database and schema using the scripts in `MusicCatalog/db`, or let the app recreate `music_catalog` when reloading the source export.

The reload process drops and recreates `music_catalog` from the headers and rows in `mp3tag.html`.

## Run

From the solution folder:

```powershell
dotnet run --project MusicCatalog/src/MusicCatalog.Server
```

Then open the URL printed by ASP.NET Core, commonly:

```text
http://localhost:5000
```

## Catalog Workflow

1. Open **Music Catalog**.
2. Click **Reload Source** to load `mp3tag.html` into PostgreSQL.
3. Use the dropdown criteria and click **Search** to populate the read-only grid.

During import, codec and genre values are normalized for consistent filtering.

## End-to-End Tests

The solution includes an opt-in xUnit/Playwright test project at `MusicCatalog/tests/MusicCatalog.E2ETests`.
It opens the real web UI, clicks Search, reads the visible grid, queries PostgreSQL directly, and compares the first page of rows plus the result count.

Set `MUSICCATALOG_E2E_CONNECTIONSTRING` before running the E2E tests. Without it, the E2E test is skipped so normal builds do not require a local database or running server.
