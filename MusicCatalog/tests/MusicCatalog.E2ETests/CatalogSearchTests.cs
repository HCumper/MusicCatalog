using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Npgsql;
using NpgsqlTypes;

namespace MusicCatalog.E2ETests;

public sealed class CatalogSearchTests
{
    private const int PageSize = 100;

    // Searches through the real browser UI, then queries PostgreSQL with the same
    // filters/order/page size and compares the visible first page cell-by-cell.
    [SkippableFact]
    public async Task Search_grid_matches_database_first_page()
    {
        var settings = E2ETestSettings.Load();
        Skip.If(string.IsNullOrWhiteSpace(settings.ConnectionString), "Set MUSICCATALOG_E2E_CONNECTIONSTRING to run E2E database comparisons.");

        var expected = await LoadExpectedRows(settings, 0);

        using var playwright = await Playwright.CreateAsync();
        var browser = await LaunchBrowser(playwright, settings);

        try
        {
            var page = await browser.NewPageAsync();
            await OpenCatalog(page, settings);
            await ApplySearchCriteria(page, settings);
            await page.GetByRole(AriaRole.Button, new PageGetByRoleOptions { Name = "Search" }).ClickAsync();

            var resultCount = page.Locator(".catalog-result-count");
            await resultCount.WaitForAsync(new LocatorWaitForOptions { State = WaitForSelectorState.Visible });

            var actualTotal = ParseResultCount(await resultCount.InnerTextAsync());
            var actualRows = await ReadGridRows(page);

            Assert.Equal(expected.TotalCount, actualTotal);
            AssertRowsEqual(expected.Rows, actualRows);
        }
        finally
        {
            await browser.CloseAsync();
        }
    }

    // Runs the same UI search, clicks Next, and verifies the second visible page
    // matches the database query with the app's page-size offset.
    [SkippableFact]
    public async Task Paging_next_page_matches_database_second_page()
    {
        var settings = E2ETestSettings.Load();
        Skip.If(string.IsNullOrWhiteSpace(settings.ConnectionString), "Set MUSICCATALOG_E2E_CONNECTIONSTRING to run E2E database comparisons.");

        var firstPage = await LoadExpectedRows(settings, 0);
        Skip.If(firstPage.TotalCount <= PageSize, "Paging test needs more than one page of matching rows.");

        var secondPage = await LoadExpectedRows(settings, PageSize);

        using var playwright = await Playwright.CreateAsync();
        var browser = await LaunchBrowser(playwright, settings);

        try
        {
            var page = await browser.NewPageAsync();
            await OpenCatalog(page, settings);
            await ApplySearchCriteria(page, settings);
            await page.GetByRole(AriaRole.Button, new PageGetByRoleOptions { Name = "Search" }).ClickAsync();
            await page.Locator(".catalog-result-count").WaitForAsync(new LocatorWaitForOptions { State = WaitForSelectorState.Visible });

            await page.GetByRole(AriaRole.Button, new PageGetByRoleOptions { Name = "Next" }).ClickAsync();
            await ExpectResultCount(page, secondPage.TotalCount, 2);

            var actualRows = await ReadGridRows(page);
            AssertRowsEqual(secondPage.Rows, actualRows);
        }
        finally
        {
            await browser.CloseAsync();
        }
    }

    // Fills every filter with real catalog values, searches, then checks that
    // Clear Filters returns the form and grid to their initial empty state.
    [SkippableFact]
    public async Task Clear_filters_resets_inputs_selects_and_grid_state()
    {
        var settings = E2ETestSettings.Load();
        Skip.If(string.IsNullOrWhiteSpace(settings.ConnectionString), "Set MUSICCATALOG_E2E_CONNECTIONSTRING to run E2E database comparisons.");

        var criteria = await LoadSampleCriteria(settings.ConnectionString);

        using var playwright = await Playwright.CreateAsync();
        var browser = await LaunchBrowser(playwright, settings);

        try
        {
            var page = await browser.NewPageAsync();
            await OpenCatalog(page, settings);

            await page.GetByPlaceholder("Type title").FillAsync(criteria.Title);
            await page.GetByPlaceholder("Type artist name").FillAsync(criteria.Artist);
            await SelectIfAvailable(page.Locator("select").Nth(2), criteria.Genre);
            await SelectIfAvailable(page.Locator("select").Nth(3), criteria.Codec);

            await page.GetByRole(AriaRole.Button, new PageGetByRoleOptions { Name = "Search" }).ClickAsync();
            await page.Locator(".catalog-result-count").WaitForAsync(new LocatorWaitForOptions { State = WaitForSelectorState.Visible });

            await page.GetByRole(AriaRole.Button, new PageGetByRoleOptions { Name = "Clear Filters" }).ClickAsync();

            Assert.Equal(string.Empty, await page.GetByPlaceholder("Type title").InputValueAsync());
            Assert.Equal(string.Empty, await page.GetByPlaceholder("Type artist name").InputValueAsync());
            Assert.Equal(string.Empty, await page.Locator("select").Nth(2).InputValueAsync());
            Assert.Equal(string.Empty, await page.Locator("select").Nth(3).InputValueAsync());
            await AssertNoResultCount(page);
            Assert.Contains("No rows to display", await page.Locator(".catalog-grid tbody").InnerTextAsync());
        }
        finally
        {
            await browser.CloseAsync();
        }
    }

    // Proves the title and artist typeaheads stay quiet for one character and
    // only populate options after the two-character threshold is reached.
    [SkippableFact]
    public async Task Title_and_artist_typeahead_wait_until_two_characters()
    {
        var settings = E2ETestSettings.Load();
        Skip.If(string.IsNullOrWhiteSpace(settings.ConnectionString), "Set MUSICCATALOG_E2E_CONNECTIONSTRING to run E2E database comparisons.");

        var criteria = await LoadSampleCriteria(settings.ConnectionString);
        Skip.If(criteria.Title.Length < 2 || criteria.Artist.Length < 2, "Typeahead test needs title and artist samples with at least two characters.");

        using var playwright = await Playwright.CreateAsync();
        var browser = await LaunchBrowser(playwright, settings);

        try
        {
            var page = await browser.NewPageAsync();
            await OpenCatalog(page, settings);

            await page.GetByPlaceholder("Type title").FillAsync(criteria.Title[..1]);
            await ExpectOptionCount(page.Locator("select").Nth(0), 1);
            await page.GetByPlaceholder("Type title").FillAsync(criteria.Title[..2]);
            await ExpectOptionCountGreaterThan(page.Locator("select").Nth(0), 1);

            await page.GetByPlaceholder("Type artist name").FillAsync(criteria.Artist[..1]);
            await ExpectOptionCount(page.Locator("select").Nth(1), 1);
            await page.GetByPlaceholder("Type artist name").FillAsync(criteria.Artist[..2]);
            await ExpectOptionCountGreaterThan(page.Locator("select").Nth(1), 1);
        }
        finally
        {
            await browser.CloseAsync();
        }
    }

    // Clicks Reload Source, waits for the completion dialog, and confirms the
    // dialog values are backed by the import metadata row written to PostgreSQL.
    [SkippableFact]
    public async Task Reload_source_shows_completion_dialog_and_updates_metadata()
    {
        var settings = E2ETestSettings.Load();
        Skip.If(string.IsNullOrWhiteSpace(settings.ConnectionString), "Set MUSICCATALOG_E2E_CONNECTIONSTRING to run E2E database comparisons.");

        using var playwright = await Playwright.CreateAsync();
        var browser = await LaunchBrowser(playwright, settings);

        try
        {
            var page = await browser.NewPageAsync();
            await OpenCatalog(page, settings);

            await page.GetByRole(AriaRole.Button, new PageGetByRoleOptions { Name = "Reload Source" }).ClickAsync();

            var dialog = page.Locator(".modal-card");
            await dialog.WaitForAsync(new LocatorWaitForOptions { State = WaitForSelectorState.Visible, Timeout = 120_000 });

            var dialogText = await dialog.InnerTextAsync();
            Assert.Contains("Load complete", dialogText);
            Assert.Matches(@"\d+ rows were loaded", dialogText);
            Assert.Contains("Unknown codec", dialogText);
            Assert.Contains("Classical normalized", dialogText);
            Assert.Contains("Piano normalized", dialogText);
            Assert.Contains("Last reloaded:", dialogText);

            var metadata = await LoadImportMetadata(settings.ConnectionString);
            Assert.True(metadata.RowsLoaded > 0);
            Assert.Contains(metadata.RowsLoaded.ToString(), dialogText);
            Assert.True(metadata.ReloadedAtUtc > DateTime.UtcNow.AddMinutes(-10));
        }
        finally
        {
            await browser.CloseAsync();
        }
    }

    private static async Task OpenCatalog(IPage page, E2ETestSettings settings)
    {
        await page.GotoAsync(settings.BaseUrl, new PageGotoOptions { WaitUntil = WaitUntilState.NetworkIdle });
        await page.GetByRole(AriaRole.Heading, new PageGetByRoleOptions { Name = "Music Catalog" }).WaitForAsync();
    }

    private static async Task<IBrowser> LaunchBrowser(IPlaywright playwright, E2ETestSettings settings)
    {
        try
        {
            return await playwright.Chromium.LaunchAsync(
                new BrowserTypeLaunchOptions
                {
                    Headless = settings.Headless,
                    SlowMo = settings.SlowMoMilliseconds
                });
        }
        catch (PlaywrightException ex) when (ex.Message.Contains("Executable doesn't exist", StringComparison.OrdinalIgnoreCase))
        {
            Skip.If(true, "Install Playwright browsers first: pwsh ./bin/Debug/net8.0/playwright.ps1 install chromium");
            throw;
        }
    }

    private static async Task ApplySearchCriteria(IPage page, E2ETestSettings settings)
    {
        if (!string.IsNullOrWhiteSpace(settings.Title))
        {
            await page.GetByPlaceholder("Type title").FillAsync(settings.Title);
        }

        if (!string.IsNullOrWhiteSpace(settings.Artist))
        {
            await page.GetByPlaceholder("Type artist name").FillAsync(settings.Artist);
        }

        if (!string.IsNullOrWhiteSpace(settings.Genre))
        {
            await page.Locator("select").Nth(2).SelectOptionAsync(new SelectOptionValue { Label = settings.Genre });
        }

        if (!string.IsNullOrWhiteSpace(settings.Codec))
        {
            await page.Locator("select").Nth(3).SelectOptionAsync(new SelectOptionValue { Label = settings.Codec });
        }
    }

    private static async Task<GridExpectation> LoadExpectedRows(E2ETestSettings settings, int offset)
    {
        await using var conn = new NpgsqlConnection(settings.ConnectionString);
        await conn.OpenAsync();

        var filters = new List<string>();
        await using var rowCommand = conn.CreateCommand();

        AddContainsFilter(filters, rowCommand, "title", settings.Title);
        AddContainsFilter(filters, rowCommand, "artist", settings.Artist);
        AddExactFilter(filters, rowCommand, "genre", settings.Genre);
        AddExactFilter(filters, rowCommand, "codec", settings.Codec);

        var whereClause = filters.Count == 0
            ? string.Empty
            : " where " + string.Join(" and ", filters);

        await using var countCommand = conn.CreateCommand();
        countCommand.CommandText = $"select count(*) from music_catalog{whereClause}";

        foreach (NpgsqlParameter parameter in rowCommand.Parameters)
        {
            countCommand.Parameters.Add(new NpgsqlParameter(parameter.ParameterName, parameter.NpgsqlDbType) { Value = parameter.Value });
        }

        var totalCount = Convert.ToInt32(await countCommand.ExecuteScalarAsync());

        rowCommand.CommandText =
            $"""
             select
                 coalesce(title, '') as title,
                 coalesce(artist, '') as artist,
                 coalesce(album, '') as album,
                 coalesce(track, '') as track,
                 coalesce(year, '') as year,
                 coalesce(genre, '') as genre,
                 coalesce(codec, '') as codec,
                 coalesce(filename, '') as filename
             from music_catalog
             {whereClause}
             order by title, artist, album
             limit @pageSize offset @offset
             """;

        rowCommand.Parameters.Add(new NpgsqlParameter("pageSize", NpgsqlDbType.Integer) { Value = PageSize });
        rowCommand.Parameters.Add(new NpgsqlParameter("offset", NpgsqlDbType.Integer) { Value = offset });

        var rows = new List<string[]>();
        await using var reader = await rowCommand.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            rows.Add([
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetString(3),
                reader.GetString(4),
                reader.GetString(5),
                reader.GetString(6),
                reader.GetString(7)
            ]);
        }

        return new GridExpectation(totalCount, rows);
    }

    private static async Task<SampleCriteria> LoadSampleCriteria(string connectionString)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            """
            select
                coalesce(title, '') as title,
                coalesce(artist, '') as artist,
                coalesce(genre, '') as genre,
                coalesce(codec, '') as codec
            from music_catalog
            where length(coalesce(title, '')) >= 2
              and length(coalesce(artist, '')) >= 2
              and coalesce(genre, '') <> ''
              and coalesce(codec, '') <> ''
            order by title, artist, album
            limit 1
            """;

        await using var reader = await cmd.ExecuteReaderAsync();
        Skip.If(!await reader.ReadAsync(), "Sample criteria test needs at least one row with title, artist, genre, and codec.");

        return new SampleCriteria(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetString(3));
    }

    private static async Task<ImportMetadata> LoadImportMetadata(string connectionString)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            """
            select reloaded_at, rows_loaded
            from music_catalog_import_metadata
            where id = 1
            """;

        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync(), "Expected music_catalog_import_metadata to contain the reload row.");

        return new ImportMetadata(
            reader.GetDateTime(0).ToUniversalTime(),
            reader.GetInt32(1));
    }

    private static void AddContainsFilter(List<string> filters, NpgsqlCommand command, string column, string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return;
        }

        filters.Add($"{column} ilike @{column}");
        command.Parameters.Add(new NpgsqlParameter(column, NpgsqlDbType.Text) { Value = $"%{value}%" });
    }

    private static void AddExactFilter(List<string> filters, NpgsqlCommand command, string column, string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return;
        }

        filters.Add($"{column} = @{column}");
        command.Parameters.Add(new NpgsqlParameter(column, NpgsqlDbType.Text) { Value = value });
    }

    private static async Task<List<string[]>> ReadGridRows(IPage page)
    {
        var rows = await page.Locator(".catalog-grid tbody tr").EvaluateAllAsync<string[][]>(
            """
            rows => rows
                .map(row => Array.from(row.cells).map(cell => cell.innerText.trim()))
                .filter(cells => cells.length === 8)
            """);

        return rows.Select(row => row.ToArray()).ToList();
    }

    private static void AssertRowsEqual(List<string[]> expectedRows, List<string[]> actualRows)
    {
        Assert.Equal(expectedRows.Count, actualRows.Count);

        for (var index = 0; index < expectedRows.Count; index++)
        {
            Assert.Equal(expectedRows[index], actualRows[index]);
        }
    }

    private static async Task ExpectResultCount(IPage page, int count, int pageNumber)
    {
        await page.Locator(".catalog-result-count").WaitForAsync(new LocatorWaitForOptions { State = WaitForSelectorState.Visible });
        var text = await page.Locator(".catalog-result-count").InnerTextAsync();
        Assert.Equal(count, ParseResultCount(text));
        Assert.Contains($"Page {pageNumber}", text);
    }

    private static async Task AssertNoResultCount(IPage page)
    {
        await WaitUntilAsync(
            async () => await page.Locator(".catalog-result-count").CountAsync() == 0,
            "Expected result count to be hidden.");
    }

    private static async Task SelectIfAvailable(ILocator select, string label)
    {
        var optionCount = await select.Locator("option", new LocatorLocatorOptions { HasTextString = label }).CountAsync();

        if (optionCount > 0)
        {
            await select.SelectOptionAsync(new SelectOptionValue { Label = label });
        }
    }

    private static async Task ExpectOptionCount(ILocator select, int count)
    {
        await WaitUntilAsync(
            async () => await select.Locator("option").CountAsync() == count,
            $"Expected exactly {count} option(s).");
    }

    private static async Task ExpectOptionCountGreaterThan(ILocator select, int count)
    {
        await WaitUntilAsync(
            async () => await select.Locator("option").CountAsync() > count,
            $"Expected more than {count} option(s).");
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, string failureMessage)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        while (!timeout.IsCancellationRequested)
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(100, timeout.Token).ContinueWith(_ => { });
        }

        Assert.Fail(failureMessage);
    }

    private static int ParseResultCount(string text)
    {
        var match = Regex.Match(text, @"^\s*(\d+)\s+matches\b");
        Assert.True(match.Success, $"Could not parse result count from '{text}'.");
        return int.Parse(match.Groups[1].Value);
    }

    private sealed record GridExpectation(int TotalCount, List<string[]> Rows);

    private sealed record SampleCriteria(string Title, string Artist, string Genre, string Codec);

    private sealed record ImportMetadata(DateTime ReloadedAtUtc, int RowsLoaded);
}
