namespace MusicCatalog.Server

open System
open System.IO
open System.Globalization
open System.Net
open System.Text
open System.Text.RegularExpressions

open Bolero
open Bolero.Remoting
open Bolero.Remoting.Server
open Microsoft.AspNetCore.Hosting
open Microsoft.Extensions.Configuration
open Npgsql

open MusicCatalog

// Owns the server-side catalog workflow: import Mp3tag HTML into Postgres,
// normalize source values, and query catalog data for Bolero remoting.
module RecordingRepository =
    type SourceTable =
        { columns: string array
          rows: string array array }

    type ImportRules =
        { codecMappings: Collections.Generic.IReadOnlyDictionary<string, string>
          classicalGenreTerms: string array
          pianoGenreTerms: string array }

    type ImportCounters =
        { mutable unknownCodecCount: int
          mutable classicalGenreCount: int
          mutable pianoGenreCount: int }

    // Fail early when database access is not configured for the server.
    let private requireConnectionString (connectionString: string) =
        if String.IsNullOrWhiteSpace connectionString then
            invalidOp "Connection string 'MusicCatalogDb' is missing."

    // Format timestamps once on the server so every client shows the same value.
    let private displayTimestamp (timestamp: DateTime) =
        timestamp
            .ToUniversalTime()
            .ToString("yyyy-MM-dd HH:mm:ss 'UTC'", CultureInfo.InvariantCulture)

    // Read one table cell, treating missing trailing cells as blank values.
    let private cellValue (cells: string array) index =
        if index < cells.Length then
            cells[index].Trim()
        else
            ""

    // Convert a source header into a safe PostgreSQL column identifier.
    let private sanitizeColumnName index (name: string) =
        let trimmed = name.Trim()

        let sourceName =
            if String.IsNullOrWhiteSpace trimmed then
                $"column_{index + 1}"
            else
                trimmed

        let builder = StringBuilder()

        for ch in sourceName.ToLowerInvariant() do
            if Char.IsLetterOrDigit ch then
                builder.Append(ch) |> ignore
            elif builder.Length = 0 || builder[builder.Length - 1] <> '_' then
                builder.Append('_') |> ignore

        let sanitized = builder.ToString().Trim('_')

        if String.IsNullOrWhiteSpace sanitized then
            $"column_{index + 1}"
        elif Char.IsDigit sanitized[0] then
            $"column_{index + 1}_{sanitized}"
        else
            sanitized

    // Ensure sanitized source headers remain unique after casing/punctuation cleanup.
    let private uniqueColumnNames (columns: string array) =
        let counts = Collections.Generic.Dictionary<string, int>()

        columns
        |> Array.mapi (fun index column ->
            let baseName = sanitizeColumnName index column

            match counts.TryGetValue(baseName) with
            | true, count ->
                counts[baseName] <- count + 1
                $"{baseName}_{count + 1}"
            | false, _ ->
                counts[baseName] <- 1
                baseName)

    // Quote a PostgreSQL identifier for dynamically generated SQL.
    let private quoteIdentifier (identifier: string) =
        "\"" + identifier.Replace("\"", "\"\"") + "\""

    // Create indexes that match the catalog's common filters and typeahead searches.
    let private createSearchIndexes (conn: NpgsqlConnection) tx (columns: string array) =
        task {
            let hasColumn column =
                columns
                |> Array.exists (fun value ->
                    String.Equals(value, column, StringComparison.OrdinalIgnoreCase))

            let executeSql sql =
                task {
                    use cmd = new NpgsqlCommand(sql, conn, tx)
                    let! _ = cmd.ExecuteNonQueryAsync()
                    return ()
                }

            if hasColumn "title" || hasColumn "artist" then
                do!
                    executeSql
                        "create extension if not exists pg_trgm"

            if hasColumn "title" then
                do!
                    executeSql
                        "create index if not exists ix_music_catalog_title_trgm on music_catalog using gin (title gin_trgm_ops)"

            if hasColumn "artist" then
                do!
                    executeSql
                        "create index if not exists ix_music_catalog_artist_trgm on music_catalog using gin (artist gin_trgm_ops)"

            if hasColumn "genre" then
                do!
                    executeSql
                        "create index if not exists ix_music_catalog_genre on music_catalog (genre)"

            if hasColumn "codec" then
                do!
                    executeSql
                        "create index if not exists ix_music_catalog_codec on music_catalog (codec)"
        }

    // Project a table row to the expected column count.
    let private rowValues (columnCount: int) (cells: string array) =
        Array.init columnCount (cellValue cells)

    // Convert a raw Mp3tag codec description to the catalog vocabulary.
    let private normalizeCodec (rules: ImportRules) (counters: ImportCounters) (codec: string) =
        match rules.codecMappings.TryGetValue(codec.Trim()) with
        | true, normalized -> normalized
        | false, _ ->
            counters.unknownCodecCount <- counters.unknownCodecCount + 1
            "Unknown"

    // Normalize the codec column in every imported row when that column exists.
    let private normalizeCodecColumn
        (rules: ImportRules)
        (counters: ImportCounters)
        (columns: string array)
        (rows: string array array)
        =
        let codecIndex =
            columns
            |> Array.tryFindIndex (fun column ->
                String.Equals(column, "codec", StringComparison.OrdinalIgnoreCase))

        match codecIndex with
        | None -> rows
        | Some index ->
            rows
            |> Array.map (fun row ->
                let normalized = Array.copy row
                normalized[index] <- normalizeCodec rules counters normalized[index]
                normalized)

    // Normalize common translated/classified genre values before inserting.
    // Piano wins over Classical when both terms appear in the source value.
    let private classicalGenreTerms =
        [| "Classical"
           "クラシック"
           "古典乐"
           "Классика"
           "Klassiskt"
           "Klassik"
           "Música Clasica"
           "Classics"
           "classique"
           "Clásica"
           "Klasik"
           "Classic" |]

    // Collapse common classical/piano genre variants to the app's standard names.
    let private normalizeGenre (rules: ImportRules) (counters: ImportCounters) (genre: string) =
        let containsPiano =
            rules.pianoGenreTerms
            |> Array.exists (fun term ->
                genre.IndexOf(term, StringComparison.OrdinalIgnoreCase) >= 0)

        let containsClassicalTerm =
            rules.classicalGenreTerms
            |> Array.exists (fun term ->
                genre.IndexOf(term, StringComparison.OrdinalIgnoreCase) >= 0)

        if containsPiano then
            counters.pianoGenreCount <- counters.pianoGenreCount + 1
            "Piano"
        elif containsClassicalTerm then
            counters.classicalGenreCount <- counters.classicalGenreCount + 1
            "Classical"
        else
            genre

    // Normalize the genre column in every imported row when that column exists.
    let private normalizeGenreColumn
        (rules: ImportRules)
        (counters: ImportCounters)
        (columns: string array)
        (rows: string array array)
        =
        let genreIndex =
            columns
            |> Array.tryFindIndex (fun column ->
                String.Equals(column, "genre", StringComparison.OrdinalIgnoreCase))

        match genreIndex with
        | None -> rows
        | Some index ->
            rows
            |> Array.map (fun row ->
                let normalized = Array.copy row
                normalized[index] <- normalizeGenre rules counters normalized[index]
                normalized)

    // Strip diacritics so accented and unaccented artist spellings group together.
    let private removeAccents (value: string) =
        let decomposed = value.Normalize(NormalizationForm.FormD)
        let builder = StringBuilder()

        for ch in decomposed do
            if CharUnicodeInfo.GetUnicodeCategory(ch) <> UnicodeCategory.NonSpacingMark then
                builder.Append(ch) |> ignore

        builder.ToString().Normalize(NormalizationForm.FormC)

    // Avoid treating ensemble names like "BBC Philharmonic, Vassily Sinaisky"
    // as a simple "Last, First" personal-name form.
    let private looksLikeEnsembleName (value: string) =
        let ensembleTerms =
            [| "band"
               "choir"
               "chorus"
               "ensemble"
               "orchestra"
               "opera"
               "philharmonic"
               "quartet"
               "quintet"
               "symphony"
               "trio" |]

        ensembleTerms
        |> Array.exists (fun term ->
            value.IndexOf(term, StringComparison.OrdinalIgnoreCase) >= 0)

    // Normalize a single artist credit for case, accents, and "Last, First" display.
    let private normalizeArtistName (artist: string) =
        let withoutAccents = removeAccents artist
        let trimmed = withoutAccents.Trim()
        let parts = trimmed.Split(',', StringSplitOptions.TrimEntries ||| StringSplitOptions.RemoveEmptyEntries)

        let canonical =
            if parts.Length = 2 && not (looksLikeEnsembleName parts[0]) then
                $"{parts[0]}, {parts[1]}"
            else
                let nameParts = trimmed.Split(' ', StringSplitOptions.TrimEntries ||| StringSplitOptions.RemoveEmptyEntries)

                if nameParts.Length >= 2
                   && nameParts.Length <= 4
                   && not (looksLikeEnsembleName trimmed) then
                    let firstNames = nameParts[.. nameParts.Length - 2] |> String.concat " "
                    let lastName = nameParts[nameParts.Length - 1]
                    $"{lastName}, {firstNames}"
                else
                    trimmed

        CultureInfo.InvariantCulture.TextInfo.ToTitleCase(canonical.ToLowerInvariant())

    // Normalize artist cells while preserving common multi-artist separators.
    let private normalizeArtist (artist: string) =
        artist.Split(" - ", StringSplitOptions.None)
        |> Array.map normalizeArtistName
        |> String.concat " - "

    // Normalize the artist column in every imported row when that column exists.
    let private normalizeArtistColumn (columns: string array) (rows: string array array) =
        let artistIndex =
            columns
            |> Array.tryFindIndex (fun column ->
                String.Equals(column, "artist", StringComparison.OrdinalIgnoreCase))

        match artistIndex with
        | None -> rows
        | Some index ->
            rows
            |> Array.map (fun row ->
                let normalized = Array.copy row
                normalized[index] <- normalizeArtist normalized[index]
                normalized)

    // Format a genre value for display in dropdown options.
    let private displayGenre (genre: string) =
        CultureInfo.InvariantCulture.TextInfo.ToTitleCase(genre.Trim().ToLowerInvariant())

    // The Mp3tag HTML export can contain tag text that is not strict XHTML.
    // Read table fragments tolerantly instead of using XDocument.Load.
    // Extract and decode text from matching HTML cells.
    let private htmlText pattern input =
        Regex.Matches(input, pattern, RegexOptions.IgnoreCase ||| RegexOptions.Singleline)
        |> Seq.cast<Match>
        |> Seq.map (fun matchResult ->
            matchResult.Groups[1].Value
            |> fun value -> Regex.Replace(value, "<.*?>", "")
            |> WebUtility.HtmlDecode
            |> fun value -> value.Trim())
        |> Seq.toArray

    // Parse the Mp3tag export into sanitized columns and normalized row values.
    let readSourceFile (rules: ImportRules) (counters: ImportCounters) (sourcePath: string) =
        let html = File.ReadAllText(sourcePath)

        let rows =
            Regex.Matches(html, "<tr[^>]*>(.*?)</tr>", RegexOptions.IgnoreCase ||| RegexOptions.Singleline)
            |> Seq.cast<Match>
            |> Seq.map (fun matchResult -> matchResult.Groups[1].Value)
            |> Seq.toArray

        let columns =
            rows
            |> Array.tryHead
            |> Option.map (htmlText "<th[^>]*>(.*?)</th>")
            |> Option.defaultValue Array.empty

        if columns.Length = 0 then
            invalidOp $"No source columns were found in '{sourcePath}'."

        // Source headers become database columns, so sanitize and de-duplicate them.
        let columnNames = uniqueColumnNames columns

        let sourceRows =
            rows
            |> Seq.skip 1
            |> Seq.choose (fun row ->
                let cells = htmlText "<td[^>]*>(.*?)</td>" row

                if cells.Length = 0 then
                    None
                else
                    Some(rowValues columns.Length cells))
            |> Seq.toArray

        let normalizedRows =
            sourceRows
            |> normalizeCodecColumn rules counters columnNames
            |> normalizeGenreColumn rules counters columnNames
            |> normalizeArtistColumn columnNames

        { columns = columnNames
          rows = normalizedRows }

    // Replace music_catalog with rows from the latest Mp3tag export.
    let reloadMusicCatalog connectionString sourcePath rules =
        task {
            requireConnectionString connectionString
            Console.WriteLine($"Reloading music_catalog from {sourcePath}")

            let counters =
                { unknownCodecCount = 0
                  classicalGenreCount = 0
                  pianoGenreCount = 0 }

            let sourceTable = readSourceFile rules counters sourcePath
            Console.WriteLine($"Source has {sourceTable.columns.Length} columns and {sourceTable.rows.Length} rows")

            use conn = new NpgsqlConnection(connectionString)
            do! conn.OpenAsync()

            // Rebuild from the export on each reload so the schema follows
            // whatever columns are present in the latest Mp3tag file.
            use tx = conn.BeginTransaction()

            use dropCmd =
                new NpgsqlCommand(
                    "drop table if exists music_catalog",
                    conn,
                    tx
                )

            let! _ = dropCmd.ExecuteNonQueryAsync()

            let columnDefinitions =
                sourceTable.columns
                |> Array.map (fun column -> $"{quoteIdentifier column} text")
                |> String.concat ", "

            use createCmd =
                new NpgsqlCommand(
                    $"create table music_catalog (id serial primary key, {columnDefinitions})",
                    conn,
                    tx
                )

            let! _ = createCmd.ExecuteNonQueryAsync()

            let insertColumns =
                sourceTable.columns
                |> Array.map quoteIdentifier
                |> String.concat ", "

            let insertValues =
                sourceTable.columns
                |> Array.mapi (fun index _ -> $"@p{index}")
                |> String.concat ", "

            use insertCmd =
                new NpgsqlCommand(
                    $"insert into music_catalog ({insertColumns}) values ({insertValues})",
                    conn,
                    tx
                )

            let parameters =
                sourceTable.columns
                |> Array.mapi (fun index _ ->
                    insertCmd.Parameters.Add(
                        $"p{index}",
                        NpgsqlTypes.NpgsqlDbType.Text
                    ))

            // Reuse the prepared command shape and swap parameter values per row.
            for row in sourceTable.rows do
                for index, value in row |> Array.indexed do
                    parameters[index].Value <- value

                let! _ = insertCmd.ExecuteNonQueryAsync()
                ()

            do! createSearchIndexes conn tx sourceTable.columns

            let reloadedAt = DateTime.UtcNow

            use createMetadataCmd =
                new NpgsqlCommand(
                    """
                    create table if not exists music_catalog_import_metadata (
                        id integer primary key,
                        reloaded_at timestamp with time zone not null,
                        rows_loaded integer not null,
                        unknown_codec_count integer not null,
                        classical_genre_count integer not null,
                        piano_genre_count integer not null
                    )
                    """,
                    conn,
                    tx
                )

            let! _ = createMetadataCmd.ExecuteNonQueryAsync()

            use updateMetadataCmd =
                new NpgsqlCommand(
                    """
                    insert into music_catalog_import_metadata (
                        id,
                        reloaded_at,
                        rows_loaded,
                        unknown_codec_count,
                        classical_genre_count,
                        piano_genre_count
                    )
                    values (
                        1,
                        @reloadedAt,
                        @rowsLoaded,
                        @unknownCodecCount,
                        @classicalGenreCount,
                        @pianoGenreCount
                    )
                    on conflict (id) do update set
                        reloaded_at = excluded.reloaded_at,
                        rows_loaded = excluded.rows_loaded,
                        unknown_codec_count = excluded.unknown_codec_count,
                        classical_genre_count = excluded.classical_genre_count,
                        piano_genre_count = excluded.piano_genre_count
                    """,
                    conn,
                    tx
                )

            updateMetadataCmd.Parameters.AddWithValue("reloadedAt", reloadedAt) |> ignore
            updateMetadataCmd.Parameters.AddWithValue("rowsLoaded", sourceTable.rows.Length) |> ignore
            updateMetadataCmd.Parameters.AddWithValue("unknownCodecCount", counters.unknownCodecCount) |> ignore
            updateMetadataCmd.Parameters.AddWithValue("classicalGenreCount", counters.classicalGenreCount) |> ignore
            updateMetadataCmd.Parameters.AddWithValue("pianoGenreCount", counters.pianoGenreCount) |> ignore
            let! _ = updateMetadataCmd.ExecuteNonQueryAsync()

            do! tx.CommitAsync()
            Console.WriteLine("Finished reloading music_catalog")

            let diagnostics: Client.Main.ImportDiagnostics =
                { rowsLoaded = sourceTable.rows.Length
                  unknownCodecCount = counters.unknownCodecCount
                  classicalGenreCount = counters.classicalGenreCount
                  pianoGenreCount = counters.pianoGenreCount
                  lastReloaded = displayTimestamp reloadedAt }

            return diagnostics
        }

    // Read the timestamp of the last successful source reload.
    let lastReloaded connectionString =
        task {
            requireConnectionString connectionString

            use conn = new NpgsqlConnection(connectionString)
            do! conn.OpenAsync()

            let tableExistsSql =
                """
                select exists (
                    select 1
                    from information_schema.tables
                    where table_schema = 'public'
                      and table_name = 'music_catalog_import_metadata'
                )
                """

            use tableExistsCmd = new NpgsqlCommand(tableExistsSql, conn)
            let! tableExists = tableExistsCmd.ExecuteScalarAsync()

            if not (tableExists :?> bool) then
                return ""
            else
                use cmd =
                    new NpgsqlCommand(
                        "select reloaded_at from music_catalog_import_metadata where id = 1",
                        conn
                    )

                let! result = cmd.ExecuteScalarAsync()

                if isNull result || result = box DBNull.Value then
                    return ""
                else
                    return displayTimestamp (result :?> DateTime)
        }

    // Load distinct genre values for the search dropdown.
    let genreOptions connectionString =
        task {
            requireConnectionString connectionString

            use conn = new NpgsqlConnection(connectionString)
            do! conn.OpenAsync()

            let tableExistsSql =
                """
                select exists (
                    select 1
                    from information_schema.tables
                    where table_schema = 'public'
                      and table_name = 'music_catalog'
                )
                """

            use tableExistsCmd = new NpgsqlCommand(tableExistsSql, conn)
            let! tableExists = tableExistsCmd.ExecuteScalarAsync()

            if not (tableExists :?> bool) then
                return Array.empty
            else
                let genreColumnExistsSql =
                    """
                    select exists (
                        select 1
                        from information_schema.columns
                        where table_schema = 'public'
                          and table_name = 'music_catalog'
                          and column_name = 'genre'
                    )
                    """

                use genreColumnExistsCmd = new NpgsqlCommand(genreColumnExistsSql, conn)
                let! genreColumnExists = genreColumnExistsCmd.ExecuteScalarAsync()

                if not (genreColumnExists :?> bool) then
                    return Array.empty
                else
                    use cmd =
                        new NpgsqlCommand(
                            """
                            select distinct genre
                            from music_catalog
                            where genre is not null
                              and btrim(genre) <> ''
                            """,
                            conn
                        )

                    use! reader = cmd.ExecuteReaderAsync()
                    let genres = ResizeArray<string>()

                    while! reader.ReadAsync() do
                        genres.Add(reader.GetString(0))

                    // Keep the musically important buckets first, then sort the rest.
                    let priority genre =
                        if String.Equals(genre, "Classical", StringComparison.OrdinalIgnoreCase) then
                            0
                        elif String.Equals(genre, "Piano", StringComparison.OrdinalIgnoreCase) then
                            1
                        else
                            2

                    return
                        genres
                        |> Seq.map displayGenre
                        |> Seq.distinctBy (fun genre -> genre.ToUpperInvariant())
                        |> Seq.sortWith (fun left right ->
                            let priorityComparison =
                                compare (priority left) (priority right)

                            if priorityComparison <> 0 then
                                priorityComparison
                            else
                                StringComparer.OrdinalIgnoreCase.Compare(left, right))
                        |> Seq.toArray
        }

    // Load a capped list of artist values matching the current typeahead text.
    let artistOptions connectionString (search: string) =
        task {
            requireConnectionString connectionString

            if search.Trim().Length < 2 then
                return Array.empty
            else
                use conn = new NpgsqlConnection(connectionString)
                do! conn.OpenAsync()

                let tableExistsSql =
                    """
                    select exists (
                        select 1
                        from information_schema.tables
                        where table_schema = 'public'
                          and table_name = 'music_catalog'
                    )
                    """

                use tableExistsCmd = new NpgsqlCommand(tableExistsSql, conn)
                let! tableExists = tableExistsCmd.ExecuteScalarAsync()

                if not (tableExists :?> bool) then
                    return Array.empty
                else
                    let artistColumnExistsSql =
                        """
                        select exists (
                            select 1
                            from information_schema.columns
                            where table_schema = 'public'
                              and table_name = 'music_catalog'
                              and column_name = 'artist'
                        )
                        """

                    use artistColumnExistsCmd = new NpgsqlCommand(artistColumnExistsSql, conn)
                    let! artistColumnExists = artistColumnExistsCmd.ExecuteScalarAsync()

                    if not (artistColumnExists :?> bool) then
                        return Array.empty
                    else
                        use cmd =
                            new NpgsqlCommand(
                                """
                                select distinct artist
                                from music_catalog
                                where artist is not null
                                  and btrim(artist) <> ''
                                  and artist ilike @pattern
                                order by artist
                                limit 100
                                """,
                                conn
                            )

                        cmd.Parameters.AddWithValue("pattern", "%" + search + "%") |> ignore
                        use! reader = cmd.ExecuteReaderAsync()
                        let artists = ResizeArray<string>()

                        while! reader.ReadAsync() do
                            artists.Add(reader.GetString(0))

                        return artists.ToArray()
        }

    // Load a capped list of title values matching the current typeahead text.
    let titleOptions connectionString (search: string) =
        task {
            requireConnectionString connectionString

            if search.Trim().Length < 2 then
                return Array.empty
            else
                use conn = new NpgsqlConnection(connectionString)
                do! conn.OpenAsync()

                let tableExistsSql =
                    """
                    select exists (
                        select 1
                        from information_schema.tables
                        where table_schema = 'public'
                          and table_name = 'music_catalog'
                    )
                    """

                use tableExistsCmd = new NpgsqlCommand(tableExistsSql, conn)
                let! tableExists = tableExistsCmd.ExecuteScalarAsync()

                if not (tableExists :?> bool) then
                    return Array.empty
                else
                    let titleColumnExistsSql =
                        """
                        select exists (
                            select 1
                            from information_schema.columns
                            where table_schema = 'public'
                              and table_name = 'music_catalog'
                              and column_name = 'title'
                        )
                        """

                    use titleColumnExistsCmd = new NpgsqlCommand(titleColumnExistsSql, conn)
                    let! titleColumnExists = titleColumnExistsCmd.ExecuteScalarAsync()

                    if not (titleColumnExists :?> bool) then
                        return Array.empty
                    else
                        use cmd =
                            new NpgsqlCommand(
                                """
                                select distinct title
                                from music_catalog
                                where title is not null
                                  and btrim(title) <> ''
                                  and title ilike @pattern
                                order by title
                                limit 100
                                """,
                                conn
                            )

                        cmd.Parameters.AddWithValue("pattern", "%" + search + "%") |> ignore
                        use! reader = cmd.ExecuteReaderAsync()
                        let titles = ResizeArray<string>()

                        while! reader.ReadAsync() do
                            titles.Add(reader.GetString(0))

                        return titles.ToArray()
        }

    // Add one exact-match WHERE predicate when a dropdown criterion is selected.
    let private addOptionalExactFilter
        (filters: ResizeArray<string>)
        (cmd: NpgsqlCommand)
        (name: string)
        (column: string)
        (value: string)
        =
        if not (String.IsNullOrWhiteSpace value) then
            filters.Add($"{quoteIdentifier column} = @{name}")
            cmd.Parameters.AddWithValue(name, value) |> ignore

    // Add one case-insensitive contains predicate when typed search text is provided.
    let private addOptionalContainsFilter
        (filters: ResizeArray<string>)
        (cmd: NpgsqlCommand)
        (name: string)
        (column: string)
        (value: string)
        =
        if not (String.IsNullOrWhiteSpace value) then
            filters.Add($"{quoteIdentifier column} ilike @{name}")
            cmd.Parameters.AddWithValue(name, "%" + value + "%") |> ignore

    // Query music_catalog using optional dropdown criteria and return one result page.
    let searchRecordings connectionString title artist genre codec pageNumber pageSize =
        task {
            requireConnectionString connectionString

            use conn = new NpgsqlConnection(connectionString)
            do! conn.OpenAsync()

            let filters = ResizeArray<string>()
            use cmd = new NpgsqlCommand()
            cmd.Connection <- conn

            // Empty criteria mean "All"; typed title/artist use contains matching.
            addOptionalContainsFilter filters cmd "title" "title" title
            addOptionalContainsFilter filters cmd "artist" "artist" artist
            addOptionalExactFilter filters cmd "genre" "genre" genre
            addOptionalExactFilter filters cmd "codec" "codec" codec

            let whereClause =
                if filters.Count = 0 then
                    ""
                else
                    " where " + String.concat " and " filters

            use countCmd = new NpgsqlCommand()
            countCmd.Connection <- conn
            countCmd.CommandText <- $"select count(*) from music_catalog {whereClause}"

            for parameter in cmd.Parameters do
                countCmd.Parameters.AddWithValue(parameter.ParameterName, parameter.Value) |> ignore

            let! totalCountValue = countCmd.ExecuteScalarAsync()
            let totalCount = Convert.ToInt32(totalCountValue)
            let safePageSize = max 1 pageSize
            let safePageNumber = max 1 pageNumber
            let offset = (safePageNumber - 1) * safePageSize

            cmd.CommandText <-
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
                """

            cmd.Parameters.AddWithValue("pageSize", safePageSize) |> ignore
            cmd.Parameters.AddWithValue("offset", offset) |> ignore

            use! reader = cmd.ExecuteReaderAsync()
            let recordings = ResizeArray<Client.Main.Recording>()

            while! reader.ReadAsync() do
                recordings.Add {
                    title = reader.GetString(0)
                    artist = reader.GetString(1)
                    album = reader.GetString(2)
                    track = reader.GetString(3)
                    year = reader.GetString(4)
                    genre = reader.GetString(5)
                    codec = reader.GetString(6)
                    filename = reader.GetString(7)
                }

            let result: Client.Main.SearchResult =
                { rows = recordings.ToArray()
                  totalCount = totalCount }

            return result
        }

type RecordingService
    (
        config: IConfiguration,
        env: IWebHostEnvironment
    ) =
    inherit RemoteHandler<Client.Main.RecordingService>()

    let connectionString = config.GetConnectionString("MusicCatalogDb")
    let sourcePath = Path.Combine(env.ContentRootPath, "data", "mp3tag.html")

    // Load key/value mappings from configuration sections.
    let configMap sectionName =
        let values = Collections.Generic.Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)

        for item in config.GetSection(sectionName).GetChildren() do
            if not (String.IsNullOrWhiteSpace item.Key)
               && not (String.IsNullOrWhiteSpace item.Value) then
                values[item.Key.Trim()] <- item.Value.Trim()

        values :> Collections.Generic.IReadOnlyDictionary<string, string>

    // Load string-array settings from configuration sections.
    let configList sectionName =
        config.GetSection(sectionName).GetChildren()
        |> Seq.choose (fun item ->
            if String.IsNullOrWhiteSpace item.Value then
                None
            else
                Some(item.Value.Trim()))
        |> Seq.toArray

    // Build import rules from appsettings so normalization changes do not require code edits.
    let importRules () : RecordingRepository.ImportRules =
        { codecMappings = configMap "MusicCatalog:CodecMappings"
          classicalGenreTerms = configList "MusicCatalog:ClassicalGenreTerms"
          pianoGenreTerms = configList "MusicCatalog:PianoGenreTerms" }

    // Codec choices are configuration-driven because they are a fixed
    // application vocabulary, unlike genres which come from the loaded catalog.
    // Load configured codec options for the search dropdown.
    let codecOptions () =
        configList "MusicCatalog:CodecOptions"
        |> Seq.distinct
        |> Seq.sort
        |> Seq.toArray

    override _.Handler =
        { reloadSource =
            fun () ->
                async {
                    Console.WriteLine("Received reloadSource request")

                    return!
                        RecordingRepository.reloadMusicCatalog
                            connectionString
                            sourcePath
                            (importRules ())
                        |> Async.AwaitTask
                }

          addRecording =
            fun _ ->
                async {
                    return
                        raise
                        <| InvalidOperationException(
                            "Adding a single recording is not supported. Reload the music catalog from the source file instead."
                        )
                }

          getCodecOptions =
            fun () ->
                async {
                    return codecOptions ()
                }

          getGenreOptions =
            fun () ->
                async {
                    return!
                        RecordingRepository.genreOptions connectionString
                        |> Async.AwaitTask
                }

          getLastReloaded =
            fun () ->
                async {
                    return!
                        RecordingRepository.lastReloaded connectionString
                        |> Async.AwaitTask
                }

          getArtistOptions =
            fun search ->
                async {
                    return!
                        RecordingRepository.artistOptions connectionString search
                        |> Async.AwaitTask
                }

          getTitleOptions =
            fun search ->
                async {
                    return!
                        RecordingRepository.titleOptions connectionString search
                        |> Async.AwaitTask
                }

          searchRecordings =
            fun (title, artist, genre, codec, pageNumber, pageSize) ->
                async {
                    return!
                        RecordingRepository.searchRecordings
                            connectionString
                            title
                            artist
                            genre
                            codec
                            pageNumber
                            pageSize
                        |> Async.AwaitTask
                }
        }
