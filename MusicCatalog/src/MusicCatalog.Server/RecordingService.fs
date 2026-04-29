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

    // Fail early when database access is not configured for the server.
    let private requireConnectionString (connectionString: string) =
        if String.IsNullOrWhiteSpace connectionString then
            invalidOp "Connection string 'MusicCatalogDb' is missing."

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

    // Project a table row to the expected column count.
    let private rowValues (columnCount: int) (cells: string array) =
        Array.init columnCount (cellValue cells)

    // Mp3tag reports codec names in a detailed form; store the smaller
    // catalog-facing names so filtering stays predictable.
    let private codecMappings =
        dict [
            "Advanced Systems Format", "ASF"
            "Audio AAC", "AAC"
            "Audio OPUS", "Opus"
            "Audio VORBIS", "OGG/Vorbis"
            "Free Lossless Audio Codec", "FLAC"
            "Monkey's Audio 3.96", "APE"
            "Monkey's Audio 3.97", "APE"
            "Monkey's Audio 3.99", "APE"
            "MPEG 1 Layer III", "MP3"
            "MPEG 2 Layer III", "MP3"
            "MPEG 2.5 Layer III", "MP3"
            "MPEG-4 AAC", "AAC"
            "MPEG-4 AAC LC ADTS", "AAC"
            "MPEG-4 ALAC", "ALAC"
            "Vorbis", "OGG/Vorbis"
            "Windows Media Audio", "WMA"
        ]

    // Convert a raw Mp3tag codec description to the catalog vocabulary.
    let private normalizeCodec (codec: string) =
        match codecMappings.TryGetValue(codec.Trim()) with
        | true, normalized -> normalized
        | false, _ -> "Unknown"

    // Normalize the codec column in every imported row when that column exists.
    let private normalizeCodecColumn (columns: string array) (rows: string array array) =
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
                normalized[index] <- normalizeCodec normalized[index]
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
    let private normalizeGenre (genre: string) =
        let containsPiano =
            genre.IndexOf("piano", StringComparison.OrdinalIgnoreCase) >= 0

        let containsClassicalTerm =
            classicalGenreTerms
            |> Array.exists (fun term ->
                genre.IndexOf(term, StringComparison.OrdinalIgnoreCase) >= 0)

        if containsPiano then
            "Piano"
        elif containsClassicalTerm then
            "Classical"
        else
            genre

    // Normalize the genre column in every imported row when that column exists.
    let private normalizeGenreColumn (columns: string array) (rows: string array array) =
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
                normalized[index] <- normalizeGenre normalized[index]
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
    let readSourceFile (sourcePath: string) =
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
            |> normalizeCodecColumn columnNames
            |> normalizeGenreColumn columnNames

        { columns = columnNames
          rows = normalizedRows }

    // Replace music_catalog with rows from the latest Mp3tag export.
    let reloadMusicCatalog connectionString sourcePath =
        task {
            requireConnectionString connectionString
            Console.WriteLine($"Reloading music_catalog from {sourcePath}")

            let sourceTable = readSourceFile sourcePath
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

            do! tx.CommitAsync()
            Console.WriteLine("Finished reloading music_catalog")

            return Array.empty<Client.Main.Recording>
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

    // Add one exact-match WHERE predicate when a search criterion is selected.
    let private addOptionalFilter
        (filters: ResizeArray<string>)
        (cmd: NpgsqlCommand)
        (name: string)
        (column: string)
        (value: string)
        =
        if not (String.IsNullOrWhiteSpace value) then
            filters.Add($"{quoteIdentifier column} = @{name}")
            cmd.Parameters.AddWithValue(name, value) |> ignore

    // Query music_catalog using optional dropdown criteria and return grid rows.
    let searchRecordings connectionString title artist genre codec =
        task {
            requireConnectionString connectionString

            use conn = new NpgsqlConnection(connectionString)
            do! conn.OpenAsync()

            let filters = ResizeArray<string>()
            use cmd = new NpgsqlCommand()
            cmd.Connection <- conn

            // Empty criteria mean "All"; selected criteria become exact matches.
            addOptionalFilter filters cmd "title" "title" title
            addOptionalFilter filters cmd "artist" "artist" artist
            addOptionalFilter filters cmd "genre" "genre" genre
            addOptionalFilter filters cmd "codec" "codec" codec

            let whereClause =
                if filters.Count = 0 then
                    ""
                else
                    " where " + String.concat " and " filters

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
                limit 500
                """

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

            return recordings.ToArray()
        }

type RecordingService
    (
        ctx: IRemoteContext,
        config: IConfiguration,
        env: IWebHostEnvironment
    ) =
    inherit RemoteHandler<Client.Main.RecordingService>()

    let connectionString = config.GetConnectionString("MusicCatalogDb")
    let sourcePath = Path.Combine(env.ContentRootPath, "data", "mp3tag.html")

    // Codec choices are configuration-driven because they are a fixed
    // application vocabulary, unlike genres which come from the loaded catalog.
    // Load configured codec options for the search dropdown.
    let codecOptions () =
        config.GetSection("MusicCatalog:CodecOptions").GetChildren()
        |> Seq.choose (fun item ->
            if String.IsNullOrWhiteSpace item.Value then
                None
            else
                Some(item.Value.Trim()))
        |> Seq.distinct
        |> Seq.sort
        |> Seq.toArray

    override _.Handler =
        { getRecordings =
            ctx.Authorize
            <| fun () ->
                async {
                    Console.WriteLine("Received getRecordings reload request")

                    return!
                        RecordingRepository.reloadMusicCatalog
                            connectionString
                            sourcePath
                        |> Async.AwaitTask
                }

          addRecording =
            ctx.Authorize
            <| fun _ ->
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
            ctx.Authorize
            <| fun () ->
                async {
                    return!
                        RecordingRepository.genreOptions connectionString
                        |> Async.AwaitTask
                }

          searchRecordings =
            ctx.Authorize
            <| fun (title, artist, genre, codec) ->
                async {
                    return!
                        RecordingRepository.searchRecordings
                            connectionString
                            title
                            artist
                            genre
                            codec
                        |> Async.AwaitTask
                }

          signIn =
            fun (username, password) ->
                async {
                    if password = "password" then
                        do!
                            ctx.HttpContext.AsyncSignIn(
                                username,
                                TimeSpan.FromDays(365.)
                            )

                        return Some username
                    else
                        return None
                }

          signOut =
            fun () ->
                async {
                    return! ctx.HttpContext.AsyncSignOut()
                }

          getUsername =
            ctx.Authorize
            <| fun () ->
                async {
                    return ctx.HttpContext.User.Identity.Name
                } }
