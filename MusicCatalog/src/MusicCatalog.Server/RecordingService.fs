namespace MusicCatalog.Server

open System
open System.IO
open System.Net
open System.Text
open System.Text.RegularExpressions
open Bolero
open Bolero.Remoting
open Bolero.Remoting.Server
open Microsoft.AspNetCore.Hosting
open Microsoft.Extensions.Configuration
open MusicCatalog
open Npgsql

module RecordingRepository =
    type SourceTable =
        { columns: string array
          rows: string array array }

    let private requireConnectionString (connectionString: string) =
        if String.IsNullOrWhiteSpace connectionString then
            invalidOp "Connection string 'MusicCatalogDb' is missing."

    let private cellValue (cells: string array) index =
        if index < cells.Length then
            cells[index].Trim()
        else
            ""

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

    let private quoteIdentifier (identifier: string) =
        "\"" + identifier.Replace("\"", "\"\"") + "\""

    let private rowValues (columnCount: int) (cells: string array) =
        Array.init columnCount (cellValue cells)

    let private htmlText pattern input =
        Regex.Matches(input, pattern, RegexOptions.IgnoreCase ||| RegexOptions.Singleline)
        |> Seq.cast<Match>
        |> Seq.map (fun matchResult ->
            matchResult.Groups[1].Value
            |> fun value -> Regex.Replace(value, "<.*?>", "")
            |> WebUtility.HtmlDecode
            |> fun value -> value.Trim())
        |> Seq.toArray

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

        { columns = columnNames
          rows = sourceRows }

    let reloadMusicCatalog connectionString sourcePath =
        task {
            requireConnectionString connectionString
            Console.WriteLine($"Reloading music_catalog from {sourcePath}")

            let sourceTable = readSourceFile sourcePath
            Console.WriteLine($"Source has {sourceTable.columns.Length} columns and {sourceTable.rows.Length} rows")

            use conn = new NpgsqlConnection(connectionString)
            do! conn.OpenAsync()

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

            for row in sourceTable.rows do
                for index, value in row |> Array.indexed do
                    parameters[index].Value <- value

                let! _ = insertCmd.ExecuteNonQueryAsync()
                ()

            do! tx.CommitAsync()
            Console.WriteLine("Finished reloading music_catalog")

            return Array.empty<Client.Main.Recording>
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
