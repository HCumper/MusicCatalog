module MusicCatalog.Client.Main

// Owns the client-side Elmish application: routing, UI state,
// remoting calls, and template rendering for the music catalog.
open System

open Elmish
open Bolero
open Bolero.Html
open Bolero.Remoting
open Bolero.Remoting.Client
open Bolero.Templating.Client

/// The Elmish application's model.
type Model =
    { recordings: Recording[] option
      titleSearch: string
      titleFilter: string
      artistSearch: string
      artistFilter: string
      genreFilter: string
      codecFilter: string
      titleOptions: string[]
      artistOptions: string[]
      genreOptions: string[]
      codecOptions: string[]
      reloadDiagnostics: ImportDiagnostics option
      lastReloaded: string
      lastSearchCount: int option
      pageNumber: int
      pageSize: int
      error: string option
      }

and Recording =
    { title: string
      artist: string
      album: string
      track: string
      year: string
      genre: string
      codec: string
      filename: string }

and SearchResult =
    { rows: Recording[]
      totalCount: int }

and ImportDiagnostics =
    { rowsLoaded: int
      unknownCodecCount: int
      classicalGenreCount: int
      pianoGenreCount: int
      lastReloaded: string }

// Build the initial application state before startup commands load server data.
let initModel =
    { recordings = None
      titleSearch = ""
      titleFilter = ""
      artistSearch = ""
      artistFilter = ""
      genreFilter = ""
      codecFilter = ""
      titleOptions = Array.empty
      artistOptions = Array.empty
      genreOptions = Array.empty
      codecOptions = Array.empty
      reloadDiagnostics = None
      lastReloaded = ""
      lastSearchCount = None
      pageNumber = 1
      pageSize = 100
      error = None }

/// Remote service definition.
type RecordingService =
    {
        /// Reload the catalog table from the source export and return diagnostics.
        reloadSource: unit -> Async<ImportDiagnostics>

        /// Add a recording in the collection.
        addRecording: Recording -> Async<unit>

        /// Get the configured codec search options.
        getCodecOptions: unit -> Async<string[]>

        /// Get distinct genres from the music catalog.
        getGenreOptions: unit -> Async<string[]>

        /// Get the timestamp from the most recent source reload.
        getLastReloaded: unit -> Async<string>

        /// Get matching artists for the current typeahead text.
        getArtistOptions: string -> Async<string[]>

        /// Get matching titles for the current typeahead text.
        getTitleOptions: string -> Async<string[]>

        /// Search music catalog rows by selected criteria and page.
        searchRecordings: string * string * string * string * int * int -> Async<SearchResult>
    }

    interface IRemoteService with
        member this.BasePath = "/recordings"

/// The Elmish application's update messages.
type Message =
    | ReloadSource
    | SourceReloaded of ImportDiagnostics
    | GotRecordings of SearchResult
    | SetTitleSearch of string
    | SetTitleFilter of string
    | SetArtistSearch of string
    | SetArtistFilter of string
    | SetGenreFilter of string
    | SetCodecFilter of string
    | GetCodecOptions
    | RecvCodecOptions of string[]
    | GetGenreOptions
    | RecvGenreOptions of string[]
    | GetLastReloaded
    | RecvLastReloaded of string
    | GetArtistOptions of string
    | RecvArtistOptions of string[]
    | GetTitleOptions of string
    | RecvTitleOptions of string[]
    | SearchCatalog
    | StartSearch
    | PreviousPage
    | NextPage
    | ClearFilters
    | DismissReloadComplete
    | Error of exn
    | ClearError

// Apply Elmish messages to the model and start remote commands when needed.
let update remote message model =
    match message with
    | ReloadSource ->
        let cmd = Cmd.OfAsync.either remote.reloadSource () SourceReloaded Error
        { model with recordings = None }, cmd
    | SourceReloaded diagnostics ->
        { model with
            recordings = Some Array.empty
            lastSearchCount = None
            reloadDiagnostics = Some diagnostics
            lastReloaded = diagnostics.lastReloaded },
        Cmd.ofMsg GetGenreOptions
    | GotRecordings result ->
        { model with
            recordings = Some result.rows
            lastSearchCount = Some result.totalCount },
        Cmd.ofMsg GetGenreOptions

    | SetTitleSearch value ->
        { model with
            titleSearch = value
            titleFilter = "" },
        Cmd.ofMsg (GetTitleOptions value)
    | SetTitleFilter value -> { model with titleFilter = value }, Cmd.none
    | SetArtistSearch value ->
        { model with
            artistSearch = value
            artistFilter = "" },
        Cmd.ofMsg (GetArtistOptions value)
    | SetArtistFilter value -> { model with artistFilter = value }, Cmd.none
    | SetGenreFilter value -> { model with genreFilter = value }, Cmd.none
    | SetCodecFilter value -> { model with codecFilter = value }, Cmd.none
    | GetCodecOptions ->
        model,
        Cmd.OfAsync.either remote.getCodecOptions () RecvCodecOptions Error
    | RecvCodecOptions codecs ->
        { model with codecOptions = codecs }, Cmd.none
    | GetGenreOptions ->
        model,
        Cmd.OfAsync.either remote.getGenreOptions () RecvGenreOptions Error
    | RecvGenreOptions genres ->
        let selectedGenre =
            if Array.contains model.genreFilter genres then
                model.genreFilter
            else
                ""

        { model with
            genreOptions = genres
            genreFilter = selectedGenre },
        Cmd.none
    | GetLastReloaded ->
        model,
        Cmd.OfAsync.either remote.getLastReloaded () RecvLastReloaded Error
    | RecvLastReloaded lastReloaded ->
        { model with lastReloaded = lastReloaded }, Cmd.none
    | GetArtistOptions search ->
        model,
        Cmd.OfAsync.either remote.getArtistOptions search RecvArtistOptions Error
    | RecvArtistOptions artists ->
        let selectedArtist =
            if Array.contains model.artistFilter artists then
                model.artistFilter
            else
                ""

        { model with
            artistOptions = artists
            artistFilter = selectedArtist },
        Cmd.none
    | GetTitleOptions search ->
        model,
        Cmd.OfAsync.either remote.getTitleOptions search RecvTitleOptions Error
    | RecvTitleOptions titles ->
        let selectedTitle =
            if Array.contains model.titleFilter titles then
                model.titleFilter
            else
                ""

        { model with
            titleOptions = titles
            titleFilter = selectedTitle },
        Cmd.none
    | StartSearch ->
        { model with pageNumber = 1 }, Cmd.ofMsg SearchCatalog
    | SearchCatalog ->
        let titleCriteria =
            if String.IsNullOrWhiteSpace model.titleSearch then
                model.titleFilter
            else
                model.titleSearch

        let artistCriteria =
            if String.IsNullOrWhiteSpace model.artistSearch then
                model.artistFilter
            else
                model.artistSearch

        let criteria =
            titleCriteria,
            artistCriteria,
            model.genreFilter,
            model.codecFilter,
            model.pageNumber,
            model.pageSize

        { model with recordings = None },
        Cmd.OfAsync.either remote.searchRecordings criteria GotRecordings Error
    | PreviousPage ->
        if model.pageNumber <= 1 then
            model, Cmd.none
        else
            { model with pageNumber = model.pageNumber - 1 }, Cmd.ofMsg SearchCatalog
    | NextPage ->
        let totalCount = defaultArg model.lastSearchCount 0
        let maxPage = max 1 ((totalCount + model.pageSize - 1) / model.pageSize)

        if model.pageNumber >= maxPage then
            model, Cmd.none
        else
            { model with pageNumber = model.pageNumber + 1 }, Cmd.ofMsg SearchCatalog
    | ClearFilters ->
        { model with
            titleSearch = ""
            titleFilter = ""
            artistSearch = ""
            artistFilter = ""
            genreFilter = ""
            codecFilter = ""
            pageNumber = 1
            lastSearchCount = None
            recordings = None },
        Cmd.none
    | DismissReloadComplete ->
        { model with reloadDiagnostics = None }, Cmd.none

    | Error exn -> { model with error = Some exn.Message }, Cmd.none
    | ClearError -> { model with error = None }, Cmd.none

// Bind the application shell template.
type Main = Template<"wwwroot/main.html">

// Bind the separate music catalog page template.
type MusicCatalog = Template<"wwwroot/music-catalog.html">

// Render an "All" option followed by selectable values for a dropdown.
let optionList selected values =
    concat {
        option {
            attr.value ""
            attr.selected (String.IsNullOrWhiteSpace selected)
            "All"
        }

        for value in values do
            option {
                attr.value value
                attr.selected (selected = value)
                value
            }
    }

// Render the searchable music catalog page and wire its controls to messages.
let dataPage (model: Model) dispatch =
    MusicCatalog
        .MusicCatalog()
        .Reload(fun _ -> dispatch ReloadSource)
        .LastReloaded(
            if String.IsNullOrWhiteSpace model.lastReloaded then
                "Last reloaded: never"
            else
                "Last reloaded: " + model.lastReloaded
        )
        .Search(fun _ -> dispatch StartSearch)
        .PreviousPage(fun _ -> dispatch PreviousPage)
        .NextPage(fun _ -> dispatch NextPage)
        .ClearFilters(fun _ -> dispatch ClearFilters)
        .TitleSearch(model.titleSearch, fun value -> dispatch (SetTitleSearch value))
        .Title(model.titleFilter, fun value -> dispatch (SetTitleFilter value))
        .ArtistSearch(model.artistSearch, fun value -> dispatch (SetArtistSearch value))
        .Artist(model.artistFilter, fun value -> dispatch (SetArtistFilter value))
        .Genre(model.genreFilter, fun value -> dispatch (SetGenreFilter value))
        .Codec(model.codecFilter, fun value -> dispatch (SetCodecFilter value))
        .TitleOptions(optionList model.titleFilter model.titleOptions)
        .ArtistOptions(optionList model.artistFilter model.artistOptions)
        .GenreOptions(optionList model.genreFilter model.genreOptions)
        .CodecOptions(optionList model.codecFilter model.codecOptions)
        .ResultCount(
            cond model.lastSearchCount
            <| function
                | None -> empty ()
                | Some count ->
                    let startRow =
                        if count = 0 then
                            0
                        else
                            ((model.pageNumber - 1) * model.pageSize) + 1

                    let endRow = min count (model.pageNumber * model.pageSize)

                    MusicCatalog
                        .ResultCount()
                        .Count(string count)
                        .StartRow(string startRow)
                        .EndRow(string endRow)
                        .PageNumber(string model.pageNumber)
                        .Elt()
        )
        .ReloadCompleteDialog(
            cond model.reloadDiagnostics
            <| function
                | None -> empty ()
                | Some diagnostics ->
                    MusicCatalog
                        .ReloadCompleteDialog()
                        .RowCount(string diagnostics.rowsLoaded)
                        .UnknownCodecCount(string diagnostics.unknownCodecCount)
                        .ClassicalGenreCount(string diagnostics.classicalGenreCount)
                        .PianoGenreCount(string diagnostics.pianoGenreCount)
                        .LastReloaded(diagnostics.lastReloaded)
                        .Close(fun _ -> dispatch DismissReloadComplete)
                        .Elt()
        )
        .Rows(
            cond model.recordings
            <| function
                | None -> MusicCatalog.EmptyData().Elt()
                | Some recordings ->
                    forEach recordings
                    <| fun recording ->
                        tr {
                            td { recording.title }
                            td { recording.artist }
                            td { recording.album }
                            td { recording.track }
                            td { recording.year }
                            td { recording.genre }
                            td { recording.codec }
                            td { recording.filename }
                        }
        )
        .Elt()

// Render the shell around the current page and global error notification.
let view (model: Model) dispatch =
    Main()
        .Body(dataPage model dispatch)
        .Error(
            cond model.error
            <| function
                | None -> empty ()
                | Some err ->
                    Main
                        .ErrorNotification()
                        .Text(err)
                        .Hide(fun _ -> dispatch ClearError)
                        .Elt()
        )
        .Elt()

// Host the Elmish program inside the Bolero component lifecycle.
type MyApp() =
    inherit ProgramComponent<Model, Message>()

    override _.CssScope = CssScopes.MyApp

    override this.Program =
        // Create the remoting proxy and load user/options metadata at startup.
        let recordingService = this.Remote<RecordingService>()
        let update = update recordingService

        Program.mkProgram
            (fun _ ->
                initModel,
                Cmd.batch [
                    Cmd.ofMsg GetCodecOptions
                    Cmd.ofMsg GetGenreOptions
                    Cmd.ofMsg GetLastReloaded
                ])
            update
            view
#if DEBUG
        |> Program.withHotReload


#endif
