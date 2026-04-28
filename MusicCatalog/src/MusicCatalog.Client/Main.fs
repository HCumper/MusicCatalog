module MusicCatalog.Client.Main

open System
open Elmish
open Bolero
open Bolero.Html
open Bolero.Remoting
open Bolero.Remoting.Client
open Bolero.Templating.Client

/// Routing endpoints definition.
type Page =
    | [<EndPoint "/">] Home
    | [<EndPoint "/counter">] Counter
    | [<EndPoint "/data">] Data

/// The Elmish application's model.
type Model =
    { page: Page
      counter: int
      recordings: Recording[] option
      titleFilter: string
      artistFilter: string
      genreFilter: string
      codecFilter: string
      genreOptions: string[]
      codecOptions: string[]
      menuCollapsed: bool
      error: string option
      username: string
      password: string
      signedInAs: option<string>
      signInFailed: bool }

and Recording =
    { title: string
      artist: string
      album: string
      track: string
      year: string
      genre: string
      codec: string
      filename: string }

let sampleRecordings =
    [| { title = "- 2. Galop. Presto"
         artist = "Kathryn Stott - BBC Philharmonic"
         album = "Kabalevsky: Piano Concertos Nos. 2 & 3"
         track = "06/17"
         year = "2003"
         genre = "Classical"
         codec = "MP3"
         filename = "- 2. Galop. Presto.mp3" }
       { title = "Decomposing Composers"
         artist = "Monty Python"
         album = "The Final Rip Off"
         track = "20"
         year = "1988"
         genre = "Comedy"
         codec = "WMA"
         filename = "Decomposing Composers Monty Python 20.wma" }
       { title = "Great Composers Love Folk Songs Too"
         artist = "Various Artists"
         album = "Classical Folk"
         track = "03"
         year = "1997"
         genre = "Classical"
         codec = "FLAC"
         filename = "Great Composers Love Folk Songs Too.flac" } |]

let initModel =
    { page = Home
      counter = 0
      recordings = None
      titleFilter = ""
      artistFilter = ""
      genreFilter = ""
      codecFilter = ""
      genreOptions = Array.empty
      codecOptions = Array.empty
      menuCollapsed = false
      error = None
      username = ""
      password = ""
      signedInAs = None
      signInFailed = false }

/// Remote service definition.
type RecordingService =
    {
        /// Get the list of all recordings in the collection.
        getRecordings: unit -> Async<Recording[]>

        /// Add a recording in the collection.
        addRecording: Recording -> Async<unit>

        /// Get the configured codec search options.
        getCodecOptions: unit -> Async<string[]>

        /// Get distinct genres from the music catalog.
        getGenreOptions: unit -> Async<string[]>

        /// Search music catalog rows by selected criteria.
        searchRecordings: string * string * string * string -> Async<Recording[]>

        /// Sign into the application.
        signIn: string * string -> Async<option<string>>

        /// Get the user's name, or None if they are not authenticated.
        getUsername: unit -> Async<string>

        /// Sign out from the application.
        signOut: unit -> Async<unit>
    }

    interface IRemoteService with
        member this.BasePath = "/recordings"

/// The Elmish application's update messages.
type Message =
    | SetPage of Page
    | Increment
    | Decrement
    | SetCounter of int
    | GetRecordings
    | GotRecordings of Recording[]
    | SetTitleFilter of string
    | SetArtistFilter of string
    | SetGenreFilter of string
    | SetCodecFilter of string
    | GetCodecOptions
    | RecvCodecOptions of string[]
    | GetGenreOptions
    | RecvGenreOptions of string[]
    | SearchCatalog
    | ToggleMenu
    | SetUsername of string
    | SetPassword of string
    | ClearLoginForm
    | GetSignedInAs
    | RecvSignedInAs of option<string>
    | SendSignIn
    | RecvSignIn of option<string>
    | SendSignOut
    | RecvSignOut
    | Error of exn
    | ClearError

let update remote message model =
    let onSignIn =
        function
        | Some _ -> Cmd.ofMsg ClearLoginForm
        | None -> Cmd.none

    match message with
    | SetPage page -> { model with page = page }, Cmd.none

    | Increment ->
        { model with
            counter = model.counter + 1 },
        Cmd.none
    | Decrement ->
        { model with
            counter = model.counter - 1 },
        Cmd.none
    | SetCounter value -> { model with counter = value }, Cmd.none

    | GetRecordings ->
        let cmd = Cmd.OfAsync.either remote.getRecordings () GotRecordings Error
        { model with recordings = None }, cmd
    | GotRecordings recordings ->
        { model with recordings = Some recordings }, Cmd.ofMsg GetGenreOptions

    | SetTitleFilter value -> { model with titleFilter = value }, Cmd.none
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
    | SearchCatalog ->
        let criteria =
            model.titleFilter,
            model.artistFilter,
            model.genreFilter,
            model.codecFilter

        { model with recordings = None },
        Cmd.OfAsync.either remote.searchRecordings criteria GotRecordings Error
    | ToggleMenu -> { model with menuCollapsed = not model.menuCollapsed }, Cmd.none

    | SetUsername s -> { model with username = s }, Cmd.none
    | SetPassword s -> { model with password = s }, Cmd.none
    | ClearLoginForm ->
        { model with
            username = ""
            password = "" },
        Cmd.none
    | GetSignedInAs ->
        model,
        Cmd.OfAuthorized.either remote.getUsername () RecvSignedInAs Error
    | RecvSignedInAs username ->
        { model with signedInAs = username }, onSignIn username
    | SendSignIn ->
        model,
        Cmd.OfAsync.either
            remote.signIn
            (model.username, model.password)
            RecvSignIn
            Error
    | RecvSignIn username ->
        { model with
            signedInAs = username
            signInFailed = Option.isNone username },
        onSignIn username
    | SendSignOut ->
        model,
        Cmd.OfAsync.either remote.signOut () (fun () -> RecvSignOut) Error
    | RecvSignOut ->
        { model with
            signedInAs = None
            signInFailed = false },
        Cmd.none

    | Error RemoteUnauthorizedException ->
        { model with
            error = Some "You have been logged out."
            signedInAs = None },
        Cmd.none
    | Error exn -> { model with error = Some exn.Message }, Cmd.none
    | ClearError -> { model with error = None }, Cmd.none

/// Connects the routing system to the Elmish application.
let router = Router.infer SetPage (fun model -> model.page)

type Main = Template<"wwwroot/main.html">
type MusicCatalog = Template<"wwwroot/music-catalog.html">

let homePage model dispatch = Main.Home().Elt()

let counterPage model dispatch =
    Main
        .Counter()
        .Decrement(fun _ -> dispatch Decrement)
        .Increment(fun _ -> dispatch Increment)
        .Value(model.counter, fun v -> dispatch (SetCounter v))
        .Elt()

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

let distinctValues selector =
    sampleRecordings
    |> Array.map selector
    |> Array.distinct
    |> Array.sort

let dataPage model (username: string) dispatch =
    MusicCatalog
        .MusicCatalog()
        .Reload(fun _ -> dispatch GetRecordings)
        .Search(fun _ -> dispatch SearchCatalog)
        .Username(username)
        .SignOut(fun _ -> dispatch SendSignOut)
        .Title(model.titleFilter, fun value -> dispatch (SetTitleFilter value))
        .Artist(model.artistFilter, fun value -> dispatch (SetArtistFilter value))
        .Genre(model.genreFilter, fun value -> dispatch (SetGenreFilter value))
        .Codec(model.codecFilter, fun value -> dispatch (SetCodecFilter value))
        .TitleOptions(optionList model.titleFilter (distinctValues (fun recording -> recording.title)))
        .ArtistOptions(optionList model.artistFilter (distinctValues (fun recording -> recording.artist)))
        .GenreOptions(optionList model.genreFilter model.genreOptions)
        .CodecOptions(optionList model.codecFilter model.codecOptions)
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

let signInPage model dispatch =
    Main
        .SignIn()
        .Username(model.username, fun s -> dispatch (SetUsername s))
        .Password(model.password, fun s -> dispatch (SetPassword s))
        .SignIn(fun _ -> dispatch SendSignIn)
        .ErrorNotification(
            cond model.signInFailed
            <| function
                | false -> empty ()
                | true ->
                    Main
                        .ErrorNotification()
                        .HideClass("is-hidden")
                        .Text(
                            "Sign in failed. Use any username and the password \"password\"."
                        )
                        .Elt()
        )
        .Elt()

let menuItem (model: Model) (page: Page) (text: string) =
    Main
        .MenuItem()
        .Active(if model.page = page then "is-active" else "")
        .Url(router.Link page)
        .Text(text)
        .Elt()

let view model dispatch =
    Main()
        .SidebarCollapsed(if model.menuCollapsed then "is-collapsed" else "")
        .ToggleMenu(fun _ -> dispatch ToggleMenu)
        .ToggleMenuText(if model.menuCollapsed then "Show menu" else "Hide menu")
        .Menu(
            concat {
                menuItem model Home "Home"
                menuItem model Counter "Counter"
                menuItem model Data "Music Catalog"
            }
        )
        .Body(
            cond model.page
            <| function
                | Home -> homePage model dispatch
                | Counter -> counterPage model dispatch
                | Data ->
                    cond model.signedInAs
                    <| function
                        | Some username -> dataPage model username dispatch
                        | None -> signInPage model dispatch
        )
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

type MyApp() =
    inherit ProgramComponent<Model, Message>()

    override _.CssScope = CssScopes.MyApp

    override this.Program =
        let recordingService = this.Remote<RecordingService>()
        let update = update recordingService

        Program.mkProgram
            (fun _ ->
                initModel,
                Cmd.batch [
                    Cmd.ofMsg GetSignedInAs
                    Cmd.ofMsg GetCodecOptions
                    Cmd.ofMsg GetGenreOptions
                ])
            update
            view
        |> Program.withRouter router
#if DEBUG
        |> Program.withHotReload


#endif
