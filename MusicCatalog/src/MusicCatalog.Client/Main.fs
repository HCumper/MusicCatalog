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
      filename: string }

let initModel =
    { page = Home
      counter = 0
      recordings = None
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
    | GotRecordings recordings -> { model with recordings = Some recordings }, Cmd.none

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

let homePage model dispatch = Main.Home().Elt()

let counterPage model dispatch =
    Main
        .Counter()
        .Decrement(fun _ -> dispatch Decrement)
        .Increment(fun _ -> dispatch Increment)
        .Value(model.counter, fun v -> dispatch (SetCounter v))
        .Elt()

let dataPage model (username: string) dispatch =
    Main
        .Data()
        .Reload(fun _ -> dispatch GetRecordings)
        .Username(username)
        .SignOut(fun _ -> dispatch SendSignOut)
        .Rows(
            cond model.recordings
            <| function
                | None -> Main.EmptyData().Elt()
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
        .Menu(
            concat {
                menuItem model Home "Home"
                menuItem model Counter "Counter"
                menuItem model Data "Load data"
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
            (fun _ -> initModel, Cmd.ofMsg GetSignedInAs)
            update
            view
        |> Program.withRouter router
#if DEBUG
        |> Program.withHotReload


#endif
