module MusicCatalog.Server.Index

open Microsoft.AspNetCore.Components
open Microsoft.AspNetCore.Components.Web

open Bolero
open Bolero.Html
open Bolero.Server.Html

open MusicCatalog

let page = doctypeHtml {
    head {
        meta { attr.charset "UTF-8" }
        meta {
            attr.name "viewport"
            attr.content "width=device-width, initial-scale=1.0"
        }
        title { "Music Catalog" }
        ``base`` { attr.href "/" }
        link {
            attr.rel "stylesheet"
            attr.href "https://cdnjs.cloudflare.com/ajax/libs/bulma/0.7.4/css/bulma.min.css"
        }
        link { attr.rel "stylesheet"; attr.href "css/index.css" }
        link { attr.rel "stylesheet"; attr.href "MusicCatalog.Client.styles.css" }
    }
    body {
        div {
            attr.id "main"
            comp<Client.Main.MyApp> {
                attr.renderMode RenderMode.InteractiveWebAssembly
            }
        }
        boleroScript
    }
}

[<Route "/{*path}">]
type Page() =
    inherit Bolero.Component()

    override _.Render() = page
