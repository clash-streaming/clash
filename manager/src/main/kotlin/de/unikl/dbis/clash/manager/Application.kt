package de.unikl.dbis.clash.manager

import io.ktor.application.Application
import io.ktor.application.call
import io.ktor.application.install
import io.ktor.auth.Authentication
import io.ktor.client.HttpClient
import io.ktor.client.engine.jetty.Jetty
import io.ktor.features.CORS
import io.ktor.features.CallLogging
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.cio.websocket.Frame
import io.ktor.http.cio.websocket.readText
import io.ktor.http.content.resources
import io.ktor.http.content.static
import io.ktor.locations.Location
import io.ktor.locations.Locations
import io.ktor.response.respond
import io.ktor.response.respondText
import io.ktor.routing.get
import io.ktor.routing.post
import io.ktor.routing.routing
import io.ktor.server.engine.ShutDownUrl
import io.ktor.thymeleaf.Thymeleaf
import io.ktor.thymeleaf.ThymeleafContent
import io.ktor.webjars.Webjars
import io.ktor.websocket.webSocket
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver
import java.time.Duration
import java.time.ZoneId

// private val logger: Logger = LogManager.getLogger(Application::class.java)
//
// class TestApp {
//     init {
//         logger.warn("ICH BIN GESTARTET!")
//     }
// }

fun main(args: Array<String>): Unit = io.ktor.server.netty.EngineMain.main(args)


@Suppress("unused") // Referenced in application.conf
@kotlin.jvm.JvmOverloads
fun Application.module(testing: Boolean = false) {

    val client = HttpClient(Jetty) {
    }

    install(Thymeleaf) {
        setTemplateResolver(ClassLoaderTemplateResolver().apply {
            prefix = "templates/thymeleaf/"
            suffix = ".html"
            characterEncoding = "utf-8"
        })
    }

    install(CallLogging)

    install(Authentication) {
    }

    install(Locations) {
    }

    install(CORS) {
        method(HttpMethod.Options)
        method(HttpMethod.Put)
        method(HttpMethod.Delete)
        method(HttpMethod.Patch)
        header(HttpHeaders.Authorization)
        header("MyCustomHeader")
        allowCredentials = true
        anyHost() // @TODO: Don't do this in production if possible. Try to limit it.
    }

    install(Webjars) {
        path = "/webjars" //defaults to /webjars
        zone = ZoneId.systemDefault() //defaults to ZoneId.systemDefault()
    }

    install(ShutDownUrl.ApplicationCallFeature) {
        // The URL that will be intercepted (you can also use the application.conf's ktor.deployment.shutdown.url key)
        shutDownUrl = "/ktor/application/shutdown"
        // A function that will be executed to get the exit code of the process
        exitCodeSupplier = { 0 } // ApplicationCall.() -> Int
    }

    install(io.ktor.websocket.WebSockets) {
        pingPeriod = Duration.ofSeconds(15)
        timeout = Duration.ofSeconds(15)
        maxFrameSize = Long.MAX_VALUE
        masking = false
    }

    routing {
        get("/") {
            call.respondText("HELLO WORLD!", contentType = ContentType.Text.Plain)
        }

        get("/html-thymeleaf") {
            call.respond(ThymeleafContent("index", mapOf("user" to ThymeleafUser(1, "user1"))))
        }

        get("/api/v1/collect-clash-spout-commands") {
            call.respondText("[{\"instruction\": \"TEST RESPONSE\"}]", contentType = ContentType.Application.Json)
        }

        post("/api/v1/collect-clash-spout-commands") {
            call.respondText("[{\"instruction\": \"TEST RESPONSE\"}]", contentType = ContentType.Application.Json)
        }

        // Static feature. Try to access `/static/ktor_logo.svg`
        static("/static") {
            resources("static")
        }
        //
        // get<MyLocation> {
        //     call.respondText("Location: name=${it.name}, arg1=${it.arg1}, arg2=${it.arg2}")
        // }
        // // Register nested routes
        // get<Type.Edit> {
        //     call.respondText("Inside $it")
        // }
        // get<Type.List> {
        //     call.respondText("Inside $it")
        // }
        //
        // get("/webjars") {
        //     call.respondText("<script src='/webjars/jquery/jquery.js'></script>", ContentType.Text.Html)
        // }

        webSocket("/myws/echo") {
            send(Frame.Text("Hi from server"))
            while (true) {
                val frame = incoming.receive()
                if (frame is Frame.Text) {
                    send(Frame.Text("Client said: " + frame.readText()))
                }
            }
        }
    }
}

data class ThymeleafUser(val id: Int, val name: String)

@Location("/location/{name}")
class MyLocation(val name: String, val arg1: Int = 42, val arg2: String = "default")

@Location("/type/{name}") data class Type(val name: String) {
    @Location("/edit")
    data class Edit(val type: Type)

    @Location("/list/{page}")
    data class List(val type: Type, val page: Int)
}
