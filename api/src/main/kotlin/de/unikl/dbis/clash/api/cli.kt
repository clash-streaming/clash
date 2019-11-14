package de.unikl.dbis.clash.api

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.versionOption
import de.unikl.dbis.clash.ClashConfig
import de.unikl.dbis.clash.readConfig
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.LoggerContext

fun main(args: Array<String>) = ClashCLI()
        .versionOption(version())
        .subcommands(
                Json(),
                Validate())
        .main(args)


class ClashCLI : CliktCommand() {
    override fun run() = Unit
}

abstract class CommonCLI(help: String = "",
                         epilog: String = "") : CliktCommand(help, epilog) {
    val config by option("--config", "-c").convert { readConfig(it) }.default( ClashConfig() ) // TODO
}

fun disableLogging() {
    val ctx = LogManager.getContext(false) as LoggerContext
    val config = ctx.configuration
    config.loggers["de.unikl.dbis.clash"]?.removeAppender("STDOUT")
    config.rootLogger.removeAppender("STDOUT")
}

fun version(): String {
    return "0.3.0"
    // TODO: This makes a computer with locale de-DE throw an exception?
    // return ResourceBundle.getBundle("version").getString("version")
}