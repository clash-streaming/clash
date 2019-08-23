package de.unikl.dbis.clash.api

import com.github.ajalt.clikt.core.PrintMessage
import com.github.ajalt.clikt.parameters.options.EagerOption
import de.unikl.dbis.clash.optimizer.GlobalStrategyRegistry
import de.unikl.dbis.clash.optimizer.ProbeOrderStrategyRegistry

/**
 * This file contains the [eager options](https://ajalt.github.io/clikt/options/#eager-options)
 * that can be used to ask CLASH which options are supported.
 */
fun registerSupported(clashCLI: ClashCLI) {
    clashCLI.registerOption(EagerOption("--supported-global-strategies") {
        throw PrintMessage(GlobalStrategyRegistry.SUPPORTED.joinToString("\n"))
    })

    clashCLI.registerOption(EagerOption("--supported-probe-order-strategies") {
        throw PrintMessage(ProbeOrderStrategyRegistry.SUPPORTED.joinToString("\n"))
    })
}
