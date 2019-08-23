package de.unikl.dbis.clash.optimizer

import de.unikl.dbis.clash.optimizer.materializationtree.strategies.BottomUpTheta
import de.unikl.dbis.clash.optimizer.materializationtree.strategies.FlatTheta
import de.unikl.dbis.clash.optimizer.materializationtree.strategies.LeftDeepGreedyTheta
import de.unikl.dbis.clash.optimizer.materializationtree.strategies.TopDownTheta
import de.unikl.dbis.clash.optimizer.probeorder.LeastIntermediatesProbeOrder
import de.unikl.dbis.clash.optimizer.probeorder.LeastSentProbeOrder
import de.unikl.dbis.clash.optimizer.probeorder.ProbeOrderOptimizationStrategy
import kotlin.system.exitProcess


/**
 * Here are all strategies collected that can be used.
 */
object GlobalStrategyRegistry {
    const val DEFAULT = "Flat"
    val SUPPORTED = arrayOf("Flat", "LeftDeepGreedy")

    fun initialize(name: String = DEFAULT, args: Map<String, Any>? = null): GlobalStrategy {
        return when(name) {
            "Flat" -> FlatTheta()
            "LeftDeepGreedy" -> LeftDeepGreedyTheta()
            "BottomUpTheta" -> BottomUpTheta()
            "TopDownTheta" -> TopDownTheta()
            else -> {
                System.err.println("Unknown Global Strategy $name")
                exitProcess(1)
            }
        }
    }
}


object ProbeOrderStrategyRegistry {
    const val DEFAULT = "LeastSent"
    val SUPPORTED = arrayOf("LeastIntermediate", "LeastSent")

    fun initialize(name: String = DEFAULT, args: Map<String, Any>? = null): ProbeOrderOptimizationStrategy {
        return when(name) {
            "LeastIntermediate" -> LeastIntermediatesProbeOrder()
            "LeastSent" -> LeastSentProbeOrder()
            else -> {
                System.err.println("Unknown Probe Order Strategy $name")
                exitProcess(1)
            }
        }
    }
}
