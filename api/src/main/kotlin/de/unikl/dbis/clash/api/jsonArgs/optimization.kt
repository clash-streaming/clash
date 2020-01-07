package de.unikl.dbis.clash.api.jsonArgs

import de.unikl.dbis.clash.datacharacteristics.DataCharacteristics
import de.unikl.dbis.clash.datacharacteristics.SymmetricJSONCharacteristics
import de.unikl.dbis.clash.optimizer.GlobalStrategy
import de.unikl.dbis.clash.optimizer.GlobalStrategyRegistry
import de.unikl.dbis.clash.optimizer.OptimizationParameters
import de.unikl.dbis.clash.optimizer.ProbeOrderStrategyRegistry
import de.unikl.dbis.clash.optimizer.probeorder.ProbeOrderOptimizationStrategy
import de.unikl.dbis.clash.query.RelationAlias

data class JsonOptimizationParameters(
    val taskCapacity: Long,
    val availableTasks: Long,
    val globalStrategy: JsonGlobalStragegy,
    val probeOrderOptimizationStrategy: JsonProbeOrderStrategy
) {
    fun get(): OptimizationParameters {
        return OptimizationParameters(
                taskCapacity,
                availableTasks,
                globalStrategy.get(),
                probeOrderOptimizationStrategy.get()
        )
    }
}

data class JsonGlobalStragegy(
    val name: String,
    val params: Map<String, Any>? = null
) {
    fun get(): GlobalStrategy {
        return GlobalStrategyRegistry.initialize(name, params)
    }
}

data class JsonProbeOrderStrategy(
    val name: String,
    val params: Map<String, Any>? = null
) {
    fun get(): ProbeOrderOptimizationStrategy {
        return ProbeOrderStrategyRegistry.initialize(name, params)
    }
}

data class JsonDataCharacteristicsArg(
    val rates: Map<String, Double>,
    val selectivities: Map<String, Map<String, Double>>
) {
    fun get(): DataCharacteristics {
        val dcRates = rates.map { RelationAlias(it.key) to it.value }.toMap()
        val dcSelectivities = selectivities
                .flatMap { outer -> outer.value
                        .map { inner -> Pair(RelationAlias(outer.key), RelationAlias(inner.key)) to inner.value } }
                .toMap()
        return SymmetricJSONCharacteristics(dcRates, dcSelectivities)
    }
}
