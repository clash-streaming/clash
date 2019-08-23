package de.unikl.dbis.clash.optimizer

import de.unikl.dbis.clash.datacharacteristics.DataCharacteristics
import de.unikl.dbis.clash.optimizer.probeorder.ProbeOrderOptimizationStrategy
import de.unikl.dbis.clash.physical.PhysicalGraph
import de.unikl.dbis.clash.query.AttributeAccessList
import de.unikl.dbis.clash.query.Query
import de.unikl.dbis.clash.query.RelationAlias

interface GlobalStrategy {
    fun optimize(query: Query, dataCharacteristics: DataCharacteristics, params: OptimizationParameters): OptimizationResult
}

data class OptimizationParameters(val taskCapacity: Long = Long.MAX_VALUE,
                                  val availableTasks: Long = Long.MAX_VALUE,

                                  val globalStrategy: GlobalStrategy = GlobalStrategyRegistry.initialize(),
                                  val probeOrderOptimizationStrategy: ProbeOrderOptimizationStrategy = ProbeOrderStrategyRegistry.initialize(),

                                  val crossProductsAllowed: Boolean = false
) {
    val totalCapacity: Long get() = taskCapacity * availableTasks
}


/**
 * Whenever all tuples from OldRelation are collocated in a store,
 * this store is is partitioned according to Attribute.
 */


data class OptimizationResult(
        val physicalGraph: PhysicalGraph,
        val costEstimation: CostEstimation,
        val intermediateResult: Any? = null)

data class CostEstimation(
        val storageCost: Double,
        val probeCost: Double,
        val numTasks: Long
)