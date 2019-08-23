package de.unikl.dbis.clash.optimizer.materializationtree

import de.unikl.dbis.clash.datacharacteristics.DataCharacteristics
import de.unikl.dbis.clash.optimizer.*
import de.unikl.dbis.clash.query.Query


abstract class TreeStrategy : GlobalStrategy {
    abstract fun optimizeTree(query: Query, dataCharacteristics: DataCharacteristics, params: OptimizationParameters): TreeOptimizationResult

    override fun optimize(query: Query, dataCharacteristics: DataCharacteristics, params: OptimizationParameters): OptimizationResult {
        checkAvailableTasks(query, dataCharacteristics, params)

        val result = optimizeTree(query, dataCharacteristics, params)
        return OptimizationResult(build(result.tree), result.costEstimation, result.tree)
    }
}

fun checkAvailableTasks(query: Query, dataCharacteristics: DataCharacteristics, params: OptimizationParameters) {
    val minimalRequiredTasks = minimalRequiredTasks(query, dataCharacteristics, params.taskCapacity)
    if(minimalRequiredTasks > params.availableTasks) {
        throw OptimizationError("This query requires $minimalRequiredTasks tasks, but only ${params.availableTasks} are available.")
    }

}

data class TreeOptimizationResult(
        val tree: MaterializationTree,
        val costEstimation: CostEstimation)

class OptimizationError(message: String) : RuntimeException("Cannot optimize: $message")