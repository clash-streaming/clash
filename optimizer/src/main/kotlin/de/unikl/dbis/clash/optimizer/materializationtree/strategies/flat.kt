package de.unikl.dbis.clash.optimizer.materializationtree.strategies

import de.unikl.dbis.clash.datacharacteristics.DataCharacteristics
import de.unikl.dbis.clash.optimizer.CostEstimation
import de.unikl.dbis.clash.optimizer.OptimizationParameters
import de.unikl.dbis.clash.optimizer.PartitioningAttributesSelection
import de.unikl.dbis.clash.optimizer.globalNumTasks
import de.unikl.dbis.clash.optimizer.globalProbeTuplesSent
import de.unikl.dbis.clash.optimizer.globalTuplesMaterialized
import de.unikl.dbis.clash.optimizer.materializationtree.MatSource
import de.unikl.dbis.clash.optimizer.materializationtree.MaterializationTree
import de.unikl.dbis.clash.optimizer.materializationtree.NonMatMultiStream
import de.unikl.dbis.clash.optimizer.materializationtree.TreeOptimizationResult
import de.unikl.dbis.clash.optimizer.materializationtree.TreeStrategy
import de.unikl.dbis.clash.optimizer.materializationtree.createMultiStreamImpl
import de.unikl.dbis.clash.optimizer.materializationtree.parallelismFor
import de.unikl.dbis.clash.optimizer.materializationtree.storageCostFor
import de.unikl.dbis.clash.optimizer.noPartitioning
import de.unikl.dbis.clash.query.Query
import de.unikl.dbis.clash.query.Relation

/**
 * All strategies in this file issue minimal materialization, a.k.a., they produce a flat tree.
 *
 * For example, for the query R |Join S \Join T \Join U the following tree will be created:
 *
 *        JOIN
 *      / |  | \
 *     R  S  T  U
 *
 * The chosen probe order for the MultiStream operator depends on the optimization parameters.
 */

/**
 * This strategy does not partition the stores for the base relations.
 */
class FlatTheta : TreeStrategy() {
    override fun optimizeTree(query: Query, dataCharacteristics: DataCharacteristics, optimizationParameters: OptimizationParameters): TreeOptimizationResult {
        val tree = createFlatTree(query.result, dataCharacteristics, optimizationParameters, noPartitioning())

        val sCost = globalTuplesMaterialized(dataCharacteristics, tree.root)
        val pCost = globalProbeTuplesSent(dataCharacteristics, tree.root)
        val numTasks = globalNumTasks(tree.root)
        val costEstimation = CostEstimation(sCost, pCost, numTasks)
        return TreeOptimizationResult(tree, costEstimation)
    }
}

/**
 * Creates a flat MaterializationTree, i.e., a tree comprised of a single non-materializing
 * MultiStream operator as root with materializing sources as children.
 *
 * This can be used as starting point for iterative building of other shapes of materialization trees.
 */
fun createFlatTree(relation: Relation, dataCharacteristics: DataCharacteristics, params: OptimizationParameters, partitioning: PartitioningAttributesSelection): MaterializationTree { val leafRelations = relation.baseRelations()
    val children = leafRelations.map {
        val storageCost = storageCostFor(it, dataCharacteristics)
        val partitioningAttributes = partitioning[it.inputAliases.toList()] ?: listOf()
        MatSource(it, parallelismFor(it, dataCharacteristics, params.taskCapacity), partitioningAttributes, storageCost)
    }
    return MaterializationTree(NonMatMultiStream(relation, children, createMultiStreamImpl(children, relation.joinPredicates, params.probeOrderOptimizationStrategy, dataCharacteristics)))
}
