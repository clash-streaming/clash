package de.unikl.dbis.clash.optimizer.materializationtree.strategies

import de.unikl.dbis.clash.datacharacteristics.DataCharacteristics
import de.unikl.dbis.clash.estimator.estimateSize
import de.unikl.dbis.clash.optimizer.CostEstimation
import de.unikl.dbis.clash.optimizer.OptimizationParameters
import de.unikl.dbis.clash.optimizer.PartitioningAttributesSelection
import de.unikl.dbis.clash.optimizer.globalNumTasks
import de.unikl.dbis.clash.optimizer.globalProbeTuplesSent
import de.unikl.dbis.clash.optimizer.globalTuplesMaterialized
import de.unikl.dbis.clash.optimizer.joinSize
import de.unikl.dbis.clash.optimizer.materializationtree.MatMultiStream
import de.unikl.dbis.clash.optimizer.materializationtree.MatSource
import de.unikl.dbis.clash.optimizer.materializationtree.MaterializationTree
import de.unikl.dbis.clash.optimizer.materializationtree.MtNode
import de.unikl.dbis.clash.optimizer.materializationtree.NonMatMultiStream
import de.unikl.dbis.clash.optimizer.materializationtree.TreeOptimizationResult
import de.unikl.dbis.clash.optimizer.materializationtree.TreeStrategy
import de.unikl.dbis.clash.optimizer.materializationtree.createMultiStreamImpl
import de.unikl.dbis.clash.optimizer.materializationtree.parallelismFor
import de.unikl.dbis.clash.optimizer.materializationtree.storageCostFor
import de.unikl.dbis.clash.optimizer.minimalRequiredTasks
import de.unikl.dbis.clash.optimizer.noPartitioning
import de.unikl.dbis.clash.optimizer.tuplesMaterializedForRelation
import de.unikl.dbis.clash.query.Predicate
import de.unikl.dbis.clash.query.Query
import de.unikl.dbis.clash.query.Relation
import de.unikl.dbis.clash.query.isCrossProduct
import de.unikl.dbis.clash.support.ceil

/**
 * This strategy constructs a left deep plan, such that the number of materialized intermediate tuples is minimized
 */
open class LeftDeepGreedyTheta : TreeStrategy() {
    override fun optimizeTree(query: Query, dataCharacteristics: DataCharacteristics, params: OptimizationParameters): TreeOptimizationResult {
        val tree = leastMaterializationGeneralizedLeftDeep(query, dataCharacteristics, params, 5) // TODO why 5?
        val numTasks = globalNumTasks(tree.root)
        return TreeOptimizationResult(tree, CostEstimation(globalTuplesMaterialized(dataCharacteristics, tree.root), globalProbeTuplesSent(dataCharacteristics, tree.root), numTasks))
    }
}

/**
 * Constructs the left-deep tree according to the lecture's greedy-III algorithm:
 * For each choice of a starting relation, chose the cheapest plan starting from that one.
 *
 * Note, this algorithm works on the relations and uses the leftDeepFor method to create the actual tree.
 */
internal fun leastMaterializationGeneralizedLeftDeep(query: Query, dataCharacteristics: DataCharacteristics, optimizationParameters: OptimizationParameters, k: Int): MaterializationTree {
    var bestOrder: List<Relation>? = null
    var bestCost = Double.POSITIVE_INFINITY
    val relation = query.result

    for (startRelation in relation.baseRelations()) {
        val notYetJoinedRelations = relation.baseRelations().toMutableSet()
        notYetJoinedRelations.remove(startRelation)
        val joinedRelations = mutableListOf(startRelation)
        var cost = 0.0
        while (notYetJoinedRelations.isNotEmpty()) {
            val nextRelation = notYetJoinedRelations
                    .filter {
                        optimizationParameters.crossProductsAllowed || !isCrossProduct(relation.joinPredicates, joinedRelations, it)
                    }
                    .map {
                        Pair(it, joinSize(dataCharacteristics,
                                it.join(relation.joinPredicates, *joinedRelations.toTypedArray())))
                    }.minBy { it.second }!!

            joinedRelations += nextRelation.first
            notYetJoinedRelations.remove(nextRelation.first)
            cost += nextRelation.second
        }
        if (cost < bestCost) {
            bestOrder = joinedRelations
            bestCost = cost
        }
    }

    val plan = leftDeepFor2(query,
            bestOrder!!,
            dataCharacteristics,
            optimizationParameters,
            relation.joinPredicates)

    return MaterializationTree(plan)
}

/**
 * For a list [a, b, c, d, ...] constructs a left-deep tree recursively "top-down" as follows:
 *        ...
 *       x   d
 *      / \
 *     x  c
 *    / \
 *   a   b
 *
 */
internal fun leftDeepFor(orderedRelations: List<Relation>, dataCharacteristics: DataCharacteristics, optimizationParameters: OptimizationParameters, binaryPredicates: Collection<Predicate>, upperIsMaterialized: Boolean = false): MtNode {
    if (orderedRelations.size < 2) {
        error("Cannot produce left-deep tree for less than two relations!")
    }
    // Termination
    if (orderedRelations.size == 2) {
        val leftChild = matSource(orderedRelations[0], dataCharacteristics, optimizationParameters, noPartitioning())
        val rightChild = matSource(orderedRelations[1], dataCharacteristics, optimizationParameters, noPartitioning())
        return if (upperIsMaterialized) {
            binaryMat(leftChild, rightChild, dataCharacteristics, optimizationParameters, binaryPredicates)
        } else {
            binaryNonMat(leftChild, rightChild, dataCharacteristics, optimizationParameters, binaryPredicates)
        }
    }
    // Recursion
    val leftChild = leftDeepFor(orderedRelations.subList(0, orderedRelations.lastIndex), dataCharacteristics, optimizationParameters, binaryPredicates, true)
    val rightChild = matSource(orderedRelations.last(), dataCharacteristics, optimizationParameters, noPartitioning())
    return if (upperIsMaterialized) {
        binaryMat(leftChild, rightChild, dataCharacteristics, optimizationParameters, binaryPredicates)
    } else {
        binaryNonMat(leftChild, rightChild, dataCharacteristics, optimizationParameters, binaryPredicates)
    }
}

/**
 * For a list [a, b, c, d, ...] constructs a left-deep tree bottom-up as follows:
 *        ...
 *       x   d
 *      / \
 *     x  c
 *    / \
 *   a   b
 *
 * Where the last node is n-ary.
 */
internal fun leftDeepFor2(query: Query, orderedRelations: List<Relation>, dataCharacteristics: DataCharacteristics, optimizationParameters: OptimizationParameters, binaryPredicates: Collection<Predicate>, upperIsMaterialized: Boolean = false): MtNode {
    if (orderedRelations.size < 2) {
        error("Cannot produce left-deep tree for less than two relations!")
    }

    var currentTree: MtNode = matSource(orderedRelations[0], dataCharacteristics, optimizationParameters, noPartitioning())
    var currentMaterializationCost = orderedRelations.map { estimateSize(it, dataCharacteristics) }.sum()
    var currentRequiredTasks = minimalRequiredTasks(query, dataCharacteristics, optimizationParameters.taskCapacity)
    var i = 1

    // Build the left-deep part as long as there are enough resources
    while (i < orderedRelations.size - 1) {
        var relationToAdd = orderedRelations[i]

        val relationIfJoined = currentTree.relation.join(binaryPredicates, relationToAdd)
        val sizeOfNewStore = tuplesMaterializedForRelation(relationIfJoined, dataCharacteristics)
        val tasksOfNewStore = (sizeOfNewStore / optimizationParameters.taskCapacity.toDouble()).ceil()
        if (currentMaterializationCost + sizeOfNewStore > optimizationParameters.totalCapacity ||
                tasksOfNewStore + currentRequiredTasks > optimizationParameters.availableTasks) {
            // There is no space for materializing the additional result
            break
        }

        val newChild = matSource(relationToAdd, dataCharacteristics, optimizationParameters, noPartitioning())
        val newNode = binaryMat(currentTree, newChild, dataCharacteristics, optimizationParameters, binaryPredicates)
        currentTree = newNode
        currentMaterializationCost += sizeOfNewStore
        currentRequiredTasks += tasksOfNewStore

        i++
    }

    // Now the rest needs to become children of the root node
    val reminder = orderedRelations.subList(i, orderedRelations.lastIndex + 1).map { matSource(it, dataCharacteristics, optimizationParameters, noPartitioning()) }
    return binaryNonMatGeneralized(currentTree, reminder, dataCharacteristics, optimizationParameters, binaryPredicates)
}

internal fun binaryMat(left: MtNode, right: MtNode, dataCharacteristics: DataCharacteristics, optimizationParameters: OptimizationParameters, binaryPredicates: Collection<Predicate>): MatMultiStream {
    val joinedRelation = left.relation.join(binaryPredicates, right.relation)
    return MatMultiStream(
            joinedRelation,
            listOf(left, right),
            parallelismFor(joinedRelation, dataCharacteristics, optimizationParameters.taskCapacity),
            listOf(), // TODO
            storageCostFor(joinedRelation, dataCharacteristics),
            createMultiStreamImpl(listOf(left, right), joinedRelation.joinPredicates, optimizationParameters.probeOrderOptimizationStrategy, dataCharacteristics)
    )
}

internal fun binaryNonMat(left: MtNode, right: MtNode, dataCharacteristics: DataCharacteristics, optimizationParameters: OptimizationParameters, binaryPredicates: Collection<Predicate>): NonMatMultiStream {
    val joinedRelation = left.relation.join(binaryPredicates, right.relation)
    return NonMatMultiStream(
            joinedRelation,
            listOf(left, right),
            createMultiStreamImpl(listOf(left, right), joinedRelation.joinPredicates, optimizationParameters.probeOrderOptimizationStrategy, dataCharacteristics)
    )
}

internal fun binaryNonMatGeneralized(left: MtNode, right: Collection<MtNode>, dataCharacteristics: DataCharacteristics, optimizationParameters: OptimizationParameters, binaryPredicates: Collection<Predicate>): NonMatMultiStream {
    val joinedRelation = left.relation.join(binaryPredicates, *right.map { it.relation }.toTypedArray())
    return NonMatMultiStream(
            joinedRelation,
            listOf(left) + right,
            createMultiStreamImpl(listOf(left) + right, joinedRelation.joinPredicates, optimizationParameters.probeOrderOptimizationStrategy, dataCharacteristics)
    )
}

internal fun matSource(relation: Relation, dataCharacteristics: DataCharacteristics, optimizationParameters: OptimizationParameters, partitioning: PartitioningAttributesSelection): MatSource {
    val partitioningAttributes = partitioning[relation.inputAliases.toList()] ?: listOf()
    return MatSource(
            relation,
            parallelismFor(relation, dataCharacteristics, optimizationParameters.taskCapacity),
            partitioningAttributes,
            storageCostFor(relation, dataCharacteristics)
    )
}
