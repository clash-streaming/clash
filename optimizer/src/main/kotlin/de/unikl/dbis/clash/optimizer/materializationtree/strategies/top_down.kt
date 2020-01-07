package de.unikl.dbis.clash.optimizer.materializationtree.strategies

import de.unikl.dbis.clash.datacharacteristics.DataCharacteristics
import de.unikl.dbis.clash.estimator.estimateSize
import de.unikl.dbis.clash.optimizer.CostEstimation
import de.unikl.dbis.clash.optimizer.OptimizationParameters
import de.unikl.dbis.clash.optimizer.PartitioningAttributesSelection
import de.unikl.dbis.clash.optimizer.globalNumTasks
import de.unikl.dbis.clash.optimizer.globalProbeTuplesSent
import de.unikl.dbis.clash.optimizer.globalTuplesMaterialized
import de.unikl.dbis.clash.optimizer.materializationtree.MatMultiStream
import de.unikl.dbis.clash.optimizer.materializationtree.MatSource
import de.unikl.dbis.clash.optimizer.materializationtree.MaterializationTree
import de.unikl.dbis.clash.optimizer.materializationtree.MtNode
import de.unikl.dbis.clash.optimizer.materializationtree.MultiStream
import de.unikl.dbis.clash.optimizer.materializationtree.NonMatMultiStream
import de.unikl.dbis.clash.optimizer.materializationtree.TreeOptimizationResult
import de.unikl.dbis.clash.optimizer.materializationtree.TreeStrategy
import de.unikl.dbis.clash.optimizer.materializationtree.createMultiStreamImpl
import de.unikl.dbis.clash.optimizer.materializationtree.parallelismFor
import de.unikl.dbis.clash.optimizer.materializationtree.storageCostFor
import de.unikl.dbis.clash.optimizer.noPartitioning
import de.unikl.dbis.clash.query.AttributeAccess
import de.unikl.dbis.clash.query.BinaryEquality
import de.unikl.dbis.clash.query.Query
import de.unikl.dbis.clash.query.RelationAlias
import kotlin.math.ceil

/**
 * The idea of this algorithm is, to start with a flat tree and iteratively combine children of the root
 * until no capacity is left.
 *
 * For example:
 *       root
 *     / /| \ \
 *    R S T U V
 *
 * Either, (R, S), (S, T),
 *
 * In order to support large join graphs, here, we
 * shrink the search space by limiting the exploration to only linear join graphs (also called chains).
 * If multiple linear join graphs exists, e.g., in a clique, a random one is selected.
 * This means, the optimization strategy internally considers only joins of relations
 * $R_1, ..., R_n$ with join predicates $\theta_{i, i+1}$.
 * The strategy operates in a top-down fashion and starts with a flat query plan consisting of a single MSJ.
 * Then, it proceeds with iteratively merging children into new MSJ operators,
 * until the system capacity is reached.
 * The pseudo code in Figure~\ref{alg:linearqueries} describes this procedure.
 * First, a representation of the flat tree is initialized in Line~1 as list of lists and
 * the initial budget is set as  for the previous approach to generate left-deep trees.
 * As long as the budget is not exceeded, the algorithm searches for relations to merge, as follows:
 * For all indexes $i$ and $j$ over the flattened list $T$, the ones which reside
 * in the same sublist are considered valid.
 * Then the pair $(i,j)$ with minimal cost for materializing the join of
 * relations $R_i, \dots R_j$ is selected in Line~4. If the cost for materializing
 * this would exceed the budget, the algorithm terminates---otherwise, it introduces a materialization
 * for these relations and continues.
 * Finally, the nested list of lists is converted into a tree and returned in Line~8.
 * The runtime complexity of this algorithm is in $\mathcal{O}(n^4)$.
 */
class TopDownTheta : TreeStrategy() {
    override fun optimizeTree(query: Query, dataCharacteristics: DataCharacteristics, params: OptimizationParameters): TreeOptimizationResult {
        val children = query.inputMap.keys.toList()
        val n = children.size
        val initialChildren = mutableListOf<TDTree>()
        for (i in 0 until n) {
            initialChildren += TDTree(i, i, listOf(children[i]), emptyList())
        }
        var currentT = TDTree(0, n - 1, children, initialChildren)

        val maxCapacity = (params.taskCapacity * params.availableTasks).toDouble()
        var currentCapacity = children.map { dataCharacteristics.getRate(it) }.sum()
        var usedTasks = 5 // TODO
//        var usedTasks = children.map { parallelismFor(it, dataCharacteristics, params.taskCapacity) }.sum()
        while (currentCapacity < maxCapacity && usedTasks < params.availableTasks) {
            val (i, j, scost) = findMinScost(currentT, dataCharacteristics, n) ?: break
            currentCapacity += scost
            usedTasks += ceil(scost / params.taskCapacity).toInt()
            if (currentCapacity > maxCapacity || usedTasks > params.availableTasks) {
                break
            }
            currentT = materialize(currentT, i, j)
        }

        val matTree = buildRoot(currentT, query, dataCharacteristics, params)

        val sCost = globalTuplesMaterialized(dataCharacteristics, matTree)
        val pCost = globalProbeTuplesSent(dataCharacteristics, matTree)
        val numTasks = globalNumTasks(matTree)
        val costEstimation = CostEstimation(sCost, pCost, numTasks)
        return TreeOptimizationResult(MaterializationTree(matTree), costEstimation)
    }

    private fun findMinScost(currentT: TDTree, dataCharacteristics: DataCharacteristics, n: Int): Triple<Int, Int, Double>? {
        var foundSomething = false
        var bestI = 0
        var bestJ = 0
        var bestSCost: Double = Double.MAX_VALUE

        for (i in 0 until n - 1) {
            for (j in i until n) {
                if (!valid(currentT, i, j)) {
                    continue
                }
                foundSomething = true
                val ijCost = costFor(i, j, currentT.relationAliases, dataCharacteristics)
                if (ijCost < bestSCost) {
                    bestSCost = ijCost
                    bestI = i
                    bestJ = j
                }
            }
        }

        if (foundSomething) {
            return Triple(bestI, bestJ, bestSCost)
        } else {
            return null
        }
    }

    private fun materialize(tree: TDTree, i: Int, j: Int): TDTree {
        for ((index, child)in tree.children.withIndex()) {
            // indeces are completely in a child => delegate to that child
            if (i >= child.leftIndex && j <= child.rightIndex) {
                val newChildren = tree.children.subList(0, index) + materialize(child, i, j) + tree.children.subList(index + 1, tree.children.size)
                return TDTree(tree.leftIndex, tree.rightIndex, tree.relationAliases, newChildren)
            }
        }
        val indexOfLeftChild = tree.children.indexOfFirst { it.leftIndex == i }
        val indexOfRightChild = tree.children.indexOfFirst { it.rightIndex == j }

        val materializedChildAliases = tree.relationAliases.subList(i - tree.leftIndex, j - tree.leftIndex + 1)
        val materializedChildChildren = tree.children.subList(indexOfLeftChild, indexOfRightChild + 1)
        val materializedChild = TDTree(i, j, materializedChildAliases, materializedChildChildren)
        val newChildren = tree.children.subList(0, indexOfLeftChild) + materializedChild + tree.children.subList(indexOfRightChild + 1, tree.children.size)
        return TDTree(tree.leftIndex, tree.rightIndex, tree.relationAliases, newChildren)
    }

    private fun costFor(i: Int, j: Int, relationAliases: List<RelationAlias>, dataCharacteristics: DataCharacteristics): Double {
        return estimateSize(relationAliases.subList(i, j + 1), dataCharacteristics)
    }

    private fun valid(tree: TDTree, i: Int, j: Int): Boolean {
        if (i >= j) {
            // Not allowed anyways
            return false
        }
        if (tree.leftIndex == i && tree.rightIndex == j) {
            // This would not change anything
            return false
        }
        for (child in tree.children) {
            // indeces are completely in a child => delegate to that child
            if (i >= child.leftIndex && j <= child.rightIndex) {
                return valid(child, i, j)
            }
        }
        if (tree.children.any { it.leftIndex == i } && tree.children.any { it.rightIndex == j }) {
            return true
        }
        return false
    }

    private fun buildRoot(tree: TDTree, query: Query, dataCharacteristics: DataCharacteristics, optimizationParameters: OptimizationParameters): NonMatMultiStream {
        val children = tree.children.map { buildTree(it, query, dataCharacteristics, optimizationParameters, noPartitioning()) }
        return NonMatMultiStream(
                query.result,
                children,
                createMultiStreamImpl(children, query.result.binaryPredicates, optimizationParameters.probeOrderOptimizationStrategy, dataCharacteristics)
        )
    }

    private fun buildTree(tree: TDTree, query: Query, dataCharacteristics: DataCharacteristics, optimizationParameters: OptimizationParameters, partitioning: PartitioningAttributesSelection): MtNode {
        if (tree.leftIndex == tree.rightIndex) {
            val relation = query.result.subRelation(tree.relationAliases.first())
            val partitioningAttributes = partitioning[relation.aliases.toList()] ?: listOf()
            return MatSource(relation, parallelismFor(relation, dataCharacteristics, optimizationParameters.taskCapacity), partitioningAttributes, storageCostFor(relation, dataCharacteristics))
        } else {
            val children = tree.children.map { buildTree(it, query, dataCharacteristics, optimizationParameters, noPartitioning()) }
            val childRelation = query.result.subRelation(*tree.relationAliases.toTypedArray())
            return MatMultiStream(
                    childRelation,
                    children,
                    parallelismFor(childRelation, dataCharacteristics, optimizationParameters.taskCapacity),
                    listOf(),
                    storageCostFor(childRelation, dataCharacteristics),
                    createMultiStreamImpl(children, query.result.binaryPredicates, optimizationParameters.probeOrderOptimizationStrategy, dataCharacteristics)
            )
        }
    }
}

data class TDTree(val leftIndex: Int, val rightIndex: Int, val relationAliases: List<RelationAlias>, val children: List<TDTree>) {
    override fun toString(): String {
        if (leftIndex == rightIndex) {
            return relationAliases.first().inner
        }
        return children.joinToString(",", "(", ")")
    }
}

fun fixInnerPartitioning(matTree: MtNode) {
    if (matTree is MultiStream) {
        matTree.children.forEach { fixInnerPartitioning(it) }
        // do we need to fix something?
        // only if a child has no partitioning
        // and the child appears in an equi predicate
        val candidates = mutableMapOf<MtNode, MutableList<AttributeAccess>>()
        for (child in matTree.children) {
            if (child.partitioning.isNotEmpty() || child is MatSource)
                continue
            for (predicate in matTree.relation.binaryPredicates) {
                if (predicate is BinaryEquality) {
                    if (child.relation.aliases.contains(predicate.leftAttributeAccess.relationAlias) &&
                            !child.relation.aliases.contains(predicate.rightAttributeAccess.relationAlias)) {
                        candidates.getOrPut(child, { mutableListOf() })
                        candidates[child]!!.add(predicate.leftAttributeAccess)
                    }
                    if (child.relation.aliases.contains(predicate.rightAttributeAccess.relationAlias) &&
                            !child.relation.aliases.contains(predicate.leftAttributeAccess.relationAlias)) {
                        candidates.getOrPut(child, { mutableListOf() })
                        candidates[child]!!.add(predicate.rightAttributeAccess)
                    }
                }
            }
        }

        for (candidate in candidates) {
            candidate.key.partitioning = candidate.value
        }
    }
}
