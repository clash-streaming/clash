package de.unikl.dbis.clash.optimizer.materializationtree.strategies

import de.unikl.dbis.clash.datacharacteristics.DataCharacteristics
import de.unikl.dbis.clash.optimizer.OptimizationParameters
import de.unikl.dbis.clash.optimizer.materializationtree.TreeOptimizationResult
import de.unikl.dbis.clash.optimizer.materializationtree.TreeStrategy
import de.unikl.dbis.clash.query.Query


/**
 * This strategy investigates potential subtrees in a bottom-up manner
 * and prunes the ones with highest cost during processing.
 * The algorithm, shown in Figure~\ref{alg:bottomup}, maintains a DP-style table which maps
 * so-called levels to collections of trees.
 * The level indicates the number of relations that are joined by the trees, e.g.,
 * in level 4, there are trees joining 4 relations.
 * In Line~1, the table is initialized with all flat trees for the according level.
 * Then, for each succeeding level, first the entries in that level are pruned in Line~3,
 * such that only the contained trees with the best cost are retained.
 * The remaining entries are combined with the trees of the previous levels and stored in the table.
 * The pruning of levels can be done either in relative (e.g., retain the top 10\%)
 * or absolute (e.g., retain the 15 best trees) terms---and be based on materialization or probe cost.
 * If each level is only allowed to contain the top-$k$ entries,
 * costly combinations can be already pruned at the combineLevels,
 * reducing space needed for the table. Note, that this algorithm
 * is always able to produce a working query plan, as level $n$ is
 * initialized with a flat tree, which might be replaced with a better plan
 * that still is able to compute the desired result.
 * The algorithm has storage and runtime of at least
 * exponential ($2^n$) complexity, in the number of relations.
 */
class BottomUpTheta : TreeStrategy() {
    override fun optimizeTree(query: Query, dataCharacteristics: DataCharacteristics, optimizationParameters: OptimizationParameters): TreeOptimizationResult {
//        val tree = createFlatTree(query.result, dataCharacteristics, optimizationParameters)
        TODO()
//        val sCost = globalTuplesMaterialized(dataCharacteristics, tree.root)
//        val pCost = globalProbeTuplesSent(dataCharacteristics, tree.root)
//        val costEstimation = CostEstimation(sCost, pCost)
//        return TreeOptimizationResult(tree, costEstimation)
    }
}