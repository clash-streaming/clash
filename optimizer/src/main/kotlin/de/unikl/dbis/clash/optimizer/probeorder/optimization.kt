package de.unikl.dbis.clash.optimizer.probeorder

import de.unikl.dbis.clash.datacharacteristics.DataCharacteristics
import de.unikl.dbis.clash.optimizer.globalProbeTuplesSentForProbeOrder
import de.unikl.dbis.clash.optimizer.joinSize
import de.unikl.dbis.clash.optimizer.materializationtree.MtNode
import de.unikl.dbis.clash.query.BinaryPredicate
import de.unikl.dbis.clash.query.operations.joinRelations

interface ProbeOrderOptimizationStrategy {
    fun optimize(
        dataCharacteristics: DataCharacteristics,
        predicates: Collection<BinaryPredicate>,
        children: Collection<MtNode>
    ): Pair<ProbeOrders, ProbeCost>
}

/**
 * ALGORITHM Greedy-PO-1
 *
 * This algorithm iteratively chooses the next step in a probe order
 * such that the generated output result is minimal.
 *
 * This means, it does not care how many receivers there are.
 */
class LeastIntermediatesProbeOrder : ProbeOrderOptimizationStrategy {
    override fun optimize(dataCharacteristics: DataCharacteristics, predicates: Collection<BinaryPredicate>, children: Collection<MtNode>):
            Pair<ProbeOrders, ProbeCost> {
        val probeOrders = ProbeOrders(children.associate { Pair(it, leastIntermediatesProbeOrderFor(dataCharacteristics, predicates, children, it)) })
        return Pair(probeOrders, probeOrders.inner.values.map { it.second }.sum())
    }

    fun leastIntermediatesProbeOrderFor(dataCharacteristics: DataCharacteristics, predicates: Collection<BinaryPredicate>, children: Collection<MtNode>, startingNode: MtNode): Pair<ProbeOrder, Double> {
        val schedulableNodes = children.toMutableSet()
        schedulableNodes.remove(startingNode)

        val result = mutableListOf(Pair(startingNode, setOf<BinaryPredicate>()))

        var cost = 0.0
        var currentRelation = startingNode.relation

        while (schedulableNodes.isNotEmpty()) {
            // find the element which generates the smallest join size with the chosen prefix
            val (nextNode, joinSize, foundRelation) = schedulableNodes.map {
                val candidateRelation = joinRelations(currentRelation, it.relation, predicates)
                val joinSize = joinSize(dataCharacteristics, candidateRelation)
                Triple(it, joinSize, candidateRelation)
            }.minBy { (_, size) -> size }!!
            cost += joinSize
            currentRelation = foundRelation
            schedulableNodes.remove(nextNode)
            result.add(Pair(nextNode, predicatesForJoin(predicates, result.map { it.first.relation.aliases }.flatten(), nextNode.relation.aliases)))
        }
        val probeOrder = ProbeOrder(result.toList())
        val newCost = globalProbeTuplesSentForProbeOrder(dataCharacteristics, probeOrder)
        return Pair(probeOrder, newCost)
    }
}

/**
 * ALGORITHM Greedy-PO-2
 *
 * This algorithm takes into account the next sent tuples for each pair of choices.
 * At step P It selects two relations R' and R'' and minimizes $(P \Join R') \cdot N(R'')$.
 * Note that the choice of R'' is not fixed at this point.
 */
class LeastSentProbeOrder : ProbeOrderOptimizationStrategy {
    override fun optimize(dataCharacteristics: DataCharacteristics, predicates: Collection<BinaryPredicate>, children: Collection<MtNode>):
            Pair<ProbeOrders, ProbeCost> {
        val probeOrders = ProbeOrders(children.associate { Pair(it, leastSentProbeOrderFor(dataCharacteristics, predicates, children, it)) })
        return Pair(probeOrders, probeOrders.inner.values.map { it.second }.sum())
    }

    fun leastSentProbeOrderFor(dataCharacteristics: DataCharacteristics, predicates: Collection<BinaryPredicate>, children: Collection<MtNode>, startingNode: MtNode): Pair<ProbeOrder, Double> {
        val todo = children.toMutableSet()
        todo.remove(startingNode)

        val result = mutableListOf(Pair(startingNode, setOf<BinaryPredicate>()))
        var cost = 0.0
        while (todo.isNotEmpty()) {
            // find the element which generates the smallest join size with the chosen prefix
            val (nextRelation, cur) = todo.map { node ->
                // TODO does this what I think it does?
                val relationAliases = result.map { it.first.relation.aliases }.flatten().union(node.relation.aliases)
                val x = joinSize(dataCharacteristics, relationAliases)
                Pair(node, x)
            }.minBy { (rel, size) -> size }!!
            cost += cur
            todo.remove(nextRelation)
            result.add(Pair(nextRelation, predicatesForJoin(predicates, result.map { it.first.relation.aliases }.flatten(), nextRelation.relation.aliases)))
        }
        val probeOrder = ProbeOrder(result.toList())
        val newCost = globalProbeTuplesSentForProbeOrder(dataCharacteristics, probeOrder)
        return Pair(probeOrder, newCost)
    }
}
