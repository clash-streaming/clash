package de.unikl.dbis.clash.optimizer.probeorder

import de.unikl.dbis.clash.datacharacteristics.DataCharacteristics
import de.unikl.dbis.clash.optimizer.globalIntermediateTuplesGeneratedForProbeOrder
import de.unikl.dbis.clash.optimizer.globalProbeTuplesSentForProbeOrder
import de.unikl.dbis.clash.optimizer.materializationtree.MtNode
import de.unikl.dbis.clash.query.BinaryPredicate
import de.unikl.dbis.clash.query.RelationAlias
import de.unikl.dbis.clash.support.permutations

typealias ProbeCost = Double

/**
 * A probe order is defined for a single relation.
 * For example, the probe order <R, S, T> means, tuples from R are first routed to S and then to T.
 *
 * The ProbeOrder class also specifies which predicates are to be evaluated on arrival.
 * For a chain join, the above probe order would contain:
 *
 * listOf(Pair(rNode, setOf()),
 *        Pair(sNode, setOf(rsPred)),
 *        Pair(tNode, stPred))
 */
data class ProbeOrder(val steps: List<Pair<MtNode, Set<BinaryPredicate>>>)

/**
 * The ProbeOrder wrapper contains all probe orders for children of a MultiStream operator.
 */
data class ProbeOrders(val inner: Map<MtNode, Pair<ProbeOrder, Double>>)

/**
 * ALGORITHM Exhaustive-PO
 *
 * Finds the best Probe order according to the cost model.
 */
fun exhaustiveProbeOrderScan(
    dataCharacteristics: DataCharacteristics,
    predicates: Collection<BinaryPredicate>,
    joinInputs: Collection<MtNode>,
    costFunction: (DataCharacteristics, ProbeOrder) -> (Double)
): ProbeOrders {
    val result = mutableMapOf<MtNode, Pair<ProbeOrder, Double>>()
    joinInputs.toList().permutations().forEach {
        val probeOrder = listToProbeOrder(it, predicates)
        val cost = costFunction(dataCharacteristics, probeOrder)
        val startNode = it.first()
        if (result.containsKey(startNode)) {
            if (cost < result[startNode]!!.second) {
                result[startNode] = Pair(probeOrder, cost)
            }
        } else {
            result[startNode] = Pair(probeOrder, cost)
        }
    }
    return ProbeOrders(result)
}

fun exhaustiveLeastIntermediates(
    dataCharacteristics: DataCharacteristics,
    predicates: Collection<BinaryPredicate>,
    children: Collection<MtNode>
): ProbeOrders {
    return exhaustiveProbeOrderScan(dataCharacteristics, predicates, children, ::globalIntermediateTuplesGeneratedForProbeOrder)
}

fun exhaustiveLeastSent(
    dataCharacteristics: DataCharacteristics,
    predicates: Collection<BinaryPredicate>,
    children: Collection<MtNode>
): ProbeOrders {
    return exhaustiveProbeOrderScan(dataCharacteristics, predicates, children, ::globalProbeTuplesSentForProbeOrder)
}

/**
 * Helper function that generates a probe order from a list of nodes.
 * For example, if the contains Nodes
 */
fun listToProbeOrder(list: List<MtNode>, predicates: Collection<BinaryPredicate>): ProbeOrder {
    val result = mutableListOf<Pair<MtNode, Set<BinaryPredicate>>>()
    for (node in list) {
        result.add(Pair(node, predicatesForJoin(predicates, result.map { it.first.relation.inputAliases }.flatten(), node.relation.inputAliases)))
    }
    return ProbeOrder(result)
}

/**
 * Finds these predicates that are eligible for a join between two sets of relationAlias identifiers.
 * For example, if we have join predicates ["x.a = y.a", "y.b = z.b", "x.b <= z.c"] and want to know
 * which predicates can be applied when from = ["x", "z"], and to = ["y"],
 * this method returns "x.a = y.a" and "y.b = z.b".
 */
fun predicatesForJoin(predicates: Collection<BinaryPredicate>, from: Collection<RelationAlias>, to: Collection<RelationAlias>): Set<BinaryPredicate> {
    return predicates.filter { from.contains(it.leftRelationAlias) && to.contains(it.rightRelationAlias) ||
            from.contains(it.rightRelationAlias) && to.contains(it.leftRelationAlias) }.toSet()
}
