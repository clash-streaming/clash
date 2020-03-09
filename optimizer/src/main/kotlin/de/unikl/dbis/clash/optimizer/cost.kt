package de.unikl.dbis.clash.optimizer

import de.unikl.dbis.clash.datacharacteristics.DataCharacteristics
import de.unikl.dbis.clash.optimizer.materializationtree.MtNode
import de.unikl.dbis.clash.optimizer.materializationtree.MultiStream
import de.unikl.dbis.clash.optimizer.materializationtree.storageCostFor
import de.unikl.dbis.clash.optimizer.probeorder.ProbeOrder
import de.unikl.dbis.clash.query.AttributeAccess
import de.unikl.dbis.clash.query.BinaryPredicate
import de.unikl.dbis.clash.query.RelationAlias

fun globalTuplesMaterialized(dataCharacteristics: DataCharacteristics, node: MtNode): Double {
    val nodeSize = if (node.parallelism == 0L) 0.0 else storageCostFor(node.relation, dataCharacteristics)
    return nodeSize +
            node.children.sumByDouble { globalTuplesMaterialized(dataCharacteristics, it) }
}

fun globalProbeTuplesSent(dataCharacteristics: DataCharacteristics, node: MtNode): Double {
    if (node is MultiStream) {
        return node.probeOrders.inner.values.sumByDouble { globalProbeTuplesSentForProbeOrder(dataCharacteristics, it.first) } +
                node.children.sumByDouble { globalProbeTuplesSent(dataCharacteristics, it) }
    }
    return 0.0
}

fun globalProbeTuplesSentForProbeOrder(dataCharacteristics: DataCharacteristics, probeOrder: ProbeOrder): Double {
    return probeTuplesSentForProbeOrder(dataCharacteristics, probeOrder).values.sum()
}

fun globalIntermediateTuplesGeneratedForProbeOrder(dataCharacteristics: DataCharacteristics, probeOrder: ProbeOrder): Double {
    TODO()
//    return intermediateTuplesGeneratedForProbeOrder(dataCharacteristics, probeOrder).values.sum()
}

fun globalNumTasks(node: MtNode): Long {
    return node.parallelism + node.children.map { globalNumTasks(it) }.sum()
}

/**
 * Computes the probe tuples that are sent for a certain probe order.
 * However, it ignores the output of the last stage in order to avoid double counting
 * in complex trees.
 *
 * For example, the result for probe order <R,S,T> is
 *
 * {
 *   R => |R|
 *   S => |R \Join S|
 * }
 *
 */
fun probeTuplesSentForProbeOrder(dataCharacteristics: DataCharacteristics, probeOrder: ProbeOrder): Map<MtNode, Double> {
    // TODO search the bug
    val result = mutableMapOf<MtNode, Double>()
    val probeOrderWithJoinSize = relationSizeForProbeOrder(dataCharacteristics, probeOrder)

    val seenAttributeAccesses = mutableListOf<AttributeAccess>()
    for ((index, step) in probeOrderWithJoinSize
            .steps
            .subList(0, probeOrderWithJoinSize.steps.size - 1)
            .withIndex()) {
        // seenAttributeAccesses.addAll(step.first.relation.attributeAccesses) TODO i commented this out. what's going on here?

        val targetsPartitioning = probeOrderWithJoinSize.steps[index + 1].first.partitioning
        if (seenAttributeAccesses.any { targetsPartitioning.map { it.attribute }.contains(it.attribute) }) {
            result[step.first] = step.third
        } else {
            result[step.first] = step.third * probeOrderWithJoinSize.steps[index + 1].first.parallelism
        }
    }
    return result
}

/**
 * For a join node computes for all direct children the number of probe tuples they receive.
 */
fun probeTuplesReceivedByStore(dataCharacteristics: DataCharacteristics, node: MtNode): Map<MtNode, Double> {
    TODO()
//    fun <K, V> Map<K, List<V>>.combine(other: Map<K, V>): Map<K, List<V>> {
//        val result = this.toMutableMap()
//        other.forEach { result[it.key] = (result.getOrDefault(it.key, listOf())).plus(it.value) }
//        return result
//    }
//    val collector = mapOf<MTNodeOld, List<Double>>()
//    return node.probeOrders.values.map { probeTuplesReceivedForProbeOrder(dataCharacteristics, it) }
//            .fold(collector) { acc, other -> acc.combine(other)}
//            .mapValues { it.value.sum() }
}

/**
 *
 * This estimates the size of intermediate results created during probing.
 *
 * A result for join order <R, S, T> is:
 *   {
 *     R => |R|
 *     S => |R \Join S| * 1/2
 *     T => |R \Join S \Join T| * 1/3
 *   }
 *
 */
data class ProbeOrderWithJoinSize(val steps: List<Triple<MtNode, Set<BinaryPredicate>, Double>>)
fun relationSizeForProbeOrder(dataCharacteristics: DataCharacteristics, probeOrder: ProbeOrder): ProbeOrderWithJoinSize {
    val result = mutableListOf<Triple<MtNode, Set<BinaryPredicate>, Double>>()
    val relations = mutableSetOf<RelationAlias>()
    for ((index, step) in probeOrder
            .steps
            .withIndex()) {
        relations.addAll(step.first.relation.inputAliases)

        val currentNode = step.first
        val currentPredicates = step.second

        val joinSize = joinSize(dataCharacteristics, relations)
        val orderingFactor = 1.0 / (index + 1)
        result.add(Triple(currentNode, currentPredicates, joinSize * orderingFactor))
    }
    return ProbeOrderWithJoinSize(result)
}
