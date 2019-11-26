package de.unikl.dbis.clash.optimizer.ilp

import de.unikl.dbis.clash.support.times

open class Statistics(
    val rates: MutableMap<String, Double>
) {
    private var selectivity: MutableMap<Pair<String, String>, Double> = mutableMapOf()

    open fun getSelectivity(s1: String, s2: String): Double {
        val (ss1, ss2) = listOf(s1, s2).sorted()
        return selectivity[Pair(ss1, ss2)]?: 1.0
    }

    open fun setSelectivity(s1: String, s2: String, sel: Double) {
        val (ss1, ss2) = listOf(s1, s2).sorted()
        selectivity[Pair(ss1, ss2)] = sel
    }

    open fun getRate(str: String): Double {
        return rates[str]!!
    }

    fun joinSize(relations: List<String>): Double {
        return relations.asSequence()
                        .map { getRate(it) }.product() *
                relations.times(relations)
                        .filter { (s1, s2) ->  s1 < s2}
                        .map { (s1, s2) -> getSelectivity(s1, s2) }
                        .product()
    }
}

class ConstantStatistics(val rate: Double, val selectivity: Double) : Statistics(mutableMapOf()) {
    override fun getSelectivity(s1: String, s2: String): Double {
        return selectivity
    }

    override fun getRate(str: String): Double {
        return rate
    }

}

fun Sequence<Double>.product(): Double {
    var result = 1.0
    for(x in this) {
        result *= x
    }
    return result
}

/**
 * Say we have a probe order R[a], S[b], T[c]. Then the communication cost
 * for this probe order is comprised of:
 * - sending R tuples to S, and
 * - sending R \Join S tuples to T.
 * For computing this, the rate of R, S and their selectivity is needed,
 * as well as the partitioning of S and T and information if that
 * can be exploited.
 * For example, if the query is R(a, b), then we can send R tuples directly to S.
 * If, the query would be R(a, c), we must broadcast to all S-instances.
 */
fun communicationCostFor(
        probeOrder: ProbeOrder,
        query: IlpQuery,
        statistics: Statistics,
        configuration: Configuration
): Double {
    fun mustBroadcast(seen: List<String>, other: String): Boolean {
        return !seen.contains(other)
    }
    val seenVariables = query.getBindingsFor(probeOrder.order[0].name).toMutableList()
    val seenRelations = mutableListOf(probeOrder.order[0].name)
    var cost = 0.0

    for (relation in probeOrder.order.subList(1, probeOrder.order.size)) {
        val tuplesSentFromPreviousRelation = statistics.joinSize(seenRelations)
        val routingDuplicationFactor = if(mustBroadcast(seenVariables, relation.partAttr)) configuration.getParallelism(relation.name) else 1
        println("Cost for sending to $relation is $tuplesSentFromPreviousRelation * $routingDuplicationFactor")
        cost += tuplesSentFromPreviousRelation * routingDuplicationFactor
        seenRelations += relation.name
        seenVariables += query.getBindingsFor(relation.name)
    }

    return cost
}