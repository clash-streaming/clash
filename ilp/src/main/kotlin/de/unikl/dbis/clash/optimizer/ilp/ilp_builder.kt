package de.unikl.dbis.clash.optimizer.ilp

import kotlin.math.roundToInt

class IlpBuilder {
    val rows = mutableListOf<IlpRow>()
    val probeOrderVariables = mutableMapOf<ProbeOrder, String>()
    val costCache = CostCache()

    /**
     * Add a row that chooses exactly one probe out of the given probe orders.
     * For example, for probe orders <R, S, T> and <R, T, S>,
     * this adds a new variable x1 for <R, S, T> and a variable x2 for <R, T, S>
     * and the following row:
     * x1 + x2 = 1
     */
    fun addProbeOrderChoice(probeOrders: Collection<ProbeOrder>) {
        val entries = mutableListOf<IlpEntry>()
        for (probeOrder in probeOrders) {
            val variable = newVariable()
            probeOrderVariables[probeOrder] = variable
            entries.add(IlpEntry(1, variable))
        }
        val row = IlpRow(entries, IlpCompare.EQUAL, 1, "Choose one of these Probe Orders")
        rows.add(row)
    }

    /**
     * For a given probe order, this adds the equation for the probe order's cost.
     * For example, if probe order <R, S, T>, which was given the variable x1 in `addProbeOrderChoice`
     * consists of cost for Sending to S = 4, sending the result to T = 7,
     * this adds
     * - 11 * x1 + 4 * z1 + 7 * z3 >= 0
     */
    fun addProbeOrderCost(
        probeOrder: ProbeOrder,
        query: IlpQuery,
        statistics: Statistics,
        configuration: Configuration
    ) {
        fun mustBroadcast(seen: List<String>, other: String): Boolean {
            return !seen.contains(other)
        }
        val seenVariables = query.getBindingsFor(probeOrder.order[0].name).toMutableList()
        val seenRelations = mutableListOf(probeOrder.order[0].name)
        var totalCost = 0.0
        val entries = mutableListOf<IlpEntry>()

        for (relation in probeOrder.order.subList(1, probeOrder.order.size)) {
            val tuplesSentFromPreviousRelation = statistics.joinSize(seenRelations)
            val routingDuplicationFactor = if (mustBroadcast(seenVariables, relation.partAttr)) configuration.getParallelism(relation.name) else 1
            entries += IlpEntry((tuplesSentFromPreviousRelation * routingDuplicationFactor).roundToInt(), newVariable())
            totalCost += tuplesSentFromPreviousRelation * routingDuplicationFactor
            seenRelations += relation.name
            seenVariables += query.getBindingsFor(relation.name)
        }

        entries += IlpEntry(totalCost.roundToInt(), probeOrderVariables[probeOrder]!!)
        val row = IlpRow(entries, IlpCompare.GREATER_THAN, 0, "Cost for this probe order")
        rows.add(row)
    }

    fun build(): Ilp {
        TODO()
//        return Ilp(rows)
    }
}

class CostCache {
    val probeOrderCost: MutableMap<ProbeOrder, Double> = mutableMapOf()

    fun add(probeOrder: ProbeOrder, cost: Double) {
        if (probeOrderCost.containsKey(probeOrder) && probeOrderCost[probeOrder] != cost) {
            throw RuntimeException("Probe Order $probeOrder was earlier recorded with cost ${probeOrderCost[probeOrder]} and now with $cost")
        }
        probeOrderCost[probeOrder] = cost
    }

    fun get(probeOrder: ProbeOrder): Double {
        return probeOrderCost.getOrElse(probeOrder) { throw RuntimeException("Tried to get non-existent value for probe order $probeOrder ") }
    }
}
