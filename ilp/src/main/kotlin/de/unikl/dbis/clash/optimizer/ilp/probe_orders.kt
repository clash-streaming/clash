package de.unikl.dbis.clash.optimizer.ilp

data class ProbeOrder(val start: QueriedRelation, val order: List<PartitionedRelation>) {
    override fun toString(): String {
        val reminder = order.joinToString(", ")
        return "[${start.name}, $reminder]"
    }
}

fun allProbeOrders(query: IlpQuery): Collection<ProbeOrder> {
    return query.relations.map { probeOrdersFor(listOf(it), query) }.flatten()
}

fun probeOrdersFor(head: List<QueriedRelation>, query: IlpQuery): Collection<ProbeOrder> {
    return when(head.size) {
        query.relations.size -> { explode(head).map { ProbeOrder(it.first, it.second) } }
        else -> {
            val nextElements = query.relations.filter { !head.contains(it) && notCross(head, it)}
            val x = nextElements.map { probeOrdersFor(head + it, query) }
            x.flatten()
        }
    }
}

/**
 * Given a list of relations from a query, e.g.
 *   R(a, b), S(c, d), T(e)
 * This computes all the possible partitioning schemes:
 *   R(a), S(c), T(e)
 *   R(a), S(d), T(e)
 *   R(b), S(c), T(e)
 *   R(b), S(d), T(e)
 */
fun explode(boundRelations: List<QueriedRelation>): List<Pair<QueriedRelation, List<PartitionedRelation>>> {
    fun _explode_rec(prefix: List<Pair<QueriedRelation, List<PartitionedRelation>>>, reminder: List<QueriedRelation>): List<Pair<QueriedRelation, List<PartitionedRelation>>> {
        if(reminder.isEmpty()) {
            return prefix
        } else {
            val current = reminder.first()
            val newPrefix = prefix.flatMap { p -> current.bindings.map { b -> Pair(p.first, p.second + PartitionedRelation(current.name, b)) } }
            return _explode_rec(newPrefix, reminder.subList(1, reminder.size))
        }
    }
    val current = boundRelations.first()
    val seed = listOf(Pair<QueriedRelation, List<PartitionedRelation>>(current, listOf()))
//        val seed = current.bindings.map { listOf(PartitionedRelation(current.name, it)) }
    val rest = boundRelations.subList(1, boundRelations.size)
    return _explode_rec(seed, rest)
}

fun notCross(head: List<QueriedRelation>, relation: QueriedRelation) = head.any { (it.bindings - relation.bindings) != it.bindings }
