package de.unikl.dbis.clash.optimizer.ilp

data class ProbeOrderEntry(
        val fromRelation: List<QueriedRelation>,
        val toPartitionedStore: PartitionedRelation,
        val broadcast: Boolean,
        val sendCost: Number
)

data class ProbeOrder2(
        val startingRelation: String,
        val entries: List<ProbeOrderEntry>,
        val totalCost: Number)




//
//fun explode2(probeOrder: ProbeOrder, partitioningOptions: Map<String, Set<String>>): List<ProbeOrder> {
//    fun _explode_rec(prefix: List<Pair<QueriedRelation, List<PartitionedRelation>>>, reminder: List<QueriedRelation>): List<Pair<QueriedRelation, List<PartitionedRelation>>> {
//        if(reminder.isEmpty()) {
//            return prefix
//        } else {
//            val current = reminder.first()
//            val newPrefix = prefix.flatMap { p -> current.bindings.map { b -> Pair(p.first, p.second + PartitionedRelation(current.name, b)) } }
//            return _explode_rec(newPrefix, reminder.subList(1, reminder.size))
//        }
//    }
//    val current = boundRelations.first()
//    val seed = listOf(Pair<QueriedRelation, List<PartitionedRelation>>(current, listOf()))
////        val seed = current.bindings.map { listOf(PartitionedRelation(current.name, it)) }
//    val rest = boundRelations.subList(1, boundRelations.size)
//    return _explode_rec(seed, rest)
//}



fun partitioningOptions(queries: Set<IlpQuery>): Map<String, Set<String>> {
    val result = mutableMapOf<String, MutableSet<String>>()
    for(q in queries) {
        for(qr in q.relations)  {
            val attrs = result.getOrDefault(qr.name, mutableSetOf())
            attrs.addAll(qr.bindings)
            result[qr.name] = attrs
        }
    }
    return result
}

//fun potentialProbeOrders(
//        partitioningOptions: Map<String, Set<String>>,
//        query: IlpQuery,
//        startingRelation: QueriedRelation): List<ProbeOrder2> {
//    fun _recursive(start: List<ProbeOrder2>, reminder: Set<String>): List<ProbeOrder2> {
//        if(reminder.isEmpty()) {
//            return start
//        }
//        val result = mutableListOf<ProbeOrder2>()
//        for(r in reminder) {
//            val reminder_without_r = reminder.filter { it != r }.toSet()
//            for(attr in partitioningOptions[r]!!) {
//                for(old_start in start) {
//                    val new_relation = IlpQuery()
//                    val new_start = ProbeOrder2(
//                            old_start.startingRelation,
//                            old_start.entries + ProbeOrderEntry(new_relation, PartitionedRelation(r, attr)))
//                }
//
//            }
//        }
//        return result
//    }
//
//    val seed = ProbeOrder2(startingRelation.name, listOf(), 0)
//    return _recursive(
//            listOf(seed),
//            query.relations
//                    .filter { it != startingRelation }
//                    .map { it.name }
//                    .toSet())
//}


//fun construct_ilp(queries: Set<IlpQuery>) {
//    val partitioningOptions = partitioningOptions(queries)
//    val builder = IlpBuilder2()
//    for(query in queries) {
//        for(relation in query.relations) {
//            val probeOrders = mutableListOf<ProbeOrder2>()
//            for(probeOrder in potentialProbeOrders(partitioningOptions, query, relation)) {
//                probeOrders += probeOrder
//                builder.addProbeOrderCost(probeOrder)
//            }
//            builder.addProbeOrderChoice(probeOrders)
//        }
//    }
//}

class IlpBuilder2() {
    var probeOrderVariableCounter = 0
    val probeOrderVariables = mutableMapOf<ProbeOrder2, String>()
    var probeOrderTaskVariableCounter = 0
    val probeOrderTaskVariables = mutableMapOf<ProbeOrderEntry, String>()
    val rows = mutableListOf<IlpRow>()

    fun addProbeOrderCost(probeOrder: ProbeOrder2) {
        val head = IlpEntry(probeOrder.totalCost, getProbeOrderVariable(probeOrder))
        val rest = mutableListOf<IlpEntry>(head)
        for (entry in probeOrder.entries) {
            rest += IlpEntry(entry.sendCost, getProbeOrderTaskVariable(entry))
        }
        rows += IlpRow(rest, IlpCompare.GREATER_THAN, 0)
    }

    fun addProbeOrderChoice(probeOrders: List<ProbeOrder2>) {
        val entries = mutableListOf<IlpEntry>()
        for(probeOrder in probeOrders) {
            entries += IlpEntry(1, getProbeOrderVariable(probeOrder))
        }
        rows += IlpRow(entries, IlpCompare.EQUAL, 1)
    }

    fun getProbeOrderVariable(probeOrder: ProbeOrder2): String {
        if(!probeOrderVariables.containsKey(probeOrder)){
            probeOrderVariables[probeOrder] = "x${probeOrderVariableCounter++}"
        }
        return probeOrderVariables[probeOrder]!!
    }

    fun getProbeOrderTaskVariable(probeOrderEntry: ProbeOrderEntry): String {
        if(!probeOrderTaskVariables.containsKey(probeOrderEntry)){
            probeOrderTaskVariables[probeOrderEntry] = "y${probeOrderTaskVariableCounter++}"
        }
        return probeOrderTaskVariables[probeOrderEntry]!!
    }
}


fun main() {
//    val q1 = IlpQuery(listOf(
//            QueriedRelation("R", listOf("a", "b")),
//            QueriedRelation("S", listOf("b", "c"))
//    ), Statistics(mutableMapOf()), Configuration(mapOf()))
//    val q2 = IlpQuery(listOf(
//            QueriedRelation("S", listOf("x")),
//            QueriedRelation("T", listOf("y", "z"))
//    ), Statistics(mutableMapOf()), Configuration(mapOf()))
//    println(construct_ilp(setOf(q1, q2)))
//
//    val x = ProbeOrder3.Start("R")
//    val y = ProbeOrder3.Sequence(PartitionedRelation("S", "a"), x)
//    val z = ProbeOrder3.Sequence(PartitionedRelation("T", "b"), y)

//    println(z)
}