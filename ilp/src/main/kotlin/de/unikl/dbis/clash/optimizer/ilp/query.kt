package de.unikl.dbis.clash.optimizer.ilp

data class QueriedRelation(val name: String, val bindings: List<String>) {
    override fun toString(): String {
        return "$name(${bindings.joinToString(",")})"
    }
}

data class PartitionedRelation(val name: String, val partAttr: String) {
    override fun toString(): String {
        return "$name($partAttr)"
    }
}

data class IlpQuery(val relations: List<QueriedRelation>, val statistics: Statistics, val configuration: Configuration) {
    /**
     * All variables occurring in the query
     */
    val bindings get() = relations.map { it.bindings }.flatten().distinct()

    fun getBindingsFor(relationName: String) = relations.first { it.name == relationName }.bindings

    fun toIlp(builder: IlpBuilder): Ilp {
        // exactly one probe order for each starting relation
        for (relation in relations) {
            val probeOrders = probeOrdersFor(listOf(relation), this)
            builder.addProbeOrderChoice(probeOrders)

            for (probeOrder in probeOrders) {
                builder.addProbeOrderCost(probeOrder, this, statistics, configuration)
            }
        }
        return builder.build()
    }
}

fun main() {
    val statistics = Statistics(mutableMapOf(
            "R" to 1000.0,
            "S" to 1000.0,
            "T" to 1234.0
    ))
    statistics.setSelectivity("R", "S", 0.01)
    statistics.setSelectivity("S", "T", 0.01)
    statistics.setSelectivity("R", "T", 0.01)

    val config = Configuration(mapOf(
            "R" to 6,
            "S" to 2,
            "T" to 3
    ))

    val r1 = QueriedRelation("R", listOf("b"))
    val s1 = QueriedRelation("S", listOf("b", "c"))
    val t1 = QueriedRelation("T", listOf("c"))
    val u1 = QueriedRelation("U", listOf("x", "y"))
    val q1 = IlpQuery(listOf(r1, s1, t1), statistics, config)

    println("All possible probe orders for query q1 = $q1")
    println(allProbeOrders(q1).sortedBy { it.toString() }.joinToString("\n"))

    println("\nNext:\n")

    println("Probe order cost:")
    val somePo = allProbeOrders(q1).toList()[1] // .random()

    val builder = IlpBuilder()
    val ilp = q1.toIlp(builder)
    println(ilp)

    val sb = StringBuilder()
    ilp.toCplex(sb)
    println(sb.toString())

    //
//    val s2 = QueriedRelation("S", listOf("c"))
//    val t2 = QueriedRelation("T", listOf("c", "d"))
//    val u2 = QueriedRelation("U", listOf("d"))
//    val q2 = Query(listOf(s2, t2, u2))
//
//    println(q1.allProbeOrders().joinToString("\n"))
}
