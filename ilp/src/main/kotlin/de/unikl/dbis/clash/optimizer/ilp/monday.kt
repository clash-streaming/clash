package de.unikl.dbis.clash.optimizer.ilp

import kotlin.system.measureTimeMillis

data class Query3(val relations: List<QueriedRelation>) {
    /**
     *  Read a string like "R(a,b,c), S(c,d), T(e,f)" into a Query3
     */
    constructor(str: String) : this(parseRelations(str))

    fun getBindingsFor(relationName: String) = relations.first { it.name == relationName }.bindings
}

fun parseRelations(str: String): List<QueriedRelation> {
    var pos = 0
    var inParens = false
    var currentRelationName: String? = null
    var currentRelationBindings = mutableListOf<String>()
    val relations = mutableListOf<QueriedRelation>()
    while(pos < str.length) {
        when(str[pos]) {
            ' ', ',' -> { }
            '(' -> { inParens = true }
            ')' -> {
                inParens = false
                relations.add(QueriedRelation(currentRelationName!!, currentRelationBindings))
                currentRelationBindings = mutableListOf()
            }
            else -> {
                if(inParens) {
                    currentRelationBindings.add(str[pos].toString())
                } else {
                    currentRelationName = str[pos].toString()
                }
            }
        }
        pos++
    }
    return relations
}

sealed class ProbeOrder3 {
    class Start(val name: String) : ProbeOrder3() {
        override fun toString(): String {
            return this.name
        }
    }
    class Sequence(val from: ProbeOrder3, val target: PartitionedRelation): ProbeOrder3() {
        override fun toString(): String {
            return this.from.toString() + ", $target"
        }
    }

    fun toProbeOrderPrefixes(query: Query3,
                             statistics: Statistics,
                             configuration: Configuration): List<Pair<ProbeOrder3, Double>> {
        return when(this) {
            is Start -> { listOf(Pair(this, 0.0)) }
            is Sequence -> {
                val previousSteps = this.from.asListPair()
                val size = joinSize(previousSteps, query, statistics)
                val routingDuplicationFactor = if(mustBroadcast(previousSteps, this.target)) configuration.getParallelism(this.target.name) else 1
                return this.from.toProbeOrderPrefixes(query, statistics, configuration) + Pair(this, size*routingDuplicationFactor)
            }
        }
    }

    fun asListPair(): Pair<String, List<PartitionedRelation>> {
        return when(this) {
            is Start -> { Pair(this.name, listOf()) }
            is Sequence -> {
                val prev = this.from.asListPair()
                return Pair(prev.first, prev.second + this.target)
            }
        }
    }

    override fun equals(other: Any?): Boolean {
        return when(this) {
            is Start -> { other is Start && other.name == this.name }
            is Sequence -> {
                other is Sequence && other.target == this.target && other.from == this.from
            }
        }
    }

    override fun hashCode(): Int {
        return when(this) {
            is Start -> { name.hashCode() }
            is Sequence -> {
                this.target.hashCode().xor(this.from.hashCode())
            }
        }
    }
}

fun joinSize(listPair: Pair<String, List<PartitionedRelation>>,
             query: Query3,
             statistics: Statistics
): Double {
    val previousRelationNames = listPair.first + listPair.second.map { it.name }
    val relations = query.relations.filter { previousRelationNames.contains(it.name) }
    return statistics.joinSize(relations.map { it.name })
}

fun mustBroadcast(seen: Pair<String, List<PartitionedRelation>>, other: PartitionedRelation): Boolean {
    return !seen.second.map { it.partAttr }.contains(other.partAttr)
}


fun partitioningOptions(queries: Collection<Query3>): Map<String, Set<String>> {
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


fun isNoCrossProduct(query: Query3, probeOrder: ProbeOrder3, relation: String): Boolean {
    val attrsInRelation = query.getBindingsFor(relation)
    return when(probeOrder) {
        is ProbeOrder3.Start -> {
            val attrsInPo = query.getBindingsFor(probeOrder.name)
            (attrsInRelation - attrsInPo) != attrsInRelation
        }
        is ProbeOrder3.Sequence -> {
            val attrsInPo = query.getBindingsFor(probeOrder.target.name)
            (attrsInRelation - attrsInPo) != attrsInRelation || isNoCrossProduct(query, probeOrder.from, relation)
        }
    }
}

fun isCrossProduct(query: Query3, probeOrder: ProbeOrder3, relation: String) = !isNoCrossProduct(query, probeOrder, relation)

fun potentialProbeOrders(
        partitioningOptions: Map<String, Set<String>>,
        query: Query3,
        startingRelation: QueriedRelation): List<ProbeOrder3> {
    fun _recursive(start: List<ProbeOrder3>, reminder: Set<String>): List<ProbeOrder3> {
        if(reminder.isEmpty()) {
            return start
        }
        val result = mutableListOf<ProbeOrder3>()
        for(r in reminder) {
            val reminder_without_r = reminder.filter { it != r }.toSet()
            for(attr in partitioningOptions[r]!!) {
                for(old_start in start) {
                    if(isCrossProduct(query, old_start, r)) {
                        continue
                    }
                    result.addAll(_recursive(listOf(ProbeOrder3.Sequence(old_start, PartitionedRelation(r, attr))), reminder_without_r))
                }

            }
        }
        return result
    }

    val seed = ProbeOrder3.Start(startingRelation.name)
    val rest = query.relations
            .filter { it.name != startingRelation.name }
            .map { it.name }
            .toSet()
    return _recursive(
            listOf(seed),
            rest)
}

class IlpBuilder3() {
    var probeOrderVariableCounter = 0
    val probeOrderVariables = mutableMapOf<ProbeOrder3, String>()
    var probeOrderTaskVariableCounter = 0
    val probeOrderTaskVariables = mutableMapOf<ProbeOrder3, Pair<Double, String>>()
    val rows = mutableListOf<IlpRow>()
    val costFor = mutableMapOf<ProbeOrder3, Double>()
    val partialCost = mutableMapOf<ProbeOrder3, Double>()

    /**
     * Add a probe order cost row:
     *
     * - SUM_COST x_po  + cost1 x_po1 + cost2 + x_po2 + ...
     */
    fun addProbeOrderCost(probeOrder: ProbeOrder3, list: List<Pair<ProbeOrder3, Double>>) {
        val totalCost = list.map { it.second }.sum()
        costFor[probeOrder] = totalCost
        val head = IlpEntry(-totalCost, getProbeOrderVariable(probeOrder))
        val rest = mutableListOf(head)
        for (entry in list.subList(1, list.size)) {
            rest += IlpEntry(entry.second, getProbeOrderTaskVariable(entry))
            partialCost[entry.first] = entry.second
        }
        rows += IlpRow(rest, IlpCompare.GREATER_THAN, 0, "for probe order <$probeOrder>")
    }

    fun addProbeOrderChoice(probeOrders: List<ProbeOrder3>, query: Query3, relation: QueriedRelation) {
        val entries = mutableListOf<IlpEntry>()
        for(probeOrder in probeOrders) {
            entries += IlpEntry(1, getProbeOrderVariable(probeOrder))
        }
        rows += IlpRow(entries, IlpCompare.EQUAL, 1, "for query $query and starting relation $relation")
    }

    fun getProbeOrderVariable(probeOrder: ProbeOrder3): String {
        if(!probeOrderVariables.containsKey(probeOrder)){
            probeOrderVariables[probeOrder] = "x${probeOrderVariableCounter++}"
        }
        return probeOrderVariables[probeOrder]!!
    }

    fun getProbeOrderTaskVariable(probeOrderEntry: Pair<ProbeOrder3, Double>): String {
        if(!probeOrderTaskVariables.containsKey(probeOrderEntry.first)){
            probeOrderTaskVariables[probeOrderEntry.first] = Pair(probeOrderEntry.second, "y${probeOrderTaskVariableCounter++}")
        }
        return probeOrderTaskVariables[probeOrderEntry.first]!!.second
    }

    fun build(): Ilp {
        val goalEntries = mutableListOf<IlpEntry>()
        for(entry in probeOrderTaskVariables) {
            goalEntries += IlpEntry(entry.value.first, entry.value.second)
        }

        return Ilp(rows, goalEntries, probeOrderVariables.values + probeOrderTaskVariables.values.map { it.second })
    }
}

class Scenario(val statistics: Statistics, val configuration: Configuration, val debugMode: Boolean = false) {
    fun createIlp3(queries: Collection<Query3>): IlpProblem {
        val partitioningOptions = partitioningOptions(queries)
        val builder = IlpBuilder3()
        for(query in queries) {
            debug("\n# Starting with query $query")
            for(relation in query.relations) {
                debug("- If tuples from relation $relation arrive:")
                val probeOrders = mutableListOf<ProbeOrder3>()
                for(probeOrder in potentialProbeOrders(partitioningOptions, query, relation)) {
                    debug("    - examining probe order $probeOrder")
                    probeOrders += probeOrder
                    builder.addProbeOrderCost(probeOrder, probeOrder.toProbeOrderPrefixes(query, statistics, configuration))
                }
                builder.addProbeOrderChoice(probeOrders, query, relation)
            }
        }

        val ilp = builder.build()
        return IlpProblem(ilp, builder.probeOrderVariables.entries.associate{(k,v)-> v to k }, builder)
    }

    fun debug(str: String) {
        if(debugMode) {
            println(str)
        }
    }
}

class RandomQueryGenerator(val relations: Map<String, List<String>>) {
    fun generate(length: Int): Query3 {
        val startName = relations.keys.random()
        val queriedRelations = mutableListOf<QueriedRelation>()
        var current = QueriedRelation(startName, relations[startName]!!)
        queriedRelations += current
        var currentLength = 1
        while(currentLength < length) {
            val next = relations.entries.shuffled().firstOrNull { queriedRelations.none { prev -> prev.name == it.key } && (it.value - current.bindings) != it.value } ?: break
            val nextRelation = QueriedRelation(next.key, next.value)
            queriedRelations += nextRelation
            currentLength += 1
            current = nextRelation
        }
        // now we remove non-join attributes
        val simplifiedRelations = queriedRelations.map {
            val attrs = it.bindings.filter { inner -> queriedRelations.any { rel -> rel.name != it.name && rel.bindings.contains(inner) } }
            QueriedRelation(it.name, attrs)
        }
        return Query3(simplifiedRelations)
    }

    fun generateMany(numQueries: Int, length: Int, allowDuplicates: Boolean = true): Collection<Query3> {
        val result = if(allowDuplicates) mutableListOf<Query3>() else mutableSetOf<Query3>()
        repeat(numQueries) { result += generate(length) }
        return result
    }
}

data class IlpProblem(val ilp: Ilp, val probeOrderVariables: Map<String, ProbeOrder3>, val builder: IlpBuilder3)


fun main() {
//    val po1 = ProbeOrder3.Sequence(ProbeOrder3.Sequence(ProbeOrder3.Start("R"), PartitionedRelation("S", "a")), PartitionedRelation("T", "b"))
//    val po2 = ProbeOrder3.Sequence(ProbeOrder3.Sequence(ProbeOrder3.Start("R"), PartitionedRelation("S", "a")), PartitionedRelation("T", "b"))
//
//    println(po1==po2)
//
//    if(1==1)
//        return

    val queries = listOf(
            Query3("R(a), S(a,b), T(b)"),
            Query3("S(b), T(b,c), U(c)")
    )
    val statistics = ConstantStatistics(1000.0, 0.001)
    val configuration = ConstantConfiguration(5)
    val scenario = Scenario(statistics, configuration, debugMode = false)


    val rng10_3 = RandomQueryGenerator(mapOf(
            "Q" to listOf("a", "b", "c"),
            "R" to listOf("c", "d", "e"),
            "S" to listOf("e", "f", "g"),
            "T" to listOf("g", "h", "i"),
            "U" to listOf("g", "j", "k"),
            "V" to listOf("g", "l", "m"),
            "W" to listOf("a", "e", "g"),
            "X" to listOf("a", "m", "n"),
            "Y" to listOf("a", "n", "o"),
            "Z" to listOf("o", "p", "q")
    ))
    val rng100_3 = RandomQueryGenerator(mapOf(
            "Q1" to listOf("a1", "b1", "c1"),
            "R1" to listOf("c1", "d1", "e1"),
            "S1" to listOf("e1", "f1", "g1"),
            "T1" to listOf("g1", "h1", "i1"),
            "U1" to listOf("g1", "j1", "k1"),
            "V1" to listOf("g1", "l1", "m1"),
            "W1" to listOf("a1", "e1", "g1"),
            "X1" to listOf("a1", "m1", "n1"),
            "Y1" to listOf("a1", "n1", "o1"),
            "Z1" to listOf("o1", "p1", "q1"),
            "Q2" to listOf("o1", "b2", "c2"),
            "R2" to listOf("c2", "d2", "e2"),
            "S2" to listOf("e2", "f2", "g2"),
            "T2" to listOf("g2", "h2", "i2"),
            "U2" to listOf("g2", "j2", "k2"),
            "V2" to listOf("g2", "l2", "m2"),
            "W2" to listOf("a2", "n1", "g2"),
            "X2" to listOf("a2", "m2", "n2"),
            "Y2" to listOf("a2", "n2", "o2"),
            "Z2" to listOf("o2", "p2", "q2"),
            "Q3" to listOf("a3", "b3", "c3"),
            "R3" to listOf("c3", "d3", "e3"),
            "S3" to listOf("e3", "f3", "g3"),
            "T3" to listOf("g3", "h3", "i3"),
            "U3" to listOf("g3", "j3", "k3"),
            "V3" to listOf("a2", "l3", "m3"),
            "W3" to listOf("a3", "e3", "g3"),
            "X3" to listOf("a3", "m3", "j2"),
            "Y3" to listOf("a3", "n3", "o3"),
            "Z3" to listOf("o3", "p3", "q3"),
            "Q4" to listOf("a4", "b4", "c4"),
            "R4" to listOf("c4", "d4", "g3"),
            "S4" to listOf("e4", "f4", "g4"),
            "T4" to listOf("g4", "h4", "i4"),
            "U4" to listOf("g4", "j4", "k4"),
            "V4" to listOf("g4", "l4", "m4"),
            "W4" to listOf("a4", "e4", "g4"),
            "X4" to listOf("e3", "m4", "n4"),
            "Y4" to listOf("a4", "n4", "o4"),
            "Z4" to listOf("o4", "p4", "q4"),
            "Q5" to listOf("a5", "b5", "c5"),
            "R5" to listOf("c5", "d5", "e5"),
            "S5" to listOf("e5", "f5", "i4"),
            "T5" to listOf("g5", "h5", "i5"),
            "U5" to listOf("g5", "e4", "k5"),
            "V5" to listOf("g5", "l5", "m5"),
            "W5" to listOf("a5", "e5", "g5"),
            "X5" to listOf("a5", "m5", "n5"),
            "Y5" to listOf("a5", "n5", "o5"),
            "Z5" to listOf("o5", "p5", "q5"),
            "Q6" to listOf("a1", "b1", "c1"),
            "R6" to listOf("c1", "d1", "e1"),
            "S6" to listOf("a5", "f1", "g1"),
            "T6" to listOf("g1", "h1", "i1"),
            "U6" to listOf("g1", "j1", "k1"),
            "V6" to listOf("g1", "l1", "m1"),
            "W6" to listOf("a1", "e1", "c5"),
            "X6" to listOf("a1", "m1", "n1"),
            "Y6" to listOf("a1", "n1", "o1"),
            "Z6" to listOf("o1", "p1", "q1"),
            "Q7" to listOf("a2", "b2", "c2"),
            "R7" to listOf("c2", "d2", "e2"),
            "S7" to listOf("e2", "f2", "g2"),
            "T7" to listOf("g2", "h2", "i2"),
            "U7" to listOf("e2", "j2", "k2"),
            "V7" to listOf("g2", "l2", "m2"),
            "W7" to listOf("a2", "e2", "g2"),
            "X7" to listOf("a2", "m2", "e2"),
            "Y7" to listOf("a2", "n2", "o2"),
            "Z7" to listOf("o2", "p2", "q2"),
            "Q8" to listOf("a3", "b3", "c3"),
            "R8" to listOf("c3", "d3", "e3"),
            "S8" to listOf("e3", "f3", "g3"),
            "T8" to listOf("g3", "h3", "i3"),
            "U8" to listOf("g3", "j3", "k3"),
            "V8" to listOf("g3", "l3", "m3"),
            "W8" to listOf("a3", "e3", "g3"),
            "X8" to listOf("a3", "m3", "n3"),
            "Y8" to listOf("a3", "n3", "o3"),
            "Z8" to listOf("o3", "p3", "q3"),
            "Q9" to listOf("a4", "b4", "c4"),
            "R9" to listOf("c4", "d4", "e4"),
            "S9" to listOf("e4", "f4", "g4"),
            "T9" to listOf("e3", "h4", "i4"),
            "U9" to listOf("g4", "j4", "k4"),
            "V9" to listOf("g4", "f3", "g3"),
            "W9" to listOf("a4", "e4", "g4"),
            "X9" to listOf("a4", "m4", "n4"),
            "Y9" to listOf("a4", "n4", "o4"),
            "Z9" to listOf("o4", "p4", "q4"),
            "Q10" to listOf("a5", "b5", "c5"),
            "R10" to listOf("c5", "d5", "e5"),
            "S10" to listOf("c4", "f5", "g5"),
            "T10" to listOf("g5", "h5", "i5"),
            "U10" to listOf("g5", "j5", "k5"),
            "V10" to listOf("g5", "c1", "m5"),
            "W10" to listOf("a5", "e5", "g5"),
            "X10" to listOf("a5", "m5", "n5"),
            "Y10" to listOf("a5", "n5", "c2"),
            "Z10" to listOf("o5", "p5", "q5")
    ))
    val rng50_no_part = RandomQueryGenerator(mapOf(
            "Q1" to listOf("a"),
            "R1" to listOf("a"),
            "S1" to listOf("a"),
            "T1" to listOf("a"),
            "U1" to listOf("a"),
            "V1" to listOf("a"),
            "W1" to listOf("a"),
            "X1" to listOf("a"),
            "Y1" to listOf("a"),
            "Z1" to listOf("a"),
            "Q2" to listOf("a"),
            "R2" to listOf("a"),
            "S2" to listOf("a"),
            "T2" to listOf("a"),
            "U2" to listOf("a"),
            "V2" to listOf("a"),
            "W2" to listOf("a"),
            "X2" to listOf("a"),
            "Y2" to listOf("a"),
            "Z2" to listOf("a"),
            "Q3" to listOf("a"),
            "R3" to listOf("a"),
            "S3" to listOf("a"),
            "T3" to listOf("a"),
            "U3" to listOf("a"),
            "V3" to listOf("a"),
            "W3" to listOf("a"),
            "X3" to listOf("a"),
            "Y3" to listOf("a"),
            "Z3" to listOf("a")
    ))

    val costWithoutSharing = mutableListOf<Double>()
    val costWithSharing = mutableListOf<Double>()
    val times = mutableListOf<Long>()
    val usedVariables = mutableListOf<Int>()
    val examinedProbeOrders = mutableListOf<Int>()

    for(i in 1..100) {
        val ilp = scenario.createIlp3(rng50_no_part.generateMany(30, 6, false))
        val timeBefore = System.currentTimeMillis()
        val result = computeIlpSolution(ilp.ilp)
        times += (System.currentTimeMillis() - timeBefore)
        println("Using the following probe orders:")
        println(ilp.probeOrderVariables.filter { result.contains(it.key) }.map { it.value}.joinToString("\n"))
        println("Cost without sharing:")
        val currentCostWithoutSharing = ilp.probeOrderVariables.filter { result.contains(it.key) }.map { ilp.builder.costFor[it.value]!! }.sum()
        println(currentCostWithoutSharing)
        costWithoutSharing += currentCostWithoutSharing

        println("Cost with sharing:")
        val currentCostWithSharing = ilp.builder.probeOrderTaskVariables.filter { result.contains(it.value.second) }.map { ilp.builder.partialCost[it.key]!! }.sum()
        println(currentCostWithSharing)
        costWithSharing += currentCostWithSharing

        usedVariables += (ilp.builder.probeOrderTaskVariables.size + ilp.builder.probeOrderVariables.size)
        examinedProbeOrders += ilp.builder.probeOrderVariables.size

        println("================")
        println("===== $i =======")
        println("================")
    }

    println("===============")
    println("Average cost without sharing ${costWithoutSharing.average()}")
    println("Average cost with sharing ${costWithSharing.average()}")
    println("Average runtime ${times.average()}")
    println("Average variables used  ${usedVariables.average()}")
    println("Average probe orders ${examinedProbeOrders.average()}")

    Runtime.getRuntime().exec("say es ist fertig")
}