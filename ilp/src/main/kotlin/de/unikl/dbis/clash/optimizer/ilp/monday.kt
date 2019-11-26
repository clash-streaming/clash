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


fun partitioningOptions(queries: List<Query3>): Map<String, Set<String>> {
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
    fun createIlp3(queries: List<Query3>): IlpProblem {
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

    fun generateMany(numQueries: Int, length: Int): List<Query3> {
        val result = mutableListOf<Query3>()
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

    val costWithoutSharing = mutableListOf<Double>()
    val costWithSharing = mutableListOf<Double>()
    val times = mutableListOf<Long>()
    val usedVariables = mutableListOf<Int>()
    val examinedProbeOrders = mutableListOf<Int>()

    for(i in 1..100) {
        val ilp = scenario.createIlp3(rng10_3.generateMany(100, 3))
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
    }

    println("===============")
    println("Average cost without sharing ${costWithoutSharing.average()}")
    println("Average cost with sharing ${costWithSharing.average()}")
    println("Average runtime ${times.average()}")
    println("Average variables used  ${usedVariables.average()}")
    println("Average probe orders ${examinedProbeOrders.average()}")

}