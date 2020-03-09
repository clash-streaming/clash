package de.unikl.dbis.clash.flexstorm.sampling

import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import de.unikl.dbis.clash.flexstorm.LabelledProbeOrder
import de.unikl.dbis.clash.flexstorm.ProbeOrder
import de.unikl.dbis.clash.flexstorm.ProbeOrderCollection
import de.unikl.dbis.clash.flexstorm.ProbeOrderEntry

class LiveEstimator(val probeOrders: ProbeOrderCollection) {
    val localStores = mutableMapOf<String, LocalStore>()
    init {
        probeOrders.relations().forEach {
            val indexAttributes = probeOrders.allProbingAttributesFor(it)
            localStores[it] = LocalStore(indexAttributes.associateWith { LocalIndex() })
        }
    }

    fun addTuple(relation: String, tuple: JsonObject) {
        val store = localStores[relation]!!
        store.store(tuple)
    }
}

class LocalStore(val indexes: Map<String, LocalIndex>) {

    fun store(tuple: JsonObject) {
        indexes.forEach {
            val key = tuple.get(it.key)
            it.value.store(key, tuple)
        }
    }
}

class LocalIndex {
    val inner = mutableMapOf<JsonElement, MutableList<JsonObject>>()

    fun store(key: JsonElement, value: JsonObject) {
        inner.putIfAbsent(key, mutableListOf())
        inner[key]!!.add(value)
    }
}

fun main() {
    val parser = JsonParser()
    val relR = listOf(
        """{"a": 5, "b": 18}""",
        """{"a": 6, "b": 18}""",
        """{"a": 7, "b": 17}""",
        """{"a": 8, "b": 17}""",
        """{"a": 9, "b": 16}""",
        """{"a": 10, "b": 16}"""
    ).map {
        parser.parse(it).asJsonObject
    }
    val relS = listOf(
        """{"c": 5, "d": 18}""",
        """{"c": 6, "d": 18}""",
        """{"c": 7, "d": 17}""",
        """{"c": 8, "d": 17}""",
        """{"c": 9, "d": 16}""",
        """{"c": 10, "d": 16}"""
    ).map {
        parser.parse(it).asJsonObject
    }
    val relT = listOf(
        """{"e": 5, "f": 18}""",
        """{"e": 6, "f": 18}""",
        """{"e": 7, "f": 17}""",
        """{"e": 8, "f": 17}""",
        """{"e": 9, "f": 16}""",
        """{"e": 10, "f": 16}"""
    ).map {
        parser.parse(it).asJsonObject
    }

    val r1ProbeOrder = LabelledProbeOrder(1, ProbeOrder(listOf(
        ProbeOrderEntry("b", "S", "c"),
        ProbeOrderEntry("d", "T", "e")
    ))
    )
    val r2ProbeOrder = LabelledProbeOrder(2, ProbeOrder(listOf(
        ProbeOrderEntry("d", "T", "e"),
        ProbeOrderEntry("b", "S", "c")
    ))
    )
    val s1ProbeOrder = LabelledProbeOrder(3, ProbeOrder(listOf(
        ProbeOrderEntry("d", "T", "e"),
        ProbeOrderEntry("c", "R", "b")
    ))
    )
    val s2ProbeOrder = LabelledProbeOrder(4, ProbeOrder(listOf(
        ProbeOrderEntry("c", "R", "b"),
        ProbeOrderEntry("d", "T", "e")
    ))
    )
    val t1ProbeOrder = LabelledProbeOrder(5, ProbeOrder(listOf(
        ProbeOrderEntry("e", "S", "d"),
        ProbeOrderEntry("c", "R", "b")
    ))
    )
    val t2ProbeOrder = LabelledProbeOrder(6, ProbeOrder(listOf(
        ProbeOrderEntry("c", "R", "a"),
        ProbeOrderEntry("e", "S", "d")
    ))
    )

    val pc = ProbeOrderCollection(
        mapOf("R" to listOf(r1ProbeOrder, r2ProbeOrder),
            "S" to listOf(s1ProbeOrder, s2ProbeOrder),
            "T" to listOf(t1ProbeOrder, t2ProbeOrder))
    )

    val estimator = LiveEstimator(pc)
    relR.map { Pair("R", it) }
        .union(relS.map { Pair("S", it) })
        .union(relT.map { Pair("T", it) })
        .shuffled().forEach { estimator.addTuple(it.first, it.second) }
}
