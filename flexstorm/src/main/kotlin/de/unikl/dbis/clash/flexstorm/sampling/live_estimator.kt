package de.unikl.dbis.clash.flexstorm.sampling

import com.google.gson.JsonElement
import com.google.gson.JsonObject
import de.unikl.dbis.clash.flexstorm.ProbeOrderCollection

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

}
