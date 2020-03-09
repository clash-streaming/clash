package de.unikl.dbis.clash.flexstorm

import java.io.Serializable

class Store(indexes: List<String>) : Serializable {
    var tupleCounter = 0L
    val indexes: Map<String, MutableMap<Any, MutableList<PayloadValue>>>

    init {
        this.indexes = indexes.associateWith { mutableMapOf<Any, MutableList<PayloadValue>>() }
    }

    fun store(payload: PayloadValue) {
        for ((attribute, index) in indexes) {
            val value = payload[attribute] ?: throw AttributeNotFound(attribute)
            index.getOrPut(value, { mutableListOf() }).add(payload)
        }
        tupleCounter += 1
    }

    fun probe(key: String, value: Any): List<PayloadValue> {
        return indexes[key]?.get(value) ?: listOf()
    }
}

class AttributeNotFound(attribute: String) : Exception("Attribute $attribute not present in object.")
