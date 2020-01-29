package de.unikl.dbis.clash.flexstorm

import java.io.Serializable
import java.util.TreeMap

class StoreContainer : Serializable {
    val inner = mutableMapOf<RelationValue, TreeMap<TimestampValue, Store>>()

    fun getStore(timestamp: TimestampValue, relation: RelationValue): Store {
        val candidates = inner[relation]!!
        return candidates[candidates.floorKey(timestamp)]!!
    }

    fun newEpoch(
        timestamp: TimestampValue,
        what: Map<String, List<String>>
    ) {
        for((relation, indeces) in what) {
            val map = TreeMap<TimestampValue, Store>()
            map[timestamp] = Store(indeces)
            inner[relation] = map
        }
    }

    fun totalTuples() = inner.values
        .map { it.values }
        .flatten()
        .map { it.tupleCounter }
        .sum()

}
