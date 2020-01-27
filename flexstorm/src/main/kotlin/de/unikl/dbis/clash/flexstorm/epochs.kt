package de.unikl.dbis.clash.flexstorm

import de.unikl.dbis.clash.flexstorm.partitioning.Partitioning
import java.io.Serializable
import java.util.TreeMap

class Epochs : Serializable {
    /**
     * if k and k' are adjacent keys in this map,
     * the epoch stored under k is valid from k til k'-1
     * e.g.
     * { 42 -> epoch1, 78 -> epoch2, ....}
     * epoch1 goes from 42 to 77.
     */
    private val inner = TreeMap<Long, Epoch>()

    fun addEpoch(timestamp: Long, epoch: Epoch) {
        inner[timestamp] = epoch
    }

    fun epochFor(timestamp: Long): Epoch? {
        return inner[inner.floorKey(timestamp)]
    }
}

data class Epoch(
    val probeOrders: Map<String, ProbeOrder>,
    val partitioning: Map<String, Partitioning>,
    val accessRules: Map<String, List<AccessRule>>
) : Serializable

data class AccessRule(val attribute: String) : Serializable
