package de.unikl.dbis.clash.flexstorm

import java.io.Serializable

class ClashState: Serializable {
    var epochs = Epochs()

    /**
     * For the given timestamp, find the taskId where
     * the given relation could be.
     */
    fun getInstanceIds(timestamp: TimestampValue, key: String, value: Any, relation: RelationValue): List<Int> {
        val epoch = this.epochs.epochFor(timestamp)!!
        val partitioning = epoch.partitioning[relation]!!
        if(partitioning.attribute == key) {
          return partitioning.forValue(value)
        } else {
            return partitioning.all()
        }
    }

    /**
     * For the given timestamp, find the targets where tuples should be sent to
     * in the dispatching face.
     */
    fun getProbeOrderTarget(
        timestamp: TimestampValue,
        payload: PayloadValue,
        relation: RelationValue,
        probeOrderOffset: Int
    ): List<Int> {
        val epoch = epochs.epochFor(timestamp)!!
        val probeOrder = epoch.probeOrders[relation]!!
        val entry = probeOrder.entries[probeOrderOffset]
        val groupingKey = payload[entry.sendingAttribute]!!
        val targetStore = entry.targetStore
        return getInstanceIds(timestamp, entry.probingAttribute, groupingKey, targetStore)
    }

    fun getStoreTarget(
        timestamp: TimestampValue,
        payload: PayloadValue,
        relation: RelationValue
    ): Int {
        val epoch = epochs.epochFor(timestamp)!!
        val partitioning = epoch.partitioning[relation]!!
        val groupingKey = payload[partitioning.attribute]!!
        return partitioning.forValue(groupingKey).first()
    }
}
