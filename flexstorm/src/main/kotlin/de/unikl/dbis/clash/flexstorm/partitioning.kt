package de.unikl.dbis.clash.flexstorm

import java.io.Serializable

interface Partitioning: Serializable {
    fun forValue(value: Any): List<Int>
    fun all(): List<Int>
    val attribute: String
}

class NaiveHashPartitioning(
    val numPartitions: Int,
    override val attribute: String
) : Partitioning {

    override fun forValue(value: Any) = listOf(value.hashCode() % numPartitions)
    override fun all(): List<Int> = (0 until numPartitions).toList()
}
