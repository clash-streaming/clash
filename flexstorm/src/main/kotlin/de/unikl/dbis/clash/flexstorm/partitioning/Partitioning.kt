package de.unikl.dbis.clash.flexstorm.partitioning

import java.io.Serializable

interface Partitioning : Serializable {
    fun forValue(value: Any): List<Int>
    fun all(): List<Int>
    val attribute: String
}
