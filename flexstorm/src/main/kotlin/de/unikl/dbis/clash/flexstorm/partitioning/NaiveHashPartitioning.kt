package de.unikl.dbis.clash.flexstorm.partitioning

class NaiveHashPartitioning(
    val numPartitions: Int,
    override val attribute: String
) : Partitioning {

    override fun forValue(value: Any) = listOf(value.hashCode() % numPartitions)
    override fun all(): List<Int> = (0 until numPartitions).toList()
}
