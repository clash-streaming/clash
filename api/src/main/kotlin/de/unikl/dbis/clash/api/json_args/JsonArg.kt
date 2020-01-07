package de.unikl.dbis.clash.api.json_args

/**
 * Parent object that is fed into CLASH.
 *
 * If it only consists of a query, that query is analyzed and the result of the analysis returned.
 * If it includes optimization parameters, the according optimization is executed.
 * If data characteristics are provided, these are used during optimization, otherwise cost based optimization is skipped.
 * If a cluster is given, a topology is produced and sent to the cluster.
 */
data class JsonArg(
    val query: String,
    val optimizationParameters: JsonOptimizationParameters? = null,
    val dataCharacteristics: JsonDataCharacteristicsArg? = null,
    val cluster: JsonClusterArg? = null
)
