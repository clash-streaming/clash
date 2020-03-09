package de.unikl.dbis.clash.api

import com.beust.klaxon.Klaxon
import com.beust.klaxon.KlaxonException
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import de.unikl.dbis.clash.ClashConfig
import de.unikl.dbis.clash.api.jsonArgs.JsonArg
import de.unikl.dbis.clash.api.jsonArgs.JsonLocalCluster
import de.unikl.dbis.clash.api.jsonArgs.JsonRemoteCluster
import de.unikl.dbis.clash.datacharacteristics.AllCross
import de.unikl.dbis.clash.optimizer.GlobalStrategy
import de.unikl.dbis.clash.optimizer.GlobalStrategyRegistry
import de.unikl.dbis.clash.optimizer.OptimizationParameters
import de.unikl.dbis.clash.optimizer.OptimizationResult
import de.unikl.dbis.clash.optimizer.ProbeOrderStrategyRegistry
import de.unikl.dbis.clash.optimizer.materializationtree.OptimizationError
import de.unikl.dbis.clash.optimizer.probeorder.ProbeOrderOptimizationStrategy
import de.unikl.dbis.clash.physical.PhysicalGraph
import de.unikl.dbis.clash.physical.toJson
import de.unikl.dbis.clash.query.InputName
import de.unikl.dbis.clash.query.Query
import de.unikl.dbis.clash.query.parser.QueryParseException
import de.unikl.dbis.clash.query.parser.parseQuery
import de.unikl.dbis.clash.readConfig
import de.unikl.dbis.clash.storm.bolts.CommonSinkI
import de.unikl.dbis.clash.storm.builder.StormTopologyBuilder
import de.unikl.dbis.clash.storm.spouts.CommonSpoutI
import java.io.File
import org.apache.storm.LocalCluster
import org.apache.storm.StormSubmitter
import org.apache.storm.generated.StormTopology
import org.apache.storm.utils.Utils
import org.json.JSONArray
import org.json.JSONObject

const val TOPOLOGY_RUNTIME = 600_000L

/**
 * Accepts a single json document as input of following format:
 *
 * {
 *   "dataCharacteristics": { ... },
 *   "optimizationParameters": {
 *     "taskCapacity": 1_000_000,
 *     "availableTasks": 100,
 *     "globalStrategy": { "name": "Flat", ... },
 *     "probeOrderOptimizationStrategy": { "name": "LeastSent", ... },
 *   },
 *   "query": "SELECT ..."
 *
 * }
 */
class Json : CliktCommand() {
    val config by option("--config", "-c").convert { readConfig(it) }.default(ClashConfig()) // TODO
    val params by argument().convert {
        val rawJson = if (File(it).exists()) {
            File(it).readText()
        } else {
            it
        }

        val result: JsonArg? = try {
            Klaxon().parse<JsonArg>(rawJson)
        } catch (e: KlaxonException) {
            System.err.println("Cannot parse input.")
            e.printStackTrace()
            null
        }

        if (result == null) { fail("Input was empty.") } else {
            result
        }
    }

    override fun run() {
        disableLogging()

        // TODO replace with other default characteristics
        val dataCharacteristics = params.dataCharacteristics?.get() ?: AllCross()

        val query = parseQuery(params.query)
        var optimizationResult: OptimizationResult? = null

        if (params.optimizationParameters == null) {
            runQueryParse(params.query)
        } else {
            val optimizationParameters = params.optimizationParameters!!.get()
            val clash = Clash(config, dataCharacteristics, optimizationParameters, query)

            try {
                val output = JSONObject()
                optimizationResult = clash.optimize()

                val optimizationJson = JSONObject()
                optimizationJson.put("pCost", optimizationResult.costEstimation.probeCost)
                optimizationJson.put("sCost", optimizationResult.costEstimation.storageCost)
                optimizationJson.put("numTasks", optimizationResult.costEstimation.numTasks)
                optimizationJson.put("tree", optimizationResult.intermediateResult)
                output.put("optimizationResult", optimizationJson)

                output.put("physicalGraphResult", optimizationResult.physicalGraph.toJson())
                print(output.toString(2))
            } catch (e: OptimizationError) {
                val json = JSONObject()
                json.put("error", e.toString())
                json.put("error_details", e.localizedMessage)
                print(json.toString(2))
            }
        }

        if (params.cluster != null && optimizationResult != null) {
            if (params.cluster!! is JsonLocalCluster) {
                runLocalCluster(query, optimizationResult.physicalGraph, params.cluster!! as JsonLocalCluster)
            } else {
                runRemoteCluster(query, optimizationResult.physicalGraph, params.cluster!! as JsonRemoteCluster)
            }
        }
    }

    fun runQueryParse(query: String) {
        fun baseRelations(query: Query): JSONArray {
            return JSONArray(query.result.inputAliases.map { it.inner })
        }

        fun baseRelationAliases(query: Query): JSONArray {
            return JSONArray(query.result.inputAliases.map { it.inner })
        }

        fun binaryPredicates(query: Query): JSONArray {
            return JSONArray(query.result.joinPredicates.map { it.toString() })
        }

        fun unaryPredicates(query: Query): JSONArray {
            return JSONArray(query.result.filters.map { it.toString() })
        }

        fun success(parsedQuery: Query) {
            val json = JSONObject()
            json.put("query", query)
            json.put("baseRelations", baseRelations(parsedQuery))
            json.put("baseRelationAliases", baseRelationAliases(parsedQuery))
            json.put("binaryPredicates", binaryPredicates(parsedQuery))
            json.put("unaryPredicates", unaryPredicates(parsedQuery))

            println(json.toString(2))
        }

        fun failure(e: Exception) {
            val json = JSONObject()
            json.put("query", query)
            json.put("error", e.toString())
            json.put("error_details", e.localizedMessage)
            e.printStackTrace()

            println(json.toString(2))
        }

        try {
            val parsedQuery = parseQuery(query)
            success(parsedQuery)
        } catch (e: QueryParseException) {
            failure(e)
        }
    }

    fun runLocalCluster(query: Query, physicalGraph: PhysicalGraph, clusterArgs: JsonLocalCluster) {
        val stormTopology = buildTopology(
                query,
                config,
                physicalGraph,
                clusterArgs.sources.map { Pair(InputName(it.key), it.value.get(InputName(it.key))) }.toMap(),
                clusterArgs.sink.get()
        )

        val cluster = LocalCluster()
        cluster.submitTopology("test", config, stormTopology)
        Utils.sleep(TOPOLOGY_RUNTIME)
        cluster.killTopology("test")
        cluster.shutdown()
    }

    fun runRemoteCluster(query: Query, physicalGraph: PhysicalGraph, clusterArgs: JsonRemoteCluster) {
        val stormTopology = buildTopology(
                query,
                config,
                physicalGraph,
                clusterArgs.sources.map { Pair(InputName(it.key), it.value.get(InputName(it.key))) }.toMap(),
                clusterArgs.sink.get())
        System.setProperty("storm.jar", getClusterJarName())
//        config.put(Config.NIMBUS_SEEDS, listOf(""))
//        config.put(Config.NIMBUS_THRIFT_PORT, 6627)
//        config.put(Config.STORM_ZOOKEEPER_PORT, 2181)
//        config.put(Config.STORM_ZOOKEEPER_SERVERS, ZOOKEEPER_ID)
//        config.setNumWorkers(16)
//        config.setMaxSpoutPending(5000)
        StormSubmitter.submitTopology("test", config, stormTopology)
    }

    fun buildTopology(
        query: Query,
        config: ClashConfig,
        physicalGraph: PhysicalGraph,
        sources: Map<InputName, CommonSpoutI>,
        sink: CommonSinkI
    ): StormTopology {
        val builder = StormTopologyBuilder(
                physicalGraph,
                sources,
                query.inputMap,
                sink,
                config)
        return builder.build()
    }

    private fun getClusterJarName(): String {
        val fullName = File(Json::class
                .java
                .protectionDomain
                .codeSource
                .location
                .toURI()).absolutePath
        val name = fullName.subSequence(0, fullName.length - ".jar".length)
        return "$name-stormCluster.jar"
        // return "/Users/manuel/research/clash/clash/build/libs/clash-0.2.0-stormCluster.jar"
    }
}

@Deprecated("use json parser thing")
fun parseOptimizationParameters(raw: JSONObject): OptimizationParameters {
    fun parseGlobalStrategy(raw: JSONObject): GlobalStrategy {
        return GlobalStrategyRegistry.initialize(raw.getString("name"), raw.toMap())
    }
    fun parseProbeOrderOptimizationStrategy(raw: JSONObject): ProbeOrderOptimizationStrategy {
        return ProbeOrderStrategyRegistry.initialize(raw.getString("name"), raw.toMap())
    }

    return OptimizationParameters(
            raw.getLong("taskCapacity"),
            raw.getLong("availableTasks"),
            parseGlobalStrategy(raw.getJSONObject("globalStrategy")),
            parseProbeOrderOptimizationStrategy(raw.getJSONObject("probeOrderOptimizationStrategy"))
    )
}
