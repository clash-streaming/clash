package de.unikl.dbis.clash.api

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import de.unikl.dbis.clash.ClashConfig
import de.unikl.dbis.clash.datacharacteristics.SymmetricJSONCharacteristics
import de.unikl.dbis.clash.physical.PhysicalGraph
import de.unikl.dbis.clash.query.Query
import de.unikl.dbis.clash.query.parser.parseQuery
import de.unikl.dbis.clash.readConfig
import de.unikl.dbis.clash.storm.builder.StormTopologyBuilder
import org.apache.storm.LocalCluster
import org.apache.storm.StormSubmitter
import org.apache.storm.generated.StormTopology
import org.apache.storm.utils.Utils
import org.json.JSONObject
import java.io.File


/**
 * Accepts a  json document as input of following format:
 *
 * {
 *   "dataCharacteristics": { ... },
 *   "optimizationParameters": {
 *     "taskCapacity": 1_000_000,
 *     "availableTasks": 100,
 *     "globalStrategy": { "name": "Flat", ... },
 *     "probeOrderOptimizationStrategy": { "name": "LeastSent", ... },
 *     "partitioningAttributesSelectionStrategy": { "name": "Explicit", ... }
 *   },
 *   "query": "SELECT ..."
 *
 * }
 */
class JsonStorm : CliktCommand() {
    val localCluster by option("--local", "-l").flag(default = false)
    val config by option("--config", "-c").convert { readConfig(it) }.default( ClashConfig() ) // TODO

    val params by argument().convert { JSONObject(it) }
    val outsideInterface by argument().convert { readOutsideInterface(JSONObject(it), config) }

    override fun run() {
        val config = ClashConfig() // TODO
        val dataCharacteristics = SymmetricJSONCharacteristics(params.getJSONObject("dataCharacteristics"))
        val optimizationParameters = parseOptimizationParameters(params.getJSONObject("optimizationParameters"))
        val query = parseQuery(params.getString("query"))
        val clash = Clash(config, dataCharacteristics, optimizationParameters, query)

        val output = JSONObject()

        var bestOptimizationResult = clash.optimize()

        var list = partitionChoices(query)

        if (localCluster) {
            runLocalCluster(query, bestOptimizationResult.physicalGraph)
        } else {
            runRemoteCluster(query, bestOptimizationResult.physicalGraph)
        }

//        val optimizationJson = JSONObject()
//        optimizationJson.put("pCost", bestOptimizationResult.costEstimation.probeCost)
//        optimizationJson.put("sCost", bestOptimizationResult.costEstimation.storageCost)
//        optimizationJson.put("numTasks", bestOptimizationResult.costEstimation.numTasks)
//        optimizationJson.put("tree", bestOptimizationResult.intermediateResult)
//        output.put("optimizationResult", optimizationJson)
//
//        output.put("physicalGraphResult", bestOptimizationResult.physicalGraph.toJson())
//        print(output.toString(2))


    }


    fun runLocalCluster(query: Query, physicalGraph: PhysicalGraph) {
        val stormTopology = buildTopology(query, config, physicalGraph)

        val cluster = LocalCluster()
        cluster.submitTopology("test", config, stormTopology)
        Utils.sleep(600_000)
        cluster.killTopology("test")
        cluster.shutdown()
    }

    fun runRemoteCluster(query: Query, physicalGraph: PhysicalGraph) {
        val stormTopology = buildTopology(query, config, physicalGraph)
        System.setProperty("storm.jar", getClusterJarName())
//        config.put(Config.NIMBUS_SEEDS, listOf(""))
//        config.put(Config.NIMBUS_THRIFT_PORT, 6627)
//        config.put(Config.STORM_ZOOKEEPER_PORT, 2181)
//        config.put(Config.STORM_ZOOKEEPER_SERVERS, ZOOKEEPER_ID)
//        config.setNumWorkers(16)
//        config.setMaxSpoutPending(5000)
        StormSubmitter.submitTopology("test", config, stormTopology)

    }


    fun buildTopology(query: Query, config: ClashConfig, physicalGraph: PhysicalGraph): StormTopology {
        val sources = outsideInterface.first
        val sink = outsideInterface.second
        val builder = StormTopologyBuilder(
                physicalGraph,
                sources,
                query.inputMap,
                sink,
                config)
        return builder.build()
    }

    private fun getClusterJarName(): String {
        val fullName = File(RunStorm::class
                .java
                .protectionDomain
                .codeSource
                .location
                .toURI()).absolutePath
        val name = fullName.subSequence(0, fullName.length - 4) // strip the ".jar"
//        return "$name-stormCluster.jar"
        return "/Users/manuel/research/clash/clash/build/libs/clash-0.2.0-stormCluster.jar"
    }
}
