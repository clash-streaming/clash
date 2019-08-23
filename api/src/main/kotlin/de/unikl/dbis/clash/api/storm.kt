package de.unikl.dbis.clash.api

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.choice
import com.github.ajalt.clikt.parameters.types.int
import com.github.ajalt.clikt.parameters.types.long
import de.unikl.dbis.clash.ClashConfig
import de.unikl.dbis.clash.datacharacteristics.SymmetricJSONCharacteristics
import de.unikl.dbis.clash.optimizer.GlobalStrategyRegistry
import de.unikl.dbis.clash.optimizer.OptimizationParameters
import de.unikl.dbis.clash.optimizer.ProbeOrderStrategyRegistry
import de.unikl.dbis.clash.physical.PhysicalGraph
import de.unikl.dbis.clash.query.parser.parseQuery
import de.unikl.dbis.clash.storm.builder.StormTopologyBuilder
import org.apache.storm.Config
import org.apache.storm.LocalCluster
import org.apache.storm.StormSubmitter
import org.apache.storm.generated.StormTopology
import org.apache.storm.utils.Utils
import org.json.JSONObject
import java.io.File

class RunStorm : CommonCLI(help="Run the query as Storm topology") {
    val query by argument().convert { parseQuery(it) }
    val dataCharacteristics by argument().convert {
        SymmetricJSONCharacteristics(JSONObject(it))
    }
    val outsideInterface by argument().convert {
        readOutsideInterface(JSONObject(it), config) // TODO make sure this is not executed before config is initialized
    }

    val localCluster by option("--local", "-l").flag(default = false)
    val nimbusHost by option("--nimbus", "-n").default("dbis-exp1")
    val clashJarPath by option("--jar-path", "-j").default(getClusterJarName())

    val runtime by option("--runtime", "-t", help="Runtime in ms before the topology gets killed").int().default(30_000)
    val quiet by option("--quiet", "-q", help="Do not output log messages").flag(default = false)

    val taskCapacity by option().long().default(Long.MAX_VALUE)
    val availableTasks by option().long().default(Long.MAX_VALUE)

    val probeOrderOptimizationStrategyParams by option()
            .convert { JSONObject(it) }
            .default(JSONObject())
    val probeOrderOptimizationStrategy by option()
            .choice(*ProbeOrderStrategyRegistry.SUPPORTED)
            .convert { ProbeOrderStrategyRegistry.initialize(it, probeOrderOptimizationStrategyParams.toMap()) }
            .default(ProbeOrderStrategyRegistry.initialize())

    val partitioningAttributesSelectionStrategyParams by option()
            .convert { JSONObject(it) }

    val globalStrategyParams by option()
            .convert { JSONObject(it) }
            .default(JSONObject())
    val globalStrategy by option(help="Global Optimization Strategy")
            .choice(*GlobalStrategyRegistry.SUPPORTED)
            .convert { GlobalStrategyRegistry.initialize(it, globalStrategyParams.toMap()) }
            .default(GlobalStrategyRegistry.initialize())

    val optimizationParameters: OptimizationParameters
        get() = OptimizationParameters(
                taskCapacity,
                availableTasks,
                globalStrategy,
                probeOrderOptimizationStrategy)


    override fun run() {
        if(quiet)
            disableLogging()

        if (localCluster) {
            runLocalCluster(optimizationParameters)
        } else {
            runRemoteCluster(optimizationParameters)
        }
    }

    fun runLocalCluster(optimizationParameters: OptimizationParameters) {
        val optimizationResult = globalStrategy.optimize(query, dataCharacteristics, optimizationParameters)

        val stormTopology = buildTopology(config, optimizationResult.physicalGraph)
//        config.setNumWorkers(4)

        // TODO: Not working
        config["storm.metrics.reporters"] = listOf(
                mapOf(
                        "class" to "org.apache.storm.metrics2.reporters.ConsoleStormReporter",
                        "daemons" to listOf("worker", "nimbus", "supervisor"),
                        "report.period" to 5,
                        "report.period.units" to "SECONDS"
                )
        )

        val cluster = LocalCluster()
        cluster.submitTopology("test", config, stormTopology)
        Utils.sleep(600_000)
        cluster.killTopology("test")
        cluster.shutdown()
    }

    fun runRemoteCluster(optimizationParameters: OptimizationParameters) {
        val optimizationResult = globalStrategy.optimize(query, dataCharacteristics, optimizationParameters)
        val stormTopology = buildTopology(config, optimizationResult.physicalGraph)
        System.setProperty("storm.jar", clashJarPath)
        config.put(Config.NIMBUS_SEEDS, listOf(nimbusHost))
//        config.put(Config.NIMBUS_THRIFT_PORT, 6627)
//        config.put(Config.STORM_ZOOKEEPER_PORT, 2181)
//        config.put(Config.STORM_ZOOKEEPER_SERVERS, ZOOKEEPER_ID)
//        config.setNumWorkers(16)
//        config.setMaxSpoutPending(5000)
        StormSubmitter.submitTopology("test", config, stormTopology)

    }

    fun buildTopology(config: ClashConfig, physicalGraph: PhysicalGraph): StormTopology {
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
