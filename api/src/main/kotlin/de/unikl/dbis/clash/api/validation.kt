package de.unikl.dbis.clash.api

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import de.unikl.dbis.clash.ClashConfig
import de.unikl.dbis.clash.datacharacteristics.AllCross
import de.unikl.dbis.clash.datacharacteristics.DataCharacteristics
import de.unikl.dbis.clash.datacharacteristics.TpchOnlyJoinsCharacteristics
import de.unikl.dbis.clash.documents.Document
import de.unikl.dbis.clash.optimizer.GlobalStrategy
import de.unikl.dbis.clash.optimizer.GlobalStrategyRegistry
import de.unikl.dbis.clash.optimizer.OptimizationParameters
import de.unikl.dbis.clash.physical.PhysicalGraph
import de.unikl.dbis.clash.query.*
import de.unikl.dbis.clash.query.parser.parseQuery
import de.unikl.dbis.clash.storm.bolts.CommonSinkI
import de.unikl.dbis.clash.storm.bolts.FileSinkBolt
import de.unikl.dbis.clash.storm.builder.StormTopologyBuilder
import de.unikl.dbis.clash.storm.spouts.CommonSpoutI
import de.unikl.dbis.clash.storm.spouts.JsonFileSpout
import org.apache.storm.LocalCluster
import org.apache.storm.generated.StormTopology
import org.apache.storm.utils.Utils
import org.json.JSONObject
import java.io.File


class Validate : CommonCLI(help="Validate certain scenarios") {
    val scenarioName by argument()
    val silent by option("--silent", "-s").flag(default = false)

    override fun run() {
        if (silent) disableLogging()

        config[ClashConfig.CLASH_TICK_RATE] = 0
        config[ClashConfig.CLASH_CONTROLLER_ENABLED] = false
        config[ClashConfig.CLASH_REPORTER_PARALLELISM] = 0

        val scenario = when(scenarioName) {
            "rst1" -> Scenario.rst1()
            "tpchq3j" -> Scenario.tpchq3j()
            else -> throw RuntimeException("Validation scenario $scenarioName unknown.")
        }
        runLocalCluster(scenario)
        compareJsonFiles(scenario.actualFilename, scenario.expectedFilename)
    }

    fun runLocalCluster(scenario: Scenario) {
        val optimizationResult = scenario.globalStrategy.optimize(scenario.query, scenario.dataCharacteristics, scenario.optimizationParameters)
        val stormTopology = buildTopology(config, optimizationResult.physicalGraph, scenario)
        val cluster = LocalCluster()
        cluster.submitTopology("test", config, stormTopology)
        Utils.sleep(30 * 1_000)
        cluster.killTopology("test")
        cluster.shutdown()
    }

    fun buildTopology(config: ClashConfig, physicalGraph: PhysicalGraph, scenario: Scenario): StormTopology {
        val builder = StormTopologyBuilder(
                physicalGraph,
                scenario.outsideInterface.sourceMap.toMutableMap(),
                scenario.inputMap,
                scenario.outsideInterface.sink,
                config)
        return builder.build()
    }
}

data class Scenario(
        val query: Query,
        val dataCharacteristics: DataCharacteristics,
        val globalStrategy: GlobalStrategy,
        val optimizationParameters: OptimizationParameters,
        val outsideInterface: OutsideInterface,
        val inputMap: Map<RelationAlias, InputName>,
        val actualFilename: String,
        val expectedFilename: String
) {
    companion object {
        fun rst1(): Scenario {
            return Scenario (
                    parseQuery("SELECT * FROM r, s, t WHERE r.x = s.x AND s.y = t.y"),
                    AllCross(),
                    GlobalStrategyRegistry.initialize(),
                    OptimizationParameters(),
                    OutsideInterface(mapOf(
                            InputName("r") to JsonFileSpout(InputName("r"), RelationAlias("r"),"validation/rst1_r.json"),
                            InputName("s") to JsonFileSpout(InputName("s"), RelationAlias("s"),"validation/rst1_s.json"),
                            InputName("t") to JsonFileSpout(InputName("t"), RelationAlias("t"),"validation/rst1_t.json")
                    ),
                            FileSinkBolt("output", "validation/rst1_result.json")
                    ),mapOf(
                    RelationAlias("r") to InputName("r"),
                    RelationAlias("s") to InputName("s"),
                    RelationAlias("t") to InputName("t")
            ),
                    "validation/rst1_result.json",
                    "validation/rst1_expected.json")
        }

        fun tpchq3j(): Scenario {
            return Scenario(
                    TpcHOnlyJoins.q3(),
                    TpchOnlyJoinsCharacteristics.q3(),
                    GlobalStrategyRegistry.initialize(),
                    OptimizationParameters(),
                    OutsideInterface(mapOf(
                            InputName("customer") to JsonFileSpout(InputName("customer"), RelationAlias("customer"),"validation/tpch3_customer.json"),
                            InputName("lineitem") to JsonFileSpout(InputName("lineitem"), RelationAlias("lineitem"),"validation/tpch3_lineitem.json"),
                            InputName("orders") to JsonFileSpout(InputName("orders"), RelationAlias("orders"),"validation/tpch3_orders.json")
                    ),
                            FileSinkBolt("output", "validation/tpchq3j_result.json")
                    )
                    ,
                    mapOf(
                            RelationAlias("customer") to InputName("customer"),
                            RelationAlias("lineitem") to InputName("lineitem"),
                            RelationAlias("orders") to InputName("orders")
                    ),
                    "validation/tpchq3j_result.json",
                    "validation/tpchq3j_expected.json")
        }
    }
}

data class OutsideInterface(
        val sourceMap: Map<InputName, CommonSpoutI>, // source name -> filename for that input
        val sink: CommonSinkI // filename for the output
)

fun compareJsonFiles(actual: String, expected: String) {
    fun getDocumentsValues(filename: String): Set<Document> {
        val jsonObjects = readJsonFile(filename)
        val documents = jsonObjects.map {
            val document = Document()
            for (key in it.keySet()) {
                document[AttributeAccess(key)] = it.get(key).toString()
            }
            document
        }
        return documents.toSet()
    }

    val actualDocuments = getDocumentsValues(actual)
    val expectedDocuments = getDocumentsValues(expected)

    println("Comparing files $actual and $expected")
    if(actualDocuments == expectedDocuments)
        println("They are equal :)")
    else {
        println("They are not equal :(")
        val superfluous = (actualDocuments - expectedDocuments).size
        println("- there are $superfluous many elements in actual that were not expected")
        val missing = (expectedDocuments - actualDocuments).size
        println("- there are $missing many elements missing that were expected")
        val correct = expectedDocuments.intersect(actualDocuments).size
        println("- there are $correct many elements correct")
    }
}

fun readJsonFile(path: String): Set<JSONObject> = File(path).readLines().map { JSONObject(it) }.toSet()