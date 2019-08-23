package de.unikl.dbis.clash.api

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.choice
import com.github.ajalt.clikt.parameters.types.long
import de.unikl.dbis.clash.datacharacteristics.SymmetricJSONCharacteristics
import de.unikl.dbis.clash.optimizer.GlobalStrategyRegistry
import de.unikl.dbis.clash.optimizer.OptimizationParameters
import de.unikl.dbis.clash.optimizer.PartitioningAttributesSelection
import de.unikl.dbis.clash.optimizer.ProbeOrderStrategyRegistry
import de.unikl.dbis.clash.physical.toJson
import de.unikl.dbis.clash.query.AttributeAccess
import de.unikl.dbis.clash.query.AttributeAccessList
import de.unikl.dbis.clash.query.RelationAlias
import de.unikl.dbis.clash.query.parser.parseQuery
import org.json.JSONObject


class Optimize : CommonCLI(help="Show the optimization result of a query") {
    val query by argument().convert { parseQuery(it) }
    val dataCharacteristics by argument().convert {
        SymmetricJSONCharacteristics(JSONObject(it))
    }

    val onlyTreeOptimizationResult by option("--onlyTreeOptimizationResult", "-t").flag(default = false)
    val onlyPhysicalGraphResult by option("--onlyPhysicalGraph", "-p").flag(default = false)
    val nice by option("--nice").flag(default = false)

    val taskCapacity by option().long().default(Long.MAX_VALUE)
    val availableTasks by option().long().default(Long.MAX_VALUE)

    val probeOrderOptimizationStrategyParams by option()
            .convert { JSONObject(it) }
            .default(JSONObject())
    val probeOrderOptimizationStrategy by option()
            .choice(*ProbeOrderStrategyRegistry.SUPPORTED)
            .convert { ProbeOrderStrategyRegistry.initialize(it, probeOrderOptimizationStrategyParams.toMap()) }
            .default(ProbeOrderStrategyRegistry.initialize())

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
                probeOrderOptimizationStrategy
        )

    override fun run() {
        disableLogging()

        val clash = Clash(config, dataCharacteristics, optimizationParameters, query)

        if (onlyTreeOptimizationResult) {
            printNice(clash.optimize().intermediateResult!!)
            return
        }
        if (onlyPhysicalGraphResult) {
            printNice(clash.optimize().physicalGraph.toJson())
            return
        }

        val result = JSONObject()
        result.put("optimizationResult", clash.optimize().intermediateResult)
        result.put("physicalGraphResult", clash.optimize().physicalGraph.toJson())
        printNice(result)
    }

    fun printNice(input: Any) {
        if(input is JSONObject)
            if(nice)
                println(input.toString(2))
            else
                println(input)
    }

}

/**
 * Parses an attribute selection in the form of:
 *
 * x:a;y:b;z:c,d;x,y:b
 *
 * This is a sequence of semicolon separated parts which again are colon delimited pairs
 * of comma separated lists. The above list for example translates to:
 *
 * x     -> a
 * y     -> b
 * z     -> c, d
 * x, y  -> b
 *
 * This means, the relation with relationAlias x should be partitioned according to attribute a and
 * relation with relationAlias z should be partitioned according to attributes c and d in that order.
 * The join of relations x, y (if it is materialized) should be partitioned according to attribute b.
 *
 * TODO this does not work universally yet
 */
fun parseAttributeSelection(string: String): PartitioningAttributesSelection? {
    val selection = mutableMapOf<List<RelationAlias>, AttributeAccessList>()

    string.split(";")
            .forEach {
                val (relations, attributes) = it.split(":")
                selection[relations.split(",").map(::RelationAlias)] = attributes.split(",").map { attrs -> AttributeAccess(it, attrs) }
            }

    return selection
}
