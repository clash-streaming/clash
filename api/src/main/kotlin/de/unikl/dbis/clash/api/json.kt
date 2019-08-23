package de.unikl.dbis.clash.api

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import de.unikl.dbis.clash.ClashConfig
import de.unikl.dbis.clash.datacharacteristics.SymmetricJSONCharacteristics
import de.unikl.dbis.clash.optimizer.*
import de.unikl.dbis.clash.optimizer.materializationtree.OptimizationError
import de.unikl.dbis.clash.optimizer.probeorder.ProbeOrderOptimizationStrategy
import de.unikl.dbis.clash.physical.toJson
import de.unikl.dbis.clash.query.AttributeAccess
import de.unikl.dbis.clash.query.BinaryEquality
import de.unikl.dbis.clash.query.Query
import de.unikl.dbis.clash.query.RelationAlias
import de.unikl.dbis.clash.query.parser.parseQuery
import org.json.JSONObject


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
 *     "partitioningAttributesSelectionStrategy": { "name": "Explicit", ... }
 *   },
 *   "query": "SELECT ..."
 *
 * }
 */
class Json : CliktCommand() {
    val params by argument().convert { JSONObject(it) }

    override fun run() {
        disableLogging()

        try {
            val config = ClashConfig() // TODO
            val dataCharacteristics = SymmetricJSONCharacteristics(params.getJSONObject("dataCharacteristics"))
            val optimizationParameters = parseOptimizationParameters(params.getJSONObject("optimizationParameters"))
            val query = parseQuery(params.getString("query"))
            val clash = Clash(config, dataCharacteristics, optimizationParameters, query)

            val output = JSONObject()
            var bestOptimizationResult = clash.optimize()

            val optimizationJson = JSONObject()
            optimizationJson.put("pCost", bestOptimizationResult.costEstimation.probeCost)
            optimizationJson.put("sCost", bestOptimizationResult.costEstimation.storageCost)
            optimizationJson.put("numTasks", bestOptimizationResult.costEstimation.numTasks)
            optimizationJson.put("tree", bestOptimizationResult.intermediateResult)
            output.put("optimizationResult", optimizationJson)

            output.put("physicalGraphResult", bestOptimizationResult.physicalGraph.toJson())
            print(output.toString(2))
        } catch (e: OptimizationError) {
            val json = JSONObject()
            json.put("error", e.toString())
            json.put("error_details", e.localizedMessage)
            print(json.toString(2))
        }
    }
}

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

fun partitionChoices(query: Query): Collection<PartitioningAttributesSelection> {
    if(query.result.binaryPredicates.isEmpty()) {
        return listOf()
    }

    val candidates = mutableMapOf<RelationAlias, MutableList<AttributeAccess>>()
    for(predicate in query.result.binaryPredicates) {
        if(predicate is BinaryEquality) {
            candidates.getOrPut(predicate.leftAttributeAccess.relationAlias, { mutableListOf() })
            candidates[predicate.leftAttributeAccess.relationAlias]!!.add(predicate.leftAttributeAccess)
            candidates.getOrPut(predicate.rightAttributeAccess.relationAlias, { mutableListOf() })
            candidates[predicate.rightAttributeAccess.relationAlias]!!.add(predicate.rightAttributeAccess)
        }
    }

    fun recursive(input: Map<RelationAlias, List<AttributeAccess>>): List<PartitioningAttributesSelection> {
        if(input.isEmpty()) {
            return listOf()
        }
        if(input.size == 1) {
            val entry = input.entries.first()
            return listOf(mapOf(listOf(entry.key) to entry.value))
        }

        val firstEntry = input.entries.first()
        val tailOptions = recursive(input.minus(firstEntry.key))

        val result = mutableListOf<PartitioningAttributesSelection>()
        for(attribute in firstEntry.value) {
            for(tail in tailOptions) {
                result.add(tail.plus(Pair(listOf(firstEntry.key), listOf(attribute))))
            }
        }
        return result
    }

    return recursive(candidates)
}
