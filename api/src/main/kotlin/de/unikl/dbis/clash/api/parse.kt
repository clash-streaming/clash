package de.unikl.dbis.clash.api

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.arguments.argument
import de.unikl.dbis.clash.query.Query
import de.unikl.dbis.clash.query.parser.parseQuery
import org.json.JSONArray
import org.json.JSONObject


/**
 * This command reads a query and outputs the interpreted relation as JSON string.
 *
 * This query:
 *
 * ```sql
 * SELECT ps_partkey
 * FROM partsupp ps, supplier s, nation n
 * WHERE ps.suppkey = s.suppkey and s.nationkey = n.nationkey
 * AND n.name = '[NATION]'
 * ```
 *
 * Will be translated to:
 *
 * ```json
 * {
 *   "query": "SELECT ps_partkey ..."
 *   "baseRelations": ["partsupp", "supplier", "nation"]
 *   "baseRelationAliases": [ps, s, n]
 *   "binaryPredicates": ["ps.suppkey = s.suppkey", "s.nationkey = n.nationkey"]
 *   "unaryPredicates: ["n.name = '[NATION]'"]
 * }
 * ```
 *
 * If there is an error an object with error attribute is produced.
 */
class QueryCommand : CliktCommand(name="query", help="Parse a query and show interpreted relation as JSON") {
    val query: String by argument()
    override fun run() {
        try {
            val parsedQuery = parseQuery(query)
            success(parsedQuery)
        } catch(e: Exception) {
            failure(e)
        }
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

    fun baseRelations(query: Query): JSONArray {
        return JSONArray(query.result.aliases.map { it.inner })
    }

    fun baseRelationAliases(query: Query): JSONArray {
        return JSONArray(query.result.aliases.map { it.inner })
    }

    fun binaryPredicates(query: Query): JSONArray {
        return JSONArray(query.result.binaryPredicates.map { it.toString() })
    }

    fun unaryPredicates(query: Query): JSONArray {
        return JSONArray(query.result.unaryPredicates.map { it.toString() })
    }
}