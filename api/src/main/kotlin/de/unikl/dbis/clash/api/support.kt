package de.unikl.dbis.clash.api

import de.unikl.dbis.clash.ClashConfig
import de.unikl.dbis.clash.documents.Document
import de.unikl.dbis.clash.query.InputName
import de.unikl.dbis.clash.query.RelationAlias
import de.unikl.dbis.clash.storm.bolts.CommonSinkI
import de.unikl.dbis.clash.storm.bolts.NullSinkBolt
import de.unikl.dbis.clash.storm.bolts.PgSinkBolt
import de.unikl.dbis.clash.storm.spouts.CommonSpoutI
import de.unikl.dbis.clash.storm.spouts.LinewiseFileSpout
import de.unikl.dbis.clash.storm.spouts.PgTableSpout
import de.unikl.dbis.clash.support.PostgresConfig
import org.json.JSONObject


/**
 * Interpret a json object looking as follows:
 *
 * {
 *   "sources":
 *   {
 *      "x": {
 *        "type": "postgres",
 *        "query": "SELECT * FROM x",
 *        "millisDelay": 1
 *      },
 *      "y": {
 *        "type": "tpch-file",
 *        "path": "/srv/...",
 *        "attributes": ["x", "y"],
 *        "millisDelay": 1
 *      }
 *   },
 *   "sink": {
 *     "type": "null"
 *   }
 * }
 */
fun readOutsideInterface(jsonObject: JSONObject, config: ClashConfig): Pair<MutableMap<InputName, CommonSpoutI>, CommonSinkI> {
    fun readTcphFileSource(inputName: InputName, jsonObject: JSONObject): LinewiseFileSpout {
        val path = jsonObject.getString("path")
        val millisDelay = jsonObject.getInt("millisDelay")
        val attributes = jsonObject.getJSONArray("attributes")
                .toList()
                .map { it.toString() }
        val fn: (RelationAlias, String) -> Document = { relationAlias, line ->
            val document = Document()
            val values = line.trim().split("|")
            for((index, attribute) in attributes.withIndex()) {
                document[relationAlias, attribute] = values[index]
            }
            document
        }
        return LinewiseFileSpout(inputName, RelationAlias(inputName.inner), path, millisDelay, fn)
    }

    fun readPostgresSource(inputName: InputName, jsonObject: JSONObject): PgTableSpout {
        val query = jsonObject.getString("query")
        val millisDelay = jsonObject.getInt("millisDelay")
        val postgresConfig = TODO()
        return PgTableSpout(inputName, RelationAlias(inputName.inner), query, millisDelay, postgresConfig)
    }

    fun readSource(inputName: InputName, jsonObject: JSONObject): CommonSpoutI {
        return when(val type = jsonObject.getString("type")) {
            "postgres" -> readPostgresSource(inputName, jsonObject)
            "tpch-file" -> readTcphFileSource(inputName, jsonObject)
            else -> throw RuntimeException("Don't know source type $type.")
        }

    }

    fun readSources(jsonObject: JSONObject): MutableMap<InputName, CommonSpoutI> {
        val result = mutableMapOf<InputName, CommonSpoutI>()
        for(key: String in jsonObject.keySet()) {
            result[InputName(key)] = readSource(InputName(key), jsonObject.getJSONObject(key))
        }
        return result
    }

    fun readSink(jsonObject: JSONObject): CommonSinkI {
        fun readPostgresSink(jsonObject: JSONObject): PgSinkBolt {
            val query = jsonObject.getString("query") ?: throw RuntimeException("Postgres sink requires query.")
            val postgresConfig = TODO()
            return PgSinkBolt("result", query, postgresConfig)
        }

        return when(val type = jsonObject.getString("type")) {
            "null" -> NullSinkBolt()
            "postgres" -> readPostgresSink(jsonObject)
            else -> throw RuntimeException("Don't know sink type $type.")
        }
    }

    return Pair(
            readSources(jsonObject.getJSONObject("sources")),
            readSink(jsonObject.getJSONObject("sink"))
    )
}
