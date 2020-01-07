package de.unikl.dbis.clash.api.json_args

import com.beust.klaxon.TypeAdapter
import com.beust.klaxon.TypeFor
import de.unikl.dbis.clash.documents.Document
import de.unikl.dbis.clash.query.InputName
import de.unikl.dbis.clash.query.RelationAlias
import de.unikl.dbis.clash.storm.spouts.CommonSpoutI
import de.unikl.dbis.clash.storm.spouts.LinewiseFileSpout
import de.unikl.dbis.clash.storm.spouts.PgTableSpout
import de.unikl.dbis.clash.support.PostgresConfig
import java.io.Serializable
import kotlin.reflect.KClass

@TypeFor(field = "type", adapter = JsonSourceArgAdapter::class)
abstract class JsonSourceArg(val type: String) : Serializable {
    abstract fun get(inputName: InputName): CommonSpoutI
}

class JsonSourceArgAdapter : TypeAdapter<JsonSourceArg> {
    override fun classFor(type: Any): KClass<out JsonSourceArg> = when (type as String) {
        "postgres" -> JsonPostgresSpoutArg::class
        "tpch-file" -> TpchFileSpoutArg::class
        else -> throw IllegalArgumentException("Unknown type: $type")
    }
}

data class JsonPostgresSpoutArg(
    val query: String,
    val databaseName: String,
    val databaseUser: String,
    val databasePassword: String,
    val millisDelay: Int
) : JsonSourceArg("postgres") {
    override fun get(inputName: InputName): CommonSpoutI {
        val config = PostgresConfig("localhost", "5432",
                databaseName, databaseUser, databasePassword)

        return PgTableSpout(
                inputName,
                RelationAlias(inputName.inner),
                query,
                millisDelay,
                config
        )
    }
}

data class TpchFileSpoutArg(
    val path: String,
    val attributes: List<String>,
    val millisDelay: Int
) : JsonSourceArg("postgres") {
    override fun get(inputName: InputName): CommonSpoutI {
        val fn: (RelationAlias, String) -> Document = { relationAlias, line ->
            val document = Document()
            val values = line.trim().split("|")
            for ((index, attribute) in attributes.withIndex()) {
                document[relationAlias, attribute] = values[index]
            }
            document
        }
        return LinewiseFileSpout(inputName, RelationAlias(inputName.inner), path, millisDelay, fn)
    }
}
