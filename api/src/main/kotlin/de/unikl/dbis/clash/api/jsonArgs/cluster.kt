package de.unikl.dbis.clash.api.jsonArgs

import com.beust.klaxon.TypeAdapter
import com.beust.klaxon.TypeFor
import kotlin.reflect.KClass

@TypeFor(field = "type", adapter = JsonClusterArgAdapter::class)
open class JsonClusterArg(
    val type: String
)

class JsonClusterArgAdapter : TypeAdapter<JsonClusterArg> {
    override fun classFor(type: Any): KClass<out JsonClusterArg> = when (type as String) {
        "local" -> JsonLocalCluster::class
        "remote" -> JsonRemoteCluster::class
        else -> throw IllegalArgumentException("Unknown type: $type")
    }
}

data class JsonLocalCluster(
    val sources: Map<String, JsonSourceArg>,
    val sink: JsonSinkArg
) : JsonClusterArg("local")
data class JsonRemoteCluster(
    val sources: Map<String, JsonSourceArg>,
    val sink: JsonSinkArg
) : JsonClusterArg("remote")
