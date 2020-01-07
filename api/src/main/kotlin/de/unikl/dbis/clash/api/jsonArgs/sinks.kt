package de.unikl.dbis.clash.api.jsonArgs

import com.beust.klaxon.TypeAdapter
import com.beust.klaxon.TypeFor
import de.unikl.dbis.clash.storm.bolts.CommonSinkI
import de.unikl.dbis.clash.storm.bolts.NullSinkBolt
import kotlin.reflect.KClass

@TypeFor(field = "type", adapter = JsonSinkArgAdapter::class)
abstract class JsonSinkArg(val type: String) {
    abstract fun get(): CommonSinkI
}
class JsonSinkArgAdapter : TypeAdapter<JsonSinkArg> {
    override fun classFor(type: Any): KClass<out JsonSinkArg> = when (type as String) {
        "null" -> NullSinkArg::class
        else -> throw IllegalArgumentException("Unknown type: $type")
    }
}

class NullSinkArg : JsonSinkArg("null") {
    override fun get(): CommonSinkI {
        return NullSinkBolt()
    }
}
