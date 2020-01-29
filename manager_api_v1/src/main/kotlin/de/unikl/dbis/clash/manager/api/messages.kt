package de.unikl.dbis.clash.manager.api

import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive

const val MESSAGE_FIELD = "message"

interface Message {
    fun id(): String
    fun toJson(): JsonObject
}

const val MESSAGE_TOPOLOGY_ALIVE = "topology-alive"
class TopologyAliveMessage : Message {
    override fun id() = MESSAGE_TOPOLOGY_ALIVE

    override fun toJson(): JsonObject {
        val obj = JsonObject()
        obj.add(MESSAGE_FIELD, JsonPrimitive(MESSAGE_TOPOLOGY_ALIVE))
        return obj
    }
}

const val MESSAGE_PONG = "pong"
class PongMessage : Message {
    override fun id() = MESSAGE_PONG

    override fun toJson(): JsonObject {
        val obj = JsonObject()
        obj.add(MESSAGE_FIELD, JsonPrimitive(MESSAGE_PONG))
        return obj
    }
}

const val MESSAGE_RESET = "reset"
class ResetMessage : Message {
    override fun id() = MESSAGE_RESET

    override fun toJson(): JsonObject {
        val obj = JsonObject()
        obj.add(MESSAGE_FIELD, JsonPrimitive(MESSAGE_RESET))
        return obj
    }
}

const val MESSAGE_STATISTICS = "statistics"
const val FLEXBOLT_STATISTICS_FIELD = "flex_bolt_statistics"
const val GLOBAL_STATISTICS_FIELD = "global_statistics"
class StatisticsMessage(
    val globalStatistics: JsonObject,
    val flexBoltStatistics: JsonObject
) : Message {
    override fun id() = MESSAGE_STATISTICS

    override fun toJson(): JsonObject {
        val obj = JsonObject()
        obj.addProperty(MESSAGE_FIELD, MESSAGE_STATISTICS)
        obj.add(GLOBAL_STATISTICS_FIELD, globalStatistics)
        obj.add(FLEXBOLT_STATISTICS_FIELD, flexBoltStatistics)
        return obj
    }
}
