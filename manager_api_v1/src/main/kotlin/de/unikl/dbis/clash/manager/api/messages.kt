package de.unikl.dbis.clash.manager.api

import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive

const val MESSAGE_FIELD = "message"

const val MESSAGE_TOPOLOGY_ALIVE = "topology-alive"
const val MESSAGE_PONG = "pong"
const val MESSAGE_RESET = "reset"

interface Message {
    fun id(): String
    fun toJson(): JsonObject
}

class TopologyAliveMessage : Message {
    override fun id() = MESSAGE_TOPOLOGY_ALIVE

    override fun toJson(): JsonObject {
        val obj = JsonObject()
        obj.add(MESSAGE_FIELD, JsonPrimitive(MESSAGE_TOPOLOGY_ALIVE))
        return obj
    }
}

class PongMessage : Message {
    override fun id() = MESSAGE_PONG

    override fun toJson(): JsonObject {
        val obj = JsonObject()
        obj.add(MESSAGE_FIELD, JsonPrimitive(MESSAGE_PONG))
        return obj
    }
}

class ResetMessage : Message {
    override fun id() = MESSAGE_RESET

    override fun toJson(): JsonObject {
        val obj = JsonObject()
        obj.add(MESSAGE_FIELD, JsonPrimitive(MESSAGE_RESET))
        return obj
    }
}
