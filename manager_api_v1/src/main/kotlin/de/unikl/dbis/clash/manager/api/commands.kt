package de.unikl.dbis.clash.manager.api

import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import java.lang.RuntimeException
import java.time.Instant

const val COMMAND_FIELD = "command"


const val COMMAND_RESET = "reset"

interface Command {
    fun id(): String
    fun toJson(): JsonObject

    fun fromJson(obj: JsonObject): Command {
        return when(obj[COMMAND_FIELD].asString) {
            COMMAND_PING -> PingCommand(Instant.parse(obj[COMMAND_PING_SENT].asString))
            COMMAND_RESET -> ResetCommand()
            else -> { throw UnknownCommandException(obj[COMMAND_FIELD])}
        }
    }
}

class UnknownCommandException(jsonElement: JsonElement?) : RuntimeException("Can't understand command $jsonElement")

const val COMMAND_PING = "ping"
const val COMMAND_PING_SENT = "sent"
class PingCommand(val sent: Instant) : Command {
    override fun id() = COMMAND_PING

    override fun toJson(): JsonObject {
        val obj = JsonObject()
        obj.add(COMMAND_FIELD, JsonPrimitive(COMMAND_PING))
        obj.add(COMMAND_PING_SENT, JsonPrimitive(sent.toString()))
        return obj
    }
}

class ResetCommand : Command {
    override fun id() = COMMAND_RESET

    override fun toJson(): JsonObject {
        val obj = JsonObject()
        obj.add(COMMAND_FIELD, JsonPrimitive(COMMAND_RESET))
        return obj
    }
}
