package de.unikl.dbis.clash.manager.db

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import de.unikl.dbis.clash.manager.model.ReceivedMessage
import de.unikl.dbis.clash.manager.model.SentCommand
import java.time.Instant
import java.util.SortedMap

class CaMEntry(val command: SentCommand? = null) {
    var messages: SortedMap<Instant, ReceivedMessage> = sortedMapOf()

    fun toJson(): JsonObject {
        val result = JsonObject()
        if (command != null) {
            result.add(COMMAND_ATTRIBUTE, command.toJson())
        }
        val jsonMessages = JsonObject()
        for ((timestamp, message) in messages) {
            jsonMessages.add(timestamp.toString(), message.toJson())
        }
        result.add(MESSAGES_ATTRIBUTE, jsonMessages)
        return result
    }

    companion object {
        const val COMMAND_ATTRIBUTE = "command"
        const val MESSAGES_ATTRIBUTE = "messages"
    }
}

fun commandsAndMessages(): JsonObject {
    val result = sortedMapOf<Instant, CaMEntry>()

    statement {
        val res = it.executeQuery("SELECT timestamp ts, command c, payload p FROM sent_commands ORDER BY timestamp")
        while (res.next()) {
            val timestamp = java.util.Date(res.getDate("ts").time).toInstant()
            val command = res.getString("c")
            val payload = if (res.getString("p") != null) JsonParser().parse(res.getString("p")).asJsonObject else null
            val sentCommand = SentCommand(timestamp, command, payload)
            result[timestamp] = CaMEntry(sentCommand)
        }
    }

    statement {
        val res = it.executeQuery("SELECT timestamp ts, message m, sender s, answer_to at, payload p FROM received_messages ORDER BY answer_to, timestamp")
        while (res.next()) {
            val timestamp = java.util.Date(res.getDate("ts").time).toInstant()
            val message = res.getString("m")
            val sender = res.getString("s")
            val answerTo = if (res.getDate("at") != null) java.util.Date(res.getDate("at").time).toInstant() else null
            val payload = if (res.getString("p") != null) JsonParser().parse(res.getString("p")).asJsonObject else null
            val receivedMessage = ReceivedMessage(timestamp, message, sender, answerTo, payload)

            val resultTs = answerTo ?: timestamp
            val entry = result.getOrPut(resultTs) { CaMEntry() }
            entry.messages[timestamp] = receivedMessage
        }
    }

    val jsonResult = JsonObject()
    for ((timestamp, entry) in result) {
        jsonResult.add(timestamp.toString(), entry.toJson())
    }
    return jsonResult
}

fun main() {
    println(commandsAndMessages().toString())
}
