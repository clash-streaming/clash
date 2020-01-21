package de.unikl.dbis.clash.manager.model

import com.google.gson.JsonObject
import de.unikl.dbis.clash.manager.db.connection
import de.unikl.dbis.clash.manager.db.toSqlite
import java.time.Instant

data class ReceivedMessage(
    val timestamp: Instant,
    val message: String,
    val sender: String,
    val answerTo: Instant? = null,
    val payload: JsonObject? = null
) {
    fun insert() {
        connection {
            val stmt = it.prepareStatement("""INSERT INTO
|               received_messages(timestamp, message, sender, answer_to, payload)
|               VALUES(?, ?, ?, ?, ?)""".trimMargin())
            stmt.setString(1, timestamp.toSqlite())
            stmt.setString(2, message)
            stmt.setString(3, sender)
            stmt.setString(4, answerTo?.toSqlite())
            stmt.setString(5, payload?.toString())
            stmt.execute()
        }
    }

    fun toJson(): JsonObject {
        val result = JsonObject()
        result.addProperty(TIMESTAMP_ATTRIBUTE, timestamp.toString())
        result.addProperty(MESSAGE_ATTRIBUTE, message)
        result.addProperty(SENDER_ATTRIBUTE, sender)
        if (answerTo != null) result.addProperty(ANSWER_TO_ATTRIBUTE, answerTo.toString())
        if (payload != null) result.add(PAYLOAD_ATTRIBUTE, payload)
        return result
    }

    companion object {
        const val TIMESTAMP_ATTRIBUTE = "timestamp"
        const val MESSAGE_ATTRIBUTE = "message"
        const val SENDER_ATTRIBUTE = "sender"
        const val ANSWER_TO_ATTRIBUTE = "answer_to"
        const val PAYLOAD_ATTRIBUTE = "payload"
    }
}
