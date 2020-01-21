package de.unikl.dbis.clash.manager.model

import com.google.gson.JsonObject
import de.unikl.dbis.clash.manager.db.connection
import de.unikl.dbis.clash.manager.db.toSqlite
import java.time.Instant

data class SentCommand(
    val timestamp: Instant,
    val command: String,
    val payload: JsonObject? = null
) {
    fun insert() {
        connection {
            val stmt = it.prepareStatement("""INSERT INTO
|               sent_commands(timestamp, command, payload)
|               VALUES(?, ?, ?)""".trimMargin())
            stmt.setString(1, timestamp.toSqlite())
            stmt.setString(2, command)
            stmt.setString(3, payload?.toString())
            stmt.execute()
        }
    }

    fun toJson(): JsonObject {
        val result = JsonObject()
        result.addProperty(TIMESTAMP_ATTRIBUTE, timestamp.toString())
        result.addProperty(COMMAND_ATTRIBUTE, command)
        if (payload != null) result.add(PAYLOAD_ATTRIBUTE, payload)
        return result
    }

    companion object {
        const val TIMESTAMP_ATTRIBUTE = "timestamp"
        const val COMMAND_ATTRIBUTE = "command"
        const val PAYLOAD_ATTRIBUTE = "payload"
    }
}
