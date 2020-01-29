package de.unikl.dbis.clash.flexstorm.control

import com.google.gson.JsonObject
import java.io.Serializable

class InternalStatisticsMessage(
    val totalNumberOfTuplesStored: Long
) : Serializable {
    fun asJsonObject(): JsonObject {
        val result = JsonObject()
        result.addProperty("totalNumberOfMesagesStored", totalNumberOfTuplesStored)
        return result
    }
}

class StatisticsGatherer(val numberOfSources: Int) {
    val messages = mutableMapOf<Int, InternalStatisticsMessage>()

    fun add(taskId: Int, message: InternalStatisticsMessage) {
        messages[taskId] = message
    }

    fun howManyLeft() = numberOfSources - messages.size

    fun finished() = howManyLeft() == 0

    fun get(): Map<Int, InternalStatisticsMessage> = messages
}

fun createStatisticsMessage(internalMap: Map<Int, InternalStatisticsMessage>): JsonObject {
    val result = JsonObject()
    internalMap.forEach { result.add(it.key.toString(), it.value.asJsonObject())}
    return result
}
