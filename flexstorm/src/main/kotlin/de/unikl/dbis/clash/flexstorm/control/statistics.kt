package de.unikl.dbis.clash.flexstorm.control

import java.io.Serializable

class StatisticsMessage() : Serializable

class StatisticsGatherer(val numberOfSources: Int) {
    val messages = mutableListOf<StatisticsMessage>()

    fun add(message: StatisticsMessage) {
        messages.add(message)
    }

    fun howManyLeft() = numberOfSources - messages.size

    fun finished() = howManyLeft() == 0

    fun get(): List<StatisticsMessage> = messages
}
