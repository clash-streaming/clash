package de.unikl.dbis.clash.datagenerator.kafka

import java.util.Properties
import kotlin.concurrent.thread
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

const val BOOTSTRAP_SERVER = "dbis-expsrv15:9094"

class OutputGenerator() {
    var counter: Int = 0

    fun next(): String {
        return """{"attr": "val${counter++}"}"""
    }
}

data class Topic(
    val name: String,
    val delay: Long,
    val outputGenerator: OutputGenerator
)

fun main() {
    val props = kafkaProps()
    val topics = listOf(
        Topic("R", 1000, OutputGenerator()),
        Topic("S", 1000, OutputGenerator()),
        Topic("T", 1000, OutputGenerator()))

    topics.forEach {
        thread {
            val producer = KafkaProducer<String, String>(props)
            val kafkaTopic = it.name

            createTopic(kafkaTopic)

            while (true) {
                producer.send(ProducerRecord<String, String>(kafkaTopic, it.outputGenerator.next()))
                Thread.sleep(it.delay)
            }
        }
    }
}

fun kafkaProps(): Properties {
    val props = Properties()
    props["bootstrap.servers"] = BOOTSTRAP_SERVER
    props["acks"] = "all"
    props["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
    props["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
    return props
}

fun createTopic(topicName: String) {
    val adminClient = Admin.create(kafkaProps())
    val result = adminClient.createTopics(listOf(NewTopic(topicName, 1, 1)))
    adminClient.close()
    println("Topic `$topicName' created!")
}
