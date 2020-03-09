package de.unikl.dbis.clash.flexstorm.kafka

import java.util.Properties
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

class StartingFromEndListener<K, V>(private val consumer: Consumer<K, V>) : ConsumerRebalanceListener {

    override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>) {
        consumer.seekToEnd(partitions)
    }

    override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>) = Unit
}

fun kafkaConsumerProperties(): Properties {
    val props = Properties()
    props["bootstrap.servers"] = "dbis-expsrv15:9094"
    props["group.id"] = "test"
    props["enable.auto.commit"] = "true"
    props["auto.commit.interval.ms"] = "1000"
    props["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
    props["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"

    return props
}
