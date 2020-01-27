package de.unikl.dbis.clash.flexstorm

import com.google.gson.JsonParser
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import java.time.Duration
import java.time.Instant
import java.util.Arrays
import java.util.Properties

class KafkaSpout(val topicName: String, val relation: String) : BaseRichSpout() {
    lateinit var collector: SpoutOutputCollector
    lateinit var consumer: KafkaConsumer<String, String>

    val pollTimeout = Duration.ofMillis(10)

    override fun nextTuple() {
        val records = this.consumer.poll(pollTimeout)
        for (record in records) {
            val rawTuple = record.value()
            val jsonTuple = JsonParser().parse(rawTuple).asJsonObject

            val now = Instant.now()
            collector.emit(createSpoutOutput(now.epochSecond, jsonTuple, relation))
        }
    }

    override fun open(conf: MutableMap<String, Any>?, context: TopologyContext?, collector: SpoutOutputCollector?) {
        this.collector = collector!!
        val props = kafkaConsumerProperties()
        consumer = KafkaConsumer(props)
        consumer.subscribe(Arrays.asList(this.topicName))
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer?) {
        declarer!!.declare(SPOUT_OUTPUT_SCHEMA)
    }
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
