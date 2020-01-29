package de.unikl.dbis.clash.flexstorm.kafka

import com.google.gson.JsonParser
import de.unikl.dbis.clash.flexstorm.SPOUT_OUTPUT_SCHEMA
import de.unikl.dbis.clash.flexstorm.createSpoutOutput
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import java.time.Duration
import java.time.Instant
import java.util.Arrays

class KafkaJsonSpout(val topicName: String, val relation: String) : BaseRichSpout() {
    lateinit var collector: SpoutOutputCollector
    lateinit var consumer: KafkaConsumer<String, String>

    val pollTimeout = Duration.ofMillis(10)

    override fun nextTuple() {
        val records = this.consumer.poll(pollTimeout)
        for (record in records) {
            val rawTuple = record.value()
            val jsonTuple = JsonParser().parse(rawTuple).asJsonObject
            val now = Instant.now()
            collector.emit(
                createSpoutOutput(
                    now.epochSecond,
                    jsonTuple,
                    relation
                )
            )
        }
    }

    override fun open(conf: MutableMap<String, Any>?, context: TopologyContext?, collector: SpoutOutputCollector?) {
        this.collector = collector!!
        val props = kafkaConsumerProperties()
        consumer = KafkaConsumer(props)
        consumer.subscribe(Arrays.asList(this.topicName),
            StartingFromEndListener<String, String>(consumer)
        )
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer?) {
        declarer!!.declare(SPOUT_OUTPUT_SCHEMA)
    }
}
