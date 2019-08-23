package de.unikl.dbis.clash.storm.spouts

import de.unikl.dbis.clash.ClashConfig
import de.unikl.dbis.clash.documents.Document
import de.unikl.dbis.clash.query.InputName
import de.unikl.dbis.clash.query.RelationAlias
import de.unikl.dbis.clash.support.KafkaConfig
import de.unikl.dbis.clash.support.kafkaConsumerProperties
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.json.JSONObject
import org.slf4j.LoggerFactory
import java.util.*


open class KafkaSpout(
        val inputName: InputName,
        val relationAlias: RelationAlias,
        private val topicName: String,
        val config: KafkaConfig) : CommonSpout(1) {
    protected var consumer: KafkaConsumer<String, String>? = null

    override fun open(conf: MutableMap<String, Any>?, topologyContext: TopologyContext?, spoutOutputCollector: SpoutOutputCollector?) {
        super.open(conf, topologyContext, spoutOutputCollector)
        val props = kafkaConsumerProperties(this.config)
        consumer = KafkaConsumer(props)
        consumer!!.subscribe(Arrays.asList(this.topicName))
    }

    override fun nextTuple() {
        val records = this.consumer!!.poll(100)
        for (record in records) {
            val rawTuple = record.value()
            val tupleObject: JSONObject
            try {
                tupleObject = JSONObject(rawTuple)
            } catch (e: org.json.JSONException) {
                LOG.warn("Could not parse raw tuple \"$rawTuple\"!", e)
                return
            }

            val document = Document()
            for (key in tupleObject.keySet()) {
                document[relationAlias, key] = tupleObject.get(key).toString()
            }
            super.emit(document)
        }
    }

    override fun toString(): String {
        return "KafkaSpout"
    }

    companion object {

        private val LOG = LoggerFactory.getLogger(KafkaSpout::class.java)
    }
}
