package de.unikl.dbis.clash.flexstorm

import com.google.gson.JsonParser
import java.time.Duration
import java.time.Instant
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.utils.Utils

class TestSpout(val startTime: Instant, val relation: String, tuples: Map<Long, String>) : BaseRichSpout() {
    lateinit var collector: SpoutOutputCollector
    val sortedTuples = tuples.toSortedMap()
    var finished = false

    override fun nextTuple() {
        if (finished) {
            Utils.sleep(1000)
            return
        }
        val now = Instant.now()
        if (startTime > now) {
            Utils.sleep(Duration.between(now, startTime).toMillis())
        }

        val firstKey = sortedTuples.firstKey()
        val tuple = sortedTuples[firstKey]!!
        val jsonTuple = JsonParser().parse(tuple).asJsonObject
        sortedTuples.remove(firstKey)

        val desiredTime = startTime.plusMillis(firstKey)
        Utils.sleep(Duration.between(now, desiredTime).toMillis())
        collector.emit(createSpoutOutput(now.epochSecond, jsonTuple, relation))

        if (sortedTuples.isEmpty()) {
            finished = true
        }
    }

    override fun open(conf: MutableMap<String, Any>?, context: TopologyContext?, collector: SpoutOutputCollector?) {
        this.collector = collector!!
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer?) {
        declarer!!.declare(SPOUT_OUTPUT_SCHEMA)
    }
}
