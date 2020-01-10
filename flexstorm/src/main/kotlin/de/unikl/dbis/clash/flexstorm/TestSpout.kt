package de.unikl.dbis.clash.flexstorm

import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.utils.Utils
import org.json.simple.JSONObject
import org.json.simple.parser.JSONParser
import java.time.Duration
import java.time.Instant

class TestSpout(val startTime: Instant, val relation: String, tuples: Map<Long, String>) : BaseRichSpout() {
    lateinit var collector: SpoutOutputCollector
    val sortedTuples = tuples.toSortedMap()
    var finished = false

    override fun nextTuple() {
        if(finished) {
            Utils.sleep(1000)
            return
        }
        val now = Instant.now()
        if(startTime > now) {
            Utils.sleep(Duration.between(now, startTime).toMillis())
        }

        val firstKey = sortedTuples.firstKey()
        val tuple = sortedTuples[firstKey]!!
        val jsonParser = JSONParser()
        val jsonTuple = jsonParser.parse(tuple) as JSONObject
        sortedTuples.remove(firstKey)

        val desiredTime = startTime.plusMillis(firstKey)
        Utils.sleep(Duration.between(now, desiredTime).toMillis())
        collector.emit(createSpoutOutput(now.epochSecond, jsonTuple, relation))

        if(sortedTuples.isEmpty()) {
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