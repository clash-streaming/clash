package de.unikl.dbis.clash.flexstorm

import io.ktor.client.HttpClient
import io.ktor.client.engine.HttpClientEngine
import io.ktor.client.engine.cio.CIO
import io.ktor.client.features.json.GsonSerializer
import io.ktor.client.features.json.JsonFeature
import io.ktor.client.request.get
import io.ktor.client.request.url
import io.ktor.http.ContentType
import io.ktor.http.contentType
import kotlinx.coroutines.runBlocking
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Values
import org.json.simple.parser.JSONParser
import org.apache.storm.utils.Utils

data class ControlMessage(val instruction: String)

class ControlSpout(val managerUrl: String) : BaseRichSpout() {
    private lateinit var collector: SpoutOutputCollector
    lateinit var jsonParser: JSONParser

    override fun nextTuple() {

        val client = HttpClient(CIO) {
            install(JsonFeature) {
                serializer = GsonSerializer()
            }
        }

        runBlocking {
            val message = client.get<List<ControlMessage>> {
                url(managerUrl)
                // contentType(ContentType.Application.Json)
                // body = listOf<ControlMessage>()
            }

            for(command in message) {
                collector.emit(createControlOutput(System.currentTimeMillis(), command.instruction))
            }
            client.close()
        }

        Utils.sleep(1000)
    }

    override fun open(conf: MutableMap<String, Any>?, context: TopologyContext?, collector: SpoutOutputCollector) {
        this.collector = collector
        this.jsonParser = JSONParser()
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declare(CONTROL_SCHEMA)
    }
}
