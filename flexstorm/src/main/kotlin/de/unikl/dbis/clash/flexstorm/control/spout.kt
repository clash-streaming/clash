package de.unikl.dbis.clash.flexstorm.control

import com.google.gson.JsonObject
import de.unikl.dbis.clash.flexstorm.CONTROL_SCHEMA
import de.unikl.dbis.clash.flexstorm.MESSAGE_SCHEMA
import de.unikl.dbis.clash.flexstorm.createControlOutput
import de.unikl.dbis.clash.flexstorm.createMessageOutput
import de.unikl.dbis.clash.manager.api.COMMAND_FIELD
import de.unikl.dbis.clash.manager.api.COMMAND_PING
import de.unikl.dbis.clash.manager.api.COMMAND_RESET
import de.unikl.dbis.clash.manager.api.MANAGER_COMMAND_QUEUE_PATH
import de.unikl.dbis.clash.manager.api.TopologyAliveMessage
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.features.json.GsonSerializer
import io.ktor.client.features.json.JsonFeature
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.url
import io.ktor.util.KtorExperimentalAPI
import kotlinx.coroutines.runBlocking
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import org.json.simple.parser.JSONParser
import org.apache.storm.utils.Utils
import org.json.simple.JSONObject

const val CONTROL_SPOUT_NAME = "control-spout"

// data class ControlMessage(val instruction: String)

class ControlSpout(val managerUrl: String) : BaseRichSpout() {
    private lateinit var collector: SpoutOutputCollector
    lateinit var jsonParser: JSONParser

    private var topologyAliveMessageSent = false

    @KtorExperimentalAPI
    override fun nextTuple() {
        if(!topologyAliveMessageSent) {
            sendTopologyAliveMessage()
            topologyAliveMessageSent = true
        }

        val client = HttpClient(CIO) {
            install(JsonFeature) {
                serializer = GsonSerializer()
            }
        }

        runBlocking {
            val message = client.post<List<JsonObject>> {
                url(managerUrl + MANAGER_COMMAND_QUEUE_PATH)
                // contentType(ContentType.Application.Json)
                // body = listOf<JSONObject>()
            }

            for(command in message) {
                when(command[COMMAND_FIELD].asString) {
                    COMMAND_PING, COMMAND_RESET -> send(CONTROL_SPOUT_TO_ALL_STREAM_NAME, command)
                }
            }
            client.close()
        }

        Utils.sleep(1000)
    }

    private fun sendTopologyAliveMessage() {
        collector.emit(
            FORWARD_TO_CONTROL_BOLT_STREAM_NAME,
            createMessageOutput(TopologyAliveMessage())
        )
    }

    private fun send(targetStreamId: String, command: JsonObject) {
        collector.emit(
            targetStreamId,
            createControlOutput(
                System.currentTimeMillis(),
                command["command"].asString,
                command
            )
        )
    }

    override fun open(conf: MutableMap<String, Any>?, context: TopologyContext?, collector: SpoutOutputCollector) {
        this.collector = collector
        this.jsonParser = JSONParser()
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declareStream(CONTROL_SPOUT_TO_ALL_STREAM_NAME, CONTROL_SCHEMA)
        declarer.declareStream(CONTROL_SPOUT_TO_FLEX_BOLT_STREAM_NAME, CONTROL_SCHEMA)
        declarer.declareStream(CONTROL_SPOUT_TO_CONTROL_BOLT_STREAM_NAME, CONTROL_SCHEMA)
        declarer.declareStream(FORWARD_TO_CONTROL_BOLT_STREAM_NAME, MESSAGE_SCHEMA)
    }
}
