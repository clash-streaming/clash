package de.unikl.dbis.clash.flexstorm.control

import de.unikl.dbis.clash.flexstorm.MESSAGE_SCHEMA
import de.unikl.dbis.clash.flexstorm.getMessageOutput
import de.unikl.dbis.clash.manager.api.MANAGER_ANSWER_PATH
import de.unikl.dbis.clash.manager.api.TopologyAliveMessage
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.features.json.GsonSerializer
import io.ktor.client.features.json.JsonFeature
import io.ktor.client.request.post
import io.ktor.client.request.url
import io.ktor.http.ContentType
import io.ktor.http.contentType
import io.ktor.util.KtorExperimentalAPI
import kotlinx.coroutines.runBlocking
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.Tuple

const val CONTROL_BOLT_NAME = "control-bolt"

class ControlBolt(val managerUrl: String): BaseRichBolt() {
    lateinit var context: TopologyContext
    lateinit var collector: OutputCollector

    override fun prepare(topoConf: MutableMap<String, Any>, context: TopologyContext, collector: OutputCollector) {
        this.context = context
        this.collector = collector
    }

    @KtorExperimentalAPI
    override fun execute(input: Tuple) {
        if(input.sourceStreamId == FORWARD_TO_CONTROL_BOLT_STREAM_NAME) {
            justSend(getMessageOutput(input))
            return
        }

        val client = HttpClient(CIO) {
            install(JsonFeature) {
                serializer = GsonSerializer()
            }
        }

        runBlocking {
            client.post<Unit> {
                url(managerUrl + MANAGER_ANSWER_PATH)
                contentType(ContentType.Application.Json)
                body = TopologyAliveMessage().toJson()
            }
            client.close()
        }
    }

    private fun justSend(messageOutput: Any) {
        val client = HttpClient(CIO)
        runBlocking {
            client.post<Unit> {
                url(managerUrl + MANAGER_ANSWER_PATH)
                body = messageOutput.toString()
            }
            client.close()
        }
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        // nothing
    }
}