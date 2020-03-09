package de.unikl.dbis.clash.flexstorm.control

import com.google.gson.JsonObject
import de.unikl.dbis.clash.flexstorm.CONTROL_SCHEMA
import de.unikl.dbis.clash.flexstorm.ClashState
import de.unikl.dbis.clash.flexstorm.createControlOutput
import de.unikl.dbis.clash.flexstorm.getControlOutput
import de.unikl.dbis.clash.flexstorm.getMessageOutput
import de.unikl.dbis.clash.manager.api.COMMAND_GATHER_STATISTICS
import de.unikl.dbis.clash.manager.api.MANAGER_ANSWER_PATH
import de.unikl.dbis.clash.manager.api.StatisticsMessage
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.request.post
import io.ktor.client.request.url
import kotlinx.coroutines.runBlocking
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.Tuple
import org.slf4j.LoggerFactory

const val CONTROL_BOLT_NAME = "control-bolt"

class ControlBolt(val managerUrl: String, val clashState: ClashState) : BaseRichBolt() {
    lateinit var context: TopologyContext
    lateinit var collector: OutputCollector
    var statisticsGatherer: StatisticsGatherer? = null

    override fun prepare(topoConf: MutableMap<String, Any>, context: TopologyContext, collector: OutputCollector) {
        this.context = context
        this.collector = collector

        clashState.setup(context)
    }

    override fun execute(input: Tuple) {
        LOG.debug("$CONTROL_BOLT_NAME receives data from ${input.sourceStreamId}")
        when (input.sourceStreamId) {
            FORWARD_TO_CONTROL_BOLT_STREAM_NAME -> justSend(getMessageOutput(input))
            CONTROL_SPOUT_TO_ALL_STREAM_NAME -> executeControlToAll(getControlOutput(input))
            CONTROL_SPOUT_TO_CONTROL_BOLT_STREAM_NAME -> executeControlToControl(getControlOutput(input))
            TICK_SPOUT_TO_CONTROL_BOLT_STREAM_NAME -> executeTick(input.getLong(0))
            FLEX_BOLT_TO_CONTROL_BOLT_STREAM_NAME -> executeFlexToControl(input.sourceTask, getMessageOutput(input))
        }
    }

    private fun executeFlexToControl(taskId: Int, message: Any) {
        LOG.debug("$CONTROL_BOLT_NAME handles as Flex To Control Message")
        when (message) {
            is InternalStatisticsMessage -> receiveGathered(taskId, message)
        }
    }

    private fun executeTick(long: Long) {
        startGathering()
    }

    private fun executeControlToControl(controlOutput: Triple<Long, String, JsonObject>) {
        TODO("not implemented") // To change body of created functions use File | Settings | File Templates.
    }

    private fun executeControlToAll(controlOutput: Triple<Long, String, JsonObject>) {
        TODO("not implemented") // To change body of created functions use File | Settings | File Templates.
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

    private fun startGathering() {
        val currentGatherer = statisticsGatherer
        if (currentGatherer != null) {
            println("Warning: previous StatisticsGatherer was not null and still waiting for ${currentGatherer.howManyLeft()} messages.")
        }

        statisticsGatherer = StatisticsGatherer(clashState.numberOfFlexBolts)
        collector.emit(CONTROL_BOLT_TO_ALL_FLEX_BOLTS_STREAM_NAME, createControlOutput(
            System.currentTimeMillis(),
            COMMAND_GATHER_STATISTICS,
            JsonObject()
        ))
    }

    private fun receiveGathered(taskId: Int, message: InternalStatisticsMessage) {
        LOG.debug("$CONTROL_BOLT_NAME receives Internal Statistics Message")
        statisticsGatherer?.add(taskId, message)
        if (statisticsGatherer?.finished() == true) {
            val result = statisticsGatherer?.get()!!
            val jsonResult = createStatisticsMessage(result)
            justSend(StatisticsMessage(JsonObject(), jsonResult))
            statisticsGatherer = null
        }
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declareStream(CONTROL_BOLT_TO_ALL_FLEX_BOLTS_STREAM_NAME, CONTROL_SCHEMA)
        declarer.declareStream(CONTROL_BOLT_TO_FLEX_BOLT_STREAM_NAME, CONTROL_SCHEMA)
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(ControlBolt::class.java)
    }
}
