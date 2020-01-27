package de.unikl.dbis.clash.flexstorm

import com.google.gson.JsonObject
import de.unikl.dbis.clash.flexstorm.control.FORWARD_TO_CONTROL_BOLT_STREAM_NAME
import de.unikl.dbis.clash.manager.api.COMMAND_START_ACCEPTING_TUPLES
import de.unikl.dbis.clash.manager.api.COMMAND_STOP_ACCEPTING_TUPLES
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.Tuple

const val FLEX_BOLT_NAME = "boltyMcBoltface"

class FlexBolt(val clashState: ClashState) : BaseRichBolt() {
    lateinit var context: TopologyContext
    lateinit var collector: OutputCollector

    val container = StoreContainer()
    var acceptingTuples = false

    override fun prepare(topoConf: MutableMap<String, Any>, context: TopologyContext, collector: OutputCollector) {
        this.context = context
        this.collector = collector

        container.newEpoch(0, mapOf(
            "R" to listOf("b"),
            "S" to listOf("c", "d"),
            "T" to listOf("e")
        ))

        clashState.setup(context)
    }

    override fun execute(input: Tuple) {
        if (!acceptingTuples) { return }
        when (getMessageType(input)) {
            MessageType.SpoutOutput -> executeDispatch(input)
            MessageType.Store -> executeStore(input)
            MessageType.Probe -> executeProbe(input)
            MessageType.Control -> executeControl(input)
            MessageType.Message -> TODO()
        }
    }

    fun executeDispatch(input: Tuple) {
        val (timestamp, payload, relation) = getSpoutOutput(input)

        // send for storing
        val storeTarget = clashState.getStoreTarget(timestamp, payload, relation)
        val storeTargetTaskId = actualFlexBoltTaskId(storeTarget, context)
        collector.emitDirect(storeTargetTaskId, STORE_STREAM_ID, createStoreOutput(timestamp, payload, relation))

        // send for probing
        val probeTargets = clashState.getProbeOrderTarget(timestamp, payload, relation, 0)
        for (probeTarget in probeTargets) {
            val probeTargetTaskId = actualFlexBoltTaskId(probeTarget, context)
            collector.emitDirect(probeTargetTaskId, PROBE_STREAM_ID, createProbeOutput(timestamp, payload, relation, 0))
        }
    }

    fun executeStore(input: Tuple) {
        val (timestamp, payload, relation) = getStoreOutput(input)
        val store = container.getStore(timestamp, relation)
        store.store(payload)
    }

    fun executeProbe(input: Tuple) {
        val (timestamp, payload, relation, offset) = getProbeOutput(input)

        val epoch = clashState.epochs.epochFor(timestamp)!!
        val probeOrder = epoch.probeOrders[relation]!!
        val entry = probeOrder.entries[offset]
        val store = container.getStore(timestamp, entry.targetStore)
        val probeResult = store.probe(entry.probingAttribute, payload[entry.sendingAttribute]!!)
        if(probeResult.isEmpty()) {
            return
        }

        val joinResult = join(payload, probeResult)

        if(offset == 1) {
            println("Final join result found at ${context.thisTaskId}: $joinResult")
            return
        }

        println("Joining got this probe result: $probeResult")

        for(tuple in joinResult) {
            val probeTargets = clashState.getProbeOrderTarget(timestamp, tuple, relation, offset+1)
            for(probeTarget in probeTargets) {
                val probeTargetTaskId = actualFlexBoltTaskId(probeTarget, context)
                collector.emitDirect(probeTargetTaskId, PROBE_STREAM_ID, createProbeOutput(timestamp, tuple, relation, offset+1))
            }
            println(">>> PROBE")
        }
    }

    fun executeControl(input: Tuple) {
        val (timestamp, value, payload) = getControlOutput(input)
        println("$timestamp: received control message: $value")

        when (value) {
            COMMAND_STOP_ACCEPTING_TUPLES -> this.acceptingTuples = false
            COMMAND_START_ACCEPTING_TUPLES -> this.acceptingTuples = true
        }
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declareStream(STORE_STREAM_ID, true, STORE_SCHEMA)
        declarer.declareStream(PROBE_STREAM_ID, true, PROBE_SCHEMA)
        declarer.declareStream(FORWARD_TO_CONTROL_BOLT_STREAM_NAME, MESSAGE_SCHEMA)
    }
}

fun actualFlexBoltTaskId(flexBoltId: Int, context: TopologyContext)
    = context.getComponentTasks(FLEX_BOLT_NAME)!![flexBoltId]

fun join(probeObject: JsonObject, partners: List<JsonObject>): List<JsonObject> {
    return partners.map { jointObject(it, probeObject) }
}

fun jointObject(objectA: JsonObject, objectB: JsonObject): JsonObject {
    val result = JsonObject()
    for ((key, value) in objectA.entrySet()) {
        result.add(key, value)
    }
    for ((key, value) in objectB.entrySet()) {
        result.add(key, value)
    }
    return result
}
