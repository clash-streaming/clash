package de.unikl.dbis.clash.flexstorm

import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.Tuple
import org.json.simple.JSONObject

const val FLEX_BOLT_NAME = "boltyMcBoltface"

class FlexBolt() : BaseRichBolt() {
    lateinit var context: TopologyContext
    lateinit var collector: OutputCollector

    val clashState = ClashState()
    val container = StoreContainer()

    override fun prepare(topoConf: MutableMap<String, Any>, context: TopologyContext, collector: OutputCollector) {
        this.context = context
        this.collector = collector

        container.newEpoch(0, mapOf(
            "R" to listOf("b"),
            "S" to listOf("c", "d"),
            "T" to listOf("e")
        ))
    }

    override fun execute(input: Tuple) {
        when(getMessageType(input)) {
            MessageType.SpoutOutput -> executeDispatch(input)
            MessageType.Store -> executeStore(input)
            MessageType.Probe -> executeProbe(input)
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
        for(probeTarget in probeTargets) {
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

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declareStream(STORE_STREAM_ID, true, STORE_SCHEMA)
        declarer.declareStream(PROBE_STREAM_ID, true, PROBE_SCHEMA)
    }
}

fun actualFlexBoltTaskId(flexBoltId: Int, context: TopologyContext)
    = context.getComponentTasks(FLEX_BOLT_NAME)!![flexBoltId]

fun join(probeObject: JSONObject, partners: List<JSONObject>): List<JSONObject> {
    return partners.map { jointObject(it, probeObject) }
}

fun jointObject(objectA: JSONObject, objectB: JSONObject): JSONObject {
    val result = JSONObject()
    for((key, value) in objectA) {
        result[key] = value
    }
    for((key, value) in objectB) {
        result[key] = value
    }
    return result
}