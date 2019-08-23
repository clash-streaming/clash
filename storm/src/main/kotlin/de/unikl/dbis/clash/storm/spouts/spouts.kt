package de.unikl.dbis.clash.storm.spouts

import de.unikl.dbis.clash.ClashConfig
import de.unikl.dbis.clash.documents.Document
import de.unikl.dbis.clash.physical.ControlOutRule
import de.unikl.dbis.clash.physical.OutRule
import de.unikl.dbis.clash.physical.RelationSendRule
import de.unikl.dbis.clash.physical.TickOutRule
import de.unikl.dbis.clash.storm.*
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichSpout
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.utils.Utils
import org.slf4j.LoggerFactory
import java.util.*


interface CommonSpoutI : IRichSpout {
    fun addRule(rule: OutRule)
}

abstract class CommonSpout constructor(millisDelay: Int = 0) : BaseRichSpout(), CommonSpoutI {
    val rules = ArrayList<StormOutRule>()
    var collector: EdgySpoutOutputCollector? = null
    var conf: Map<*, *>? = null

    var millisDelay: Int = 0
    var delayEvery: Int = 0
    var delayCounter: Int = 0

    init {
        if (millisDelay >= 0) {
            this.millisDelay = millisDelay
        } else if (millisDelay < 0) {
            this.millisDelay = 1
            this.delayEvery = millisDelay
            this.delayCounter = 0
        }
    }

    override fun declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer) {
        this.rules.stream()
                .forEach { rule ->
                    outputFieldsDeclarer.declareStream(rule.outgoingEdgeLabel.streamName,
                            rule.messageVariant.schema)
                }
    }

    override fun open(conf: MutableMap<String, Any>?, topologyContext: TopologyContext?, spoutOutputCollector: SpoutOutputCollector?) {
        this.collector = EdgySpoutOutputCollector(spoutOutputCollector!!)
        this.conf = conf
    }

    internal fun delayNext() {
        val sleepTime = this.millisDelay.toLong()
        if (this.millisDelay > 0) {
            Utils.sleep(sleepTime)
        }
        if (this.millisDelay < 0) {
            if (this.delayCounter++ > this.delayEvery) {
                Utils.sleep(sleepTime)
                this.delayCounter = 0
            }
        }

    }

    internal fun emit(message: ControlMessage) {
        LOG.debug("Emitting message.")
        for (rule in this.rules) {
            this.collector!!.emit(rule.outgoingEdgeLabel, message)
        }
    }

    internal fun emit(message: DataPathMessage) {
        LOG.debug("Emitting message.")
        for (rule in this.rules) {
            this.collector!!.emit(rule.outgoingEdgeLabel, message)
        }
    }

    // TODO this should not be needed (pleasing the kotlin compiler)
    internal fun emit(message: FinishedMessage) {
        for (rule in this.rules) {
            this.collector!!.emit(rule.outgoingEdgeLabel, message)
        }
    }

    // TODO this should not be needed (pleasing the kotlin compiler)
    internal fun emit(message: DocumentsMessage) {
        LOG.debug("Emitting documents message.")
        for (rule in this.rules) {
            this.collector!!.emit(rule.outgoingEdgeLabel, message)
        }
    }

    internal fun emit(message: TickMessage) {
        LOG.debug("Emitting tick message.")
        for (rule in this.rules) {
            this.collector!!.emit(rule.outgoingEdgeLabel, message)
        }
    }

    internal fun emit(vararg documents: Document) {
        val packedObject = ArrayList<Document>()
        Collections.addAll(packedObject, *documents)

        val message = DocumentsMessage(0, 0, packedObject)
        this.emit(message)
    }

    override fun addRule(rule: OutRule) {
        LOG.info("Registering rule to spout $this: $rule")
        when(rule) {
            is RelationSendRule -> this.rules.add(StormRelationSendRule(rule))
            is ControlOutRule -> this.rules.add(StormControlOutRule(rule))
            is TickOutRule -> this.rules.add(StormTickOutRule(rule))
            else -> LOG.warn("Rule $rule was not added to spout $this")
        }
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(JsonFileSpout::class.java)
    }
}

class TickSpout(val config: ClashConfig) : CommonSpout() {
    var counter = 0L

    override fun nextTuple() {
        Utils.sleep(config.tickRate.toLong())
        val currentTs = System.currentTimeMillis()
        val tickMessage = TickMessage(counter++, currentTs)
        super.emit(tickMessage)
    }
}
