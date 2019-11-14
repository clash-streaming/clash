package de.unikl.dbis.clash.storm.bolts

import com.codahale.metrics.Counter
import com.codahale.metrics.Timer
import de.unikl.dbis.clash.physical.Rule
import de.unikl.dbis.clash.storm.*
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.Tuple
import org.slf4j.LoggerFactory
import java.io.Serializable
import java.util.HashMap
import java.util.HashSet


abstract class AbstractBolt(open val name: String) : BaseRichBolt(), CommonSinkI {
    val ruleSet = SiRuleSet()

    lateinit var stormConfig: Map<*, *>
    lateinit var topologyContext: TopologyContext
    lateinit var outputCollector: EdgyOutputCollector

    internal var stormComponentName: String = ""

    override fun prepare(conf: MutableMap<String, Any>?, topologyContext: TopologyContext?, outputCollector: OutputCollector?) {
        this.stormConfig = conf!!
        this.outputCollector = EdgyOutputCollector(outputCollector!!)
        this.topologyContext = topologyContext!!
        this.stormComponentName = topologyContext.thisComponentId + "_" + topologyContext.thisTaskId
    }

    override fun declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer) {
        this.ruleSet.outRules()
                .forEach { rule ->
                    outputFieldsDeclarer.declareStream(rule.outgoingEdgeLabel.streamName,
                            rule.messageVariant.schema)
                }
    }

    /**
     * During topology construction, a bolt gets rules assigned. This method takes care of assigning
     * them the in and out rules.
     *
     * @param rule the rule that is added
     */
    override fun addRule(rule: Rule) {
        LOG.info("Registering rule to bolt $this: $rule")
        this.ruleSet.add(rule)
    }

    /**
     * This method delegates the incoming tuple based on the ruleSet. If the rule that applies to the
     * message is a control rule, the message is handled by the executeControl method, a data path
     * rule is handled by executeDataPath, and a reporting rule is handled by executeReporting.
     */
    override fun execute(tuple: Tuple) {
        LOG.debug("{} handles tuple {}", this.name, tuple)
        val start = System.currentTimeMillis()
        val variant = MessageVariant.getVariantFor(tuple, this.ruleSet)
        val inRule = this.ruleSet.inRuleFor(tuple.sourceStreamId)
        when (variant) {
            MessageVariant.Control -> executeControl(ControlMessage.fromTuple(tuple), inRule)
            MessageVariant.DataPath -> executeDataPath(DataPathMessage.fromTuple(tuple), inRule)
            MessageVariant.Tick -> executeTick(TickMessage.fromTuple(tuple), inRule)
        }
        val end = System.currentTimeMillis()
    }

    fun executeTick(fromTuple: TickMessage, inRule: StormInRule) {
        if(fromTuple.ats % 10 == 0L) {

        }
    }

    fun executeControl(message: ControlMessage, stormInRule: StormInRule) {
        when (message.instruction) {
            ControlMessage.Instruction.RESET_STATE -> resetState()
        }
    }

    fun executeDataPath(message: DataPathMessage, stormInRule: StormInRule) {
        when (message) {
            is DocumentsMessage -> this.executeDocuments(message, stormInRule)
            is FinishedMessage -> this.executeFinished(message, stormInRule)
            is PunctuationMessage -> this.executePunctuation(message, stormInRule)
            else -> throw RuntimeException("Cannot handle DataPathMessage $message")
        }
    }



    open fun executeDocuments(message: DocumentsMessage,
                              stormInRule: StormInRule) {
    }

    fun executeFinished(message: FinishedMessage, stormInRule: StormInRule) {}

    fun executePunctuation(message: PunctuationMessage,
                           stormInRule: StormInRule) {
    }

    open fun resetState() { }

    companion object {
        private val LOG = LoggerFactory.getLogger(AbstractBolt::class.java)
    }
}


/**
 * The DispatchBolt collects tuples from the spouts. The incoming tuples are assumed to have exactly
 * one field.
 */
class DispatchBolt(name: String) : AbstractBolt(name), IDispatcherStats by DispatcherStats() {
    private var punctuationCounter = 0
    private var seq = 0

    override fun prepare(conf: MutableMap<String, Any>?, topologyContext: TopologyContext?, outputCollector: OutputCollector?) {
        super.prepare(conf, topologyContext, outputCollector)
        registerMetrics(topologyContext!!)
    }

    override fun executeDocuments(
            message: DocumentsMessage,
            stormInRule: StormInRule) {
        val timerContext = dispatchingTimer.time()

        val documents = message.documents

        if (stormInRule !is StormRelationReceiveRule) {
            return
        }

        this.ruleSet.outRules()
                .forEach { outRule ->
                    val sendRule = outRule as StormRelationSendRule
                    if (sendRule.relation == stormInRule.relation) {
                        this.outputCollector.emit(sendRule.outgoingEdgeLabel,
                                DocumentsMessage(seq.toLong(), System.currentTimeMillis(), documents))
                        LOG.debug(
                                "Sending message of relation '{}' to {}", sendRule.relation, sendRule.outgoingEdgeLabel)
                    }
                }
        seq += 1
        punctuationCounter += 1
        // TODO
        /*if(punctuationCounter > PUNCTUATION_EVERY) {
        punctuationCounter = 0;
        for (Rule rule : this.sendRules) {
            PunctuationMessage pMessage = new PunctuationMessage(seq, 0);
            this.collector.emit(rule.getOutgoingStreamName(), pMessage.asValues());
        }
    }*/
        timerContext.stop()
        tuplesDispatchedCounter.inc()

    }

    override fun resetState() {
        LOG.info("Reset counters of " + this.name)
        this.punctuationCounter = 0
        this.seq = 0
    }

    companion object {

        private val LOG = LoggerFactory.getLogger(DispatchBolt::class.java)

        /**
         * Send every PUNCTUATION_EVERY tuples a punctuation message through the network
         */
        private val PUNCTUATION_EVERY = 1000
    }
}

interface IDispatcherStats : Serializable {
    fun registerMetrics(topologyContext: TopologyContext)

    val tuplesDispatchedCounter: Counter
    var dispatchingTimer: Timer
}

class DispatcherStats: IDispatcherStats {
    override lateinit var tuplesDispatchedCounter: Counter

    override lateinit var dispatchingTimer: Timer

    override fun registerMetrics(topologyContext: TopologyContext) {
        tuplesDispatchedCounter = topologyContext.registerCounter("clash_metric.tuplesDispatchedCounter")
        dispatchingTimer = topologyContext.registerTimer("clash_metric.dispatchingTimer")
    }
}


class FinishedJoiner : Serializable {
    private val waitingFor = mutableMapOf<String, MutableSet<Int>>()

    val isFinished: Boolean
        get() = this.waitingFor.isEmpty()

    private fun waitFor(component: String, taskId: Int) {
        this.waitingFor.putIfAbsent(component, HashSet())
        this.waitingFor[component]!!.add(taskId)
    }

    fun received(component: String, taskId: Int) {
        if (!this.waitingFor.containsKey(component)) {
            return
        }
        this.waitingFor[component]!!.remove(taskId)
        if (this.waitingFor[component]!!.isEmpty()) {
            this.waitingFor.remove(component)
        }
    }

    fun waitForAll(sender: String, componentTasks: List<Int>) {
        for (i in componentTasks) {
            waitFor(sender, i)
        }
    }

    companion object {

        private val counter: Int = 0
    }
}

internal class SequenceTable : Serializable {

    private val inner = mutableMapOf<String, MutableMap<Int, Long>>()


    fun update(sourceComponent: String, sourceTask: Int, seq: Long) {
        val value = this.inner[sourceComponent]!![sourceTask]!!
        if (seq > value) {
            this.inner[sourceComponent]!![sourceTask] = seq
        } else {
            throw RuntimeException("Received tuple out of order :(((")
        }
    }

    fun put(sender: String, taskId: Int) {
        inner.putIfAbsent(sender, HashMap())
        inner[sender]!![taskId] = -1L
    }
}


interface IStormStats : Serializable {
    fun registerMetrics(topologyContext: TopologyContext)

    val probeTuplesReadCounter: Counter
    val probeMessagesReadCounter: Counter
    val storeTuplesReadCounter: Counter
    val storeMessagesReadCounter: Counter
}

class StormStats: IStormStats {
    override lateinit var probeTuplesReadCounter: Counter
    override lateinit var probeMessagesReadCounter: Counter
    override lateinit var storeTuplesReadCounter: Counter
    override lateinit var storeMessagesReadCounter: Counter

    override fun registerMetrics(topologyContext: TopologyContext) {
        probeTuplesReadCounter = topologyContext.registerCounter("clash_metric.probeTuplesRead")
        probeMessagesReadCounter = topologyContext.registerCounter("clash_metric.probeMessagesRead")
        storeTuplesReadCounter = topologyContext.registerCounter("clash_metric.storeTuplesRead")
        storeMessagesReadCounter = topologyContext.registerCounter("clash_metric.storeMessagesRead")
    }
}