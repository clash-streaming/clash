package de.unikl.dbis.clash.storm.bolts

import com.codahale.metrics.Counter
import com.codahale.metrics.Timer
import de.unikl.dbis.clash.ClashConfig
import de.unikl.dbis.clash.physical.BinaryPredicateEvaluation
import de.unikl.dbis.clash.query.AttributeAccessList
import de.unikl.dbis.clash.storm.*
import de.unikl.dbis.clash.workers.joiner.WindowedJoiner
import de.unikl.dbis.clash.workers.stores.PartitionedStore
import de.unikl.dbis.clash.workers.stores.ThetaStore
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.slf4j.LoggerFactory
import java.io.Serializable


interface StoreI {
    fun store(message: DocumentsMessage)

    fun probe(
            message: DocumentsMessage,
            predicates: Set<BinaryPredicateEvaluation>,
            targets: Collection<StormEdgeLabel>)
}

abstract class GeneralStore(name: String) : AbstractBolt(name), StoreI, IStoreStats by StoreStats() {

    override fun prepare(conf: MutableMap<String, Any>?, topologyContext: TopologyContext?, outputCollector: OutputCollector?) {
        super.prepare(conf, topologyContext, outputCollector)
        registerMetrics(topologyContext!!)
    }

    override fun executeDocuments(message: DocumentsMessage,
                                  stormInRule: StormInRule) {
        when (stormInRule) {
            is StormRelationReceiveRule -> this.store(message)
            is StormIntermediateJoinRule -> {
                // the rule knows where to send the result
                val predicates = stormInRule.predicates
                val targets = setOf(stormInRule.outgoingEdgeLabel)
                this.probe(message, predicates, targets)
            }
            is StormJoinResultRule -> {
                // the registered ruleset knows where to send the result
                val predicates = stormInRule.predicates
                val targets = this.ruleSet
                        .relationSendRulesFor(stormInRule.relation)
                        .map { it.outgoingEdgeLabel }
                this.probe(message, predicates, targets)
            }
        }
    }

    override fun resetState() {
        LOG.info("Reset join state of " + this.name)
        super.resetState()
    }

    override fun toString(): String {
        return "GeneralStoreBolt for relation '" + this.name + "'"
    }

    companion object {
        val LOG = LoggerFactory.getLogger(PartitionedStoreBolt::class.java)!!
    }
}

class ThetaStoreBolt(name: String, val config: ClashConfig) : GeneralStore(name) {
    var thetaStore = ThetaStore<StormEdgeLabel>(config)

    override fun store(message: DocumentsMessage) {
        LOG.debug("Storing tuple $message")

        storeTuplesReadCounter.inc(message.documents.size.toLong())
        storeMessagesReadCounter.inc()
        val timerContext = storeTimer.time()

        if(config.probeLogEnabled) {
            val delayed = this.thetaStore.store(message.seq, message.documents)
            delayed.forEach {
                val res = DocumentsMessage(delayed.seq, it.creationTime, it.joinResult)
                for (resultTarget in it.resultTargets) {
                    outputCollector.emit(resultTarget, res)
                }
            }

            emittedDelayedResultTuplesCounter.inc(delayed.size().toLong())
            emittedDelayedResultMessagesCounter.inc()
        } else {
            thetaStore.store(message.seq, message.documents)
        }
        timerContext.stop()
    }

    /**
     * Test if the documents in this message can be joined with any of the messages in the stored
     * prefix and compute the concatenated documents. Send all the produced documents to all targets.
     */
    override fun probe(
            message: DocumentsMessage,
            predicates: Set<BinaryPredicateEvaluation>,
            targets: Collection<StormEdgeLabel>) {

        probeTuplesReadCounter.inc(message.documents.size.toLong())
        probeMessagesReadCounter.inc()
        val timerContext = probeTimer.time()

        val probeResult = this.thetaStore.probe(
                message.seq,
                message.timestamp,
                message.documents,
                predicates,
                targets)
        if (probeResult.isEmpty()) {
            return
        }

        val edgeLabels = targets
                .map  { it.toString() }
        LOG.debug("successfulJoinTo " + edgeLabels.joinToString(", "))

        val outputMessage = DocumentsMessage(message.seq,
                message.timestamp, probeResult)

        for (target in targets) {
            outputCollector.emit(target, outputMessage)
        }

        emittedResultTuplesCounter.inc(probeResult.size.toLong())
        emittedResultMessagesCounter.inc()
        timerContext.stop()
    }

    override fun resetState() {
        super.resetState()
        this.thetaStore = ThetaStore(config=config)
    }
}

class PartitionedStoreBolt(name: String, private val partitioningAttributes: AttributeAccessList, val config: ClashConfig) : GeneralStore(name) {
    var partitionedStore = PartitionedStore<StormEdgeLabel>(config)

    override fun store(message: DocumentsMessage) {
        LOG.debug("Storing tuple $message")
        storeTuplesReadCounter.inc(message.documents.size.toLong())
        storeMessagesReadCounter.inc()
        val timerContext = storeTimer.time()

        val delayed = partitionedStore.store(message.seq, message.documents)

        delayed.forEach {
            val res = DocumentsMessage(delayed.seq, it.creationTime, it.joinResult)
            for (resultTarget in it.resultTargets) {
                outputCollector.emit(resultTarget, res)
            }
        }

        emittedDelayedResultTuplesCounter.inc(delayed.size().toLong())
        emittedDelayedResultMessagesCounter.inc()
        timerContext.stop()
    }

    /**
     * Test if the documents in this message can be joined with any of the messages in the stored
     * prefix and compute the concatenated documents. Send all the produced documents to all targets.
     */
    override fun probe(
            message: DocumentsMessage,
            predicates: Set<BinaryPredicateEvaluation>,
            targets: Collection<StormEdgeLabel>) {

        probeTuplesReadCounter.inc(message.documents.size.toLong())
        probeMessagesReadCounter.inc()
        val timerContext = probeTimer.time()

        val probeResult = this.partitionedStore.probe(
                message.seq,
                message.timestamp,
                message.documents,
                predicates,
                targets)
        if (probeResult.isEmpty()) {
            return
        }

        val edgeLabels = targets
                .map  { it.toString() }
        LOG.debug("successfulJoinTo " + edgeLabels.joinToString(", "))

        val outputMessage = DocumentsMessage(message.seq,
                message.timestamp, probeResult)

        for (target in targets) {
            outputCollector.emit(target, outputMessage)
        }
        emittedResultTuplesCounter.inc(probeResult.size.toLong())
        emittedResultMessagesCounter.inc()
        timerContext.stop()
    }

    override fun resetState() {
        LOG.info("Reset join state of " + this.name)
        super.resetState()
        this.partitionedStore = PartitionedStore(config)
    }

    override fun toString(): String {
        return ("PartitionedSinkBolt for relation '" + this.name
                + "'[" + this.partitioningAttributes + "]")
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(PartitionedStoreBolt::class.java)!!
    }
}

class WindowedThetaStore(name: String, val config: ClashConfig) : GeneralStore(name) {
    var joiner = WindowedJoiner<StormEdgeLabel>(config)

    override fun store(message: DocumentsMessage) {
        LOG.debug("Storing tuple $message")

        storeTuplesReadCounter.inc(message.documents.size.toLong())
        storeMessagesReadCounter.inc()
        val timerContext = storeTimer.time()

        val delayed = this.joiner.store(message.documents, message.seq)

        delayed.forEach {
            val res = DocumentsMessage(delayed.seq, it.creationTime, it.joinResult)
            for (resultTarget in it.resultTargets) {
                outputCollector.emit(resultTarget, res)
            }
        }

        emittedDelayedResultTuplesCounter.inc(delayed.size().toLong())
        emittedDelayedResultMessagesCounter.inc()
        timerContext.stop()
    }

    /**
     * Test if the documents in this message can be joined with any of the messages in the stored
     * prefix and compute the concatenated documents. Send all the produced documents to all targets.
     */
    override fun probe(
            message: DocumentsMessage,
            predicates: Set<BinaryPredicateEvaluation>,
            targets: Collection<StormEdgeLabel>) {

        probeTuplesReadCounter.inc(message.documents.size.toLong())
        probeMessagesReadCounter.inc()
        val timerContext = probeTimer.time()

        val probeResult = this.joiner.probe(
                message.documents,
                message.seq,
                message.timestamp,
                predicates,
                targets)
        if (probeResult.isEmpty()) {
            return
        }

        val edgeLabels = targets
                .map  { it.toString() }
        LOG.debug("successfulJoinTo " + edgeLabels.joinToString(", "))

        val outputMessage = DocumentsMessage(message.seq,
                message.timestamp, probeResult)

        for (target in targets) {
            outputCollector.emit(target, outputMessage)
        }

        timerContext.stop()
        emittedResultTuplesCounter.inc(probeResult.size.toLong())
        emittedResultMessagesCounter.inc()
    }

    override fun resetState() {
        super.resetState()
        this.joiner = WindowedJoiner(config)
    }
}


interface IStoreStats : Serializable {
    fun registerMetrics(topologyContext: TopologyContext)

    val probeTuplesReadCounter: Counter
    val probeMessagesReadCounter: Counter
    val storeTuplesReadCounter: Counter
    val storeMessagesReadCounter: Counter
    var emittedResultTuplesCounter: Counter
    var emittedResultMessagesCounter: Counter
    var emittedDelayedResultTuplesCounter: Counter
    var emittedDelayedResultMessagesCounter: Counter

    var probeTimer: Timer
    var storeTimer: Timer
}

class StoreStats: IStoreStats {
    override lateinit var probeTuplesReadCounter: Counter
    override lateinit var probeMessagesReadCounter: Counter
    override lateinit var storeTuplesReadCounter: Counter
    override lateinit var storeMessagesReadCounter: Counter
    override lateinit var emittedResultTuplesCounter: Counter
    override lateinit var emittedResultMessagesCounter: Counter
    override lateinit var emittedDelayedResultTuplesCounter: Counter
    override lateinit var emittedDelayedResultMessagesCounter: Counter

    override lateinit var probeTimer: Timer
    override lateinit var storeTimer: Timer

    override fun registerMetrics(topologyContext: TopologyContext) {
        probeTuplesReadCounter = topologyContext.registerCounter("clash_metric.probeTuplesReadCounter")
        probeMessagesReadCounter = topologyContext.registerCounter("clash_metric.probeMessagesReadCounter")
        storeTuplesReadCounter = topologyContext.registerCounter("clash_metric.storeTuplesReadCounter")
        storeMessagesReadCounter = topologyContext.registerCounter("clash_metric.storeMessagesReadCounter")
        emittedResultTuplesCounter = topologyContext.registerCounter("clash_metric.emittedResultTuplesCounter")
        emittedResultMessagesCounter = topologyContext.registerCounter("clash_metric.emittedResultMessagesCounter")
        emittedDelayedResultTuplesCounter = topologyContext.registerCounter("clash_metric.emittedDelayedResultTuplesCounter")
        emittedDelayedResultMessagesCounter = topologyContext.registerCounter("clash_metric.emittedDelayedResultMessagesCounter")
        probeTimer = topologyContext.registerTimer("clash_metric.probeTimer")
        storeTimer = topologyContext.registerTimer("clash_metric.storeTimer")
    }
}