package de.unikl.dbis.clash.workers.stores

import de.unikl.dbis.clash.ClashConfig
import de.unikl.dbis.clash.documents.Document
import de.unikl.dbis.clash.physical.BinaryPredicateEvaluation
import de.unikl.dbis.clash.physical.BinaryPredicateEvaluationLeftStored
import de.unikl.dbis.clash.physical.BinaryPredicateEvaluationRightStored
import java.io.Serializable
import java.util.SortedMap
import java.util.TreeMap
import org.slf4j.LoggerFactory

// TODO T was edeg label
data class LaterProbe<T>(
    private val timestamp: Long,
    val creationTime: Long,
    val documents: List<Document>,
    val predicates: Collection<BinaryPredicateEvaluation>,
    val resultTargets: Collection<T>
)

interface ProbeLogI<T> {
    fun restrict()

    fun put(
        seq: Long,
        creationTime: Long,
        documents: List<Document>,
        predicates: Collection<BinaryPredicateEvaluation>,
        resultTargets: Collection<T>
    )

    fun size(): Int

    fun examine(documents: List<Document>, seq: Long): DelayedStoreJoinResult<T>
}

// TODO T was edeg label
class ProbeLog<T>(config: ClashConfig) : ProbeLogI<T>, Serializable {
    val probeLogMaxKeys = config.probeLogMaxKeys
    private val buffer = TreeMap<Long, LaterProbe<T>>()

    fun clearUpto(seq: Long): Int {
        val headMap = this.buffer.headMap(seq, true)
        val result = headMap.size
        headMap.clear()
        return result
    }

    override fun restrict() {
        val size = this.buffer.size
        for (i in 1..(size - probeLogMaxKeys)) {
            this.buffer.pollFirstEntry()
        }
    }

    private fun subBuffer(fromSeq: Long, toSeq: Long): SortedMap<Long, LaterProbe<T>> {
        return this.buffer.subMap(fromSeq, toSeq)
    }

    override fun put(
        seq: Long,
        creationTime: Long,
        documents: List<Document>,
        predicates: Collection<BinaryPredicateEvaluation>,
        resultTargets: Collection<T>
    ) {
        val laterProbe = LaterProbe(seq, creationTime, documents, predicates, resultTargets)
        this.buffer[seq] = laterProbe
        if (probeLogMaxKeys > 0)
            restrict()
        // this.clearUpto(seq - 1000) // TODO this is a hack before: 1000
    }

    override fun size(): Int {
        return this.buffer.size
    }

    /**
     * When a store message arrives, this method is called and each missing
     * join is executed.
     */
    override fun examine(documents: List<Document>, seq: Long): DelayedStoreJoinResult<T> {
        val result = DelayedStoreJoinResult<T>(seq)

        for (laterProbe in this.subBuffer(seq, java.lang.Long.MAX_VALUE).values) {
            val joinResult = join(documents,
                    laterProbe.documents,
                    laterProbe.predicates)
            if (joinResult.isEmpty()) {
                continue
            }
            result.add(laterProbe.creationTime, joinResult, laterProbe.resultTargets)
        }
        return result
    }

    fun join(
        probed: Collection<Document>,
        stored: Collection<Document>,
        predicates: Collection<BinaryPredicateEvaluation>
    ): List<Document> {
        val result = ArrayList<Document>()

        for (probedDocument in probed) {
            for (storedDocument in stored) {
                var isJoinable = true
                for (predicateEvaluation in predicates) {
                    val (left, right) = when (predicateEvaluation) {
                        is BinaryPredicateEvaluationLeftStored -> Pair(storedDocument, probedDocument)
                        is BinaryPredicateEvaluationRightStored -> Pair(probedDocument, storedDocument)
                        else -> throw RuntimeException("Predicate Evaluation was not of type BinaryPredicateEvaluation!")
                    }
                    val predicate = predicateEvaluation.predicate
                    if (!predicate.joinable(left, right)) {
                        isJoinable = false
                        break
                    }
                }
                if (isJoinable) {
                    result.add(probedDocument.createJoint(storedDocument))
                }
            }
        }
        LOG.debug("joinPredicateEvaluation {} probed -- {} stored <<< {} >>> {}", probed, stored, predicates, result.size)

        return result
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(ProbeLog::class.java)
    }
}

class DelayedStoreJoinResult<T>(val seq: Long) {
    private val _inner = mutableListOf<InnerDelayedStoreJoinResult>()

    val inner: List<InnerDelayedStoreJoinResult>
        get() = this._inner

    fun add(
        creationTime: Long,
        joinResult: List<Document>,
        resultTargets: Collection<T>
    ) {
        this._inner.add(InnerDelayedStoreJoinResult(creationTime, joinResult, resultTargets))
    }

    fun size(): Int {
        return this._inner.size
    }

    fun forEach(action: (InnerDelayedStoreJoinResult) -> Unit) {
        this._inner.forEach(action)
    }

    inner class InnerDelayedStoreJoinResult internal constructor(
        var creationTime: Long,
        var joinResult: List<Document>,
        var resultTargets: Collection<T>
    )
}

private fun <T> emptyDelayedStoreJoinResult(): DelayedStoreJoinResult<T> = DelayedStoreJoinResult(-1)

class DisabledProbeLog<T> : ProbeLogI<T> {
    override fun restrict() = Unit

    override fun put(seq: Long, creationTime: Long, documents: List<Document>, predicates: Collection<BinaryPredicateEvaluation>, resultTargets: Collection<T>) = Unit

    override fun size() = 0

    override fun examine(documents: List<Document>, seq: Long): DelayedStoreJoinResult<T> = emptyDelayedStoreJoinResult<T>()
}
