package de.unikl.dbis.clash.workers.joiner

import de.unikl.dbis.clash.ClashConfig
import de.unikl.dbis.clash.documents.Document
import de.unikl.dbis.clash.physical.BinaryPredicateEvaluation
import de.unikl.dbis.clash.workers.stores.DelayedStoreJoinResult
import de.unikl.dbis.clash.workers.stores.ProbeLog
import java.io.Serializable

// TODO T was StormEdgeLabel
open class WindowedJoiner<T>(config: ClashConfig) : Serializable {
    protected var probeBuffer = ProbeLog<T>(config=config)
    protected var storeBuffer = WindowedStoreBuffer()

    fun probe(
            input: List<Document>,
            sequenceNumber: Long,
            creationTime: Long,
            predicates: Collection<BinaryPredicateEvaluation>,
            targets: Collection<T>): List<Document> {
        this.probeBuffer.put(sequenceNumber, creationTime, input, predicates, targets)
        val result = this.storeBuffer.join(input, sequenceNumber, predicates)
        return result
    }

    fun store(
            input: List<Document>,
            sequenceNumber: Long
    ): DelayedStoreJoinResult<T> {
        this.storeBuffer.put(sequenceNumber, input)
        val result = this.probeBuffer.examine(input, sequenceNumber)
        return result
    }

    fun storedTuples(): Long {
        return this.storeBuffer.size().toLong()
    }
}