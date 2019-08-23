package de.unikl.dbis.clash.workers.stores

import de.unikl.dbis.clash.ClashConfig
import de.unikl.dbis.clash.documents.Document
import de.unikl.dbis.clash.physical.BinaryPredicateEvaluation
import de.unikl.dbis.clash.workers.joiner.Join
import java.io.Serializable
import java.util.*

// TODO T was StormEdgeLabel
class PartitionedStore<T>(config: ClashConfig): Serializable {
    val partitionedPrefix = PartitionedPrefix()
    val probeLog = ProbeLog<T>(config)

    fun store(seq: Long, documents: List<Document>): DelayedStoreJoinResult<T> {
        partitionedPrefix.put(seq, documents)
        return probeLog.examine(documents, seq)
    }

    fun probe(seq: Long,
              ts: Long,
              documents: List<Document>,
              predicates: Collection<BinaryPredicateEvaluation>,
              targets: Collection<T>): List<Document> {
        probeLog.put(seq, ts, documents, predicates, targets)
        return partitionedPrefix.get(java.lang.Long.MIN_VALUE, seq)
                .values
                .map { Join.join(documents, it, predicates )}
                .flatten()

    }
}

/**
 * The RandomPrefix is responsible for storing the prefix of a relation.
 *
 * Each stored document (list) is stored associated with their timestamp.
 * The
 */
data class PartitionedPrefix(
        // These are the stored objects ordered by their timestamp attribute
        //  t ----------------------------------------------
        //       t1 [{k:v,k:v,...}]        t5 [{k:v,...}, {k:v,...}]
        val inner: TreeMap<Long, List<Document>> = TreeMap()
): Serializable {

    fun put(seq: Long, documents: List<Document>) {
        this.inner[seq] = documents
    }

    fun get(fromSeq: Long, toSeq: Long): SortedMap<Long, List<Document>> {
        return inner.subMap(fromSeq, toSeq) as SortedMap<Long, List<Document>>
    }
}
