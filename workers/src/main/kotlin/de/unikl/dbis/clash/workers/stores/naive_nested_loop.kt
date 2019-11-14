package de.unikl.dbis.clash.workers.stores

import de.unikl.dbis.clash.ClashConfig
import de.unikl.dbis.clash.documents.Document
import de.unikl.dbis.clash.physical.BinaryPredicateEvaluation
import de.unikl.dbis.clash.physical.BinaryPredicateEvaluationLeftStored
import de.unikl.dbis.clash.physical.BinaryPredicateEvaluationRightStored
import de.unikl.dbis.clash.query.AttributeAccess
import org.slf4j.LoggerFactory
import java.io.Serializable
import java.util.*

class NaiveNestedLoopStore<T>(val config: ClashConfig): ActualStore<T>, Serializable {
    val randomPrefix = RandomPrefix()
    val probeLog = ProbeLog<T>(config)

    override fun store(ats: Long, documents: List<Document>): DelayedStoreJoinResult<T> {
        randomPrefix.put(ats, documents)
        return probeLog.examine(documents, ats)
    }

    override fun probe(ats: Long, lts: Long, documents: List<Document>, predicates: Collection<BinaryPredicateEvaluation>, targets: Collection<T>): List<Document>
            = probeFrom(ats, lts, documents, java.lang.Long.MIN_VALUE, predicates, targets)


    override fun probe(ats: Long,  lts: Long,  documents: List<Document>,  windowSize: Long,  predicates: Collection<BinaryPredicateEvaluation>,  targets: Collection<T>): List<Document>
            = probeFrom(ats, lts, documents,ats - windowSize, predicates, targets)

    override fun probe(ats: Long, lts: Long, documents: List<Document>, attributeAccess: AttributeAccess, predicates: Collection<BinaryPredicateEvaluation>, targets: Collection<T>): List<Document> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun probe(ats: Long, lts: Long, documents: List<Document>, windowSize: Long, attributeAccess: AttributeAccess, predicates: Collection<BinaryPredicateEvaluation>, targets: Collection<T>): List<Document> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    private fun probeFrom(ats: Long,
                          lts: Long,
                          documents: List<Document>,
                          start: Long,
                          predicates: Collection<BinaryPredicateEvaluation>,
                          targets: Collection<T>): List<Document> {
        probeLog.put(ats, lts, documents, predicates, targets)
        return randomPrefix.get(start, ats)
                .values
                .map { join(documents, it, predicates )}
                .flatten()
    }

    override fun free(ats: Long): Int {
        return randomPrefix.free(ats)
    }


    fun join(probed: Collection<Document>,
             stored: Collection<Document>,
             predicates: Collection<BinaryPredicateEvaluation>): List<Document> {
        val result = ArrayList<Document>()

        for (probedDocument in probed) {
            for (storedDocument in stored) {
                var isJoinable = true
                for (predicateEvaluation in predicates) {
                    val (left, right) = when(predicateEvaluation) {
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
        private val LOG = LoggerFactory.getLogger(NaiveNestedLoopStore::class.java)
    }
}

/**
 * The RandomPrefix is responsible for storing the prefix of a relation.
 *
 * Each stored document (list) is stored associated with their timestamp.
 * The
 */
data class RandomPrefix(
        // These are the stored objects ordered by their timestamp attribute
        //  t ----------------------------------------------
        //       t1 [{k:v,k:v,...}]        t5 [{k:v,...}, {k:v,...}]
        val inner: TreeMap<Long, List<Document>> = TreeMap()
): Serializable {

    fun put(ats: Long, documents: List<Document>) {
        this.inner[ats] = documents
    }

    fun get(fromAts: Long, toAts: Long): SortedMap<Long, List<Document>> {
        return inner.subMap(fromAts, toAts) as SortedMap<Long, List<Document>>
    }

    fun free(ats: Long): Int {
        val headMap = inner.headMap(ats)
        val size = headMap.size
        headMap.clear()
        return size
    }
}
