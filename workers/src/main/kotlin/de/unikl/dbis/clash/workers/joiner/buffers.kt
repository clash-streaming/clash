package de.unikl.dbis.clash.workers.joiner

import de.unikl.dbis.clash.documents.Document
import de.unikl.dbis.clash.physical.BinaryPredicateEvaluation
import de.unikl.dbis.clash.query.AttributeList
import java.io.Serializable
import java.util.*


abstract class AbstractBuffer : Serializable


interface StoreBufferI {
    fun join(
            documents: List<Document>,
            seq: Long,
            predicates: Collection<BinaryPredicateEvaluation>): List<Document>

    fun put(seq: Long, documents: List<Document>)

    fun size(): Int
}

/**
 * The StoreBuffer is responsible for storing the prefix of a relation and computing if a probe
 * arrives, computing the join with the probe and all previously stored documents.
 *
 * This is the intuitive place where join operations happen.
 */
@Deprecated("use random prefix")
class StoreBuffer : AbstractBuffer(), StoreBufferI {

    // These are the stored objects ordered by their timestamp attribute
    //  t ----------------------------------------------
    //       t1 [{k:v,k:v,...}]        t5 [{k:v,...}, {k:v,...}]
    private val buffer = TreeMap<Long, List<Document>>()

    fun clearUpto(seq: Long): Int {
        val headMap = this.buffer.headMap(seq, true)
        val result = headMap.size
        headMap.clear()
        return result
    }

    override fun put(seq: Long, documents: List<Document>) {
        this.buffer[seq] = documents
    }

    override fun size(): Int {
        return this.buffer.size
    }

    private fun subBuffer(fromSeq: Long, toSeq: Long): SortedMap<Long, List<Document>> {
        return this.buffer.subMap(fromSeq, toSeq)
    }

    override fun join(
            documents: List<Document>,
            seq: Long,
            predicates: Collection<BinaryPredicateEvaluation>): List<Document> {
        val result = ArrayList<Document>()
        for (bufferedDocuments in this.subBuffer(java.lang.Long.MIN_VALUE, seq).values) {
            result.addAll(Join.join(documents, bufferedDocuments, predicates))
        }
        return result
    }
}

@Deprecated("use partitioned prefix")
class PartitionedStoreBuffer(private val partitioningAttributes: AttributeList) : AbstractBuffer(), StoreBufferI {
    private val buffer: HashMap<List<String>, StoreBuffer> = HashMap()

    override fun join(documents: List<Document>, seq: Long,
                      predicates: Collection<BinaryPredicateEvaluation>): List<Document> {
        val result = ArrayList<Document>()
        for (d in documents) {
            // TODO
//            if (!d.keys.containsAll(this.partitioningAttributes)) {
//                continue
//            }
            val currentKey = this.partitioningAttributes.map { d[it]!! }
            if (this.buffer.containsKey(currentKey)) {
                result.addAll(this.buffer[currentKey]!!.join(listOf(d), seq, predicates))
            }
        }
        return result
    }

    override fun put(seq: Long, documents: List<Document>) {
        for (d in documents) {
            val currentKey = this.partitioningAttributes.map { d[it]!! }
            val inner = (buffer as MutableMap<List<String>, StoreBuffer>).getOrPut(currentKey) { StoreBuffer() }
            inner.put(seq, listOf(d))
        }

    }

    override fun size(): Int {
        return this.buffer.values.sumBy { it.size() }
    }
}

class WindowedStoreBuffer : AbstractBuffer(), StoreBufferI {

    // These are the stored objects ordered by their timestamp attribute
    //  t ----------------------------------------------
    //       t1 [{k:v,k:v,...}]        t5 [{k:v,...}, {k:v,...}]
    private val buffer = TreeMap<Long, List<Document>>()

    fun clearUpto(seq: Long): Int {
        val headMap = this.buffer.headMap(seq, true)
        val result = headMap.size
        headMap.clear()
        return result
    }

    override fun put(seq: Long, documents: List<Document>) {
        this.buffer[seq] = documents
    }

    override fun size(): Int {
        return this.buffer.size
    }

    private fun subBuffer(fromSeq: Long, toSeq: Long): SortedMap<Long, List<Document>> {
        return this.buffer.subMap(fromSeq, toSeq)
    }

    override fun join(
            documents: List<Document>,
            seq: Long,
            predicates: Collection<BinaryPredicateEvaluation>): List<Document> {
        val result = ArrayList<Document>()
        for (bufferedDocuments in this.subBuffer(java.lang.Long.MIN_VALUE, seq).values) {
            result.addAll(Join.join(documents, bufferedDocuments, predicates))
        }
        return result
    }
}