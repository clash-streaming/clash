package de.unikl.dbis.clash.workers.stores

import de.unikl.dbis.clash.ClashConfig
import de.unikl.dbis.clash.documents.Document
import de.unikl.dbis.clash.physical.BinaryPredicateEvaluation
import de.unikl.dbis.clash.query.AttributeAccess
import java.io.Serializable

class ActualSimilarityStore<T>(val config: ClashConfig) : ActualStore<T>, Serializable {
    val tree = TreeWithMapModified()

    override fun store(ats: Long, documents: List<Document>): DelayedStoreJoinResult<T> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun probe(ats: Long, lts: Long, documents: List<Document>, predicates: Collection<BinaryPredicateEvaluation>, targets: Collection<T>): List<Document> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun probe(ats: Long, lts: Long, documents: List<Document>, windowSize: Long, predicates: Collection<BinaryPredicateEvaluation>, targets: Collection<T>): List<Document> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun free(ats: Long): Int {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun probe(ats: Long, lts: Long, documents: List<Document>, attributeAccess: AttributeAccess, predicates: Collection<BinaryPredicateEvaluation>, targets: Collection<T>): List<Document> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun probe(ats: Long, lts: Long, documents: List<Document>, windowSize: Long, attributeAccess: AttributeAccess, predicates: Collection<BinaryPredicateEvaluation>, targets: Collection<T>): List<Document> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}