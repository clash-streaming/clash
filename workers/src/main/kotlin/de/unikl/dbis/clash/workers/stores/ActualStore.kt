package de.unikl.dbis.clash.workers.stores

import de.unikl.dbis.clash.documents.Document
import de.unikl.dbis.clash.physical.BinaryPredicateEvaluation
import de.unikl.dbis.clash.query.AttributeAccess

interface ActualStore<T> {

    fun store(ats: Long, documents: List<Document>): DelayedStoreJoinResult<T>

    fun probe(ats: Long, lts: Long, documents: List<Document>, predicates: Collection<BinaryPredicateEvaluation>, targets: Collection<T>): List<Document>

    fun probe(ats: Long, lts: Long, documents: List<Document>, windowSize: Long, predicates: Collection<BinaryPredicateEvaluation>, targets: Collection<T>): List<Document>

    fun free(ats: Long): Int

    fun probe(ats: Long, lts: Long, documents: List<Document>, attributeAccess: AttributeAccess, predicates: Collection<BinaryPredicateEvaluation>, targets: Collection<T>): List<Document>
    fun probe(ats: Long, lts: Long, documents: List<Document>, windowSize: Long, attributeAccess: AttributeAccess, predicates: Collection<BinaryPredicateEvaluation>, targets: Collection<T>): List<Document>
}
