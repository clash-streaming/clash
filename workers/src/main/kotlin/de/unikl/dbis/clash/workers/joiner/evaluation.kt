package de.unikl.dbis.clash.workers.joiner

import de.unikl.dbis.clash.documents.Document
import de.unikl.dbis.clash.physical.BinaryPredicateEvaluation
import de.unikl.dbis.clash.physical.BinaryPredicateEvaluationLeftStored
import de.unikl.dbis.clash.physical.BinaryPredicateEvaluationRightStored
import org.slf4j.LoggerFactory
import java.util.ArrayList


object Join {

    private val LOG = LoggerFactory.getLogger(Join::class.java)

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
        LOG.debug("joinPredicateEvaluation " + probed + " -- " + stored
                + " <<< " + predicates + " >>> " + result.size)

        return result
    }
}
