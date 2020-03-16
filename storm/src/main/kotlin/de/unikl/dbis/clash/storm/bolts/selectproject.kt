package de.unikl.dbis.clash.storm.bolts

import de.unikl.dbis.clash.documents.Document
import de.unikl.dbis.clash.physical.UnaryPredicateEvaluation
import de.unikl.dbis.clash.storm.DocumentsMessage
import de.unikl.dbis.clash.storm.StormInRule
import de.unikl.dbis.clash.storm.StormSelectProjectRule

class SelectProjectBolt(
    name: String
) : AbstractBolt(name), IStoreStats by StoreStats() {

    override fun executeDocuments(message: DocumentsMessage, stormInRule: StormInRule) {
        if (stormInRule is StormSelectProjectRule) {
            val satisfyingDocuments = evaluate(stormInRule.predicates, message.documents)
            val result = satisfyingDocuments.map { it.project(stormInRule.projection) }
            outputCollector.emit(stormInRule.outgoingEdgeLabel, DocumentsMessage(message.ats, message.its, result))
        } else {
            throw RuntimeException("This bolt can only handle SelectProjectRules, but was given a $stormInRule")
        }
    }

    private fun evaluate(predicates: Set<UnaryPredicateEvaluation>, documents: List<Document>) = documents.filter { document -> predicates.all { predicate -> predicate.predicate.evaluate(document) } }
}
