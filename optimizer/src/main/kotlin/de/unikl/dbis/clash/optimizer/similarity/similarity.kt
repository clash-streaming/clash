package de.unikl.dbis.clash.optimizer.similarity

import de.unikl.dbis.clash.datacharacteristics.DataCharacteristics
import de.unikl.dbis.clash.optimizer.*
import de.unikl.dbis.clash.physical.*
import de.unikl.dbis.clash.query.Query
import java.lang.RuntimeException

class SimilarityOptimizer() : GlobalStrategy {
    override fun optimize(query: Query, dataCharacteristics: DataCharacteristics, params: OptimizationParameters): OptimizationResult {
        if(query.inputMap.size != 2) {
            throw RuntimeException("Similarity strategy cannot handle ${query.inputMap.size} relations, has to be exactly 2.")
        }
        val (aliasA, aliasB) = query.inputMap.keys.toList()
        val relA = query.result.subRelation(aliasA)
        val relB = query.result.subRelation(aliasB)

        val physicalGraph = PhysicalGraph()

        // We build the input stubs for both relations.
        // They will be replaced with according spouts later
        val inputA = physicalGraph.addInputStubFor(relA)
        val inputB = physicalGraph.addInputStubFor(relB)

        // Add the stores for both relations
        val storeA = SimilarityStore(label(relA), relA, 1)
        physicalGraph.relationStores[relA] = storeA
        val storeB = SimilarityStore(label(relB), relB, 1)
        physicalGraph.relationStores[relB] = storeB

        // Wire the stores

        // Send tuples to the stores for storing
        val storeAEdge = addEdge(inputA, storeA, EdgeType.SHUFFLE)
        inputA.addRule(RelationSendRule(relA, storeAEdge))
        storeA.addRule(RelationReceiveRule(relA, storeAEdge))
        val storeBEdge = addEdge(inputB, storeB, EdgeType.SHUFFLE)
        inputB.addRule(RelationSendRule(relB, storeBEdge))
        storeB.addRule(RelationReceiveRule(relB, storeBEdge))

        // Send tuples for probing
        val probeAEdge = addEdge(inputB, storeA, EdgeType.ALL)
        val predicateEvaluationA = query.result.binaryPredicates.map { GenericBinaryPredicateEvaluation(it) }.toSet()
        storeA.addRule(JoinResultRule(probeAEdge, predicateEvaluationA, query.result))

        val probeBEdge = addEdge(inputA, storeB, EdgeType.ALL)
        val predicateEvaluationB = query.result.binaryPredicates.map { GenericBinaryPredicateEvaluation(it) }.toSet()
        storeB.addRule(JoinResultRule(probeBEdge, predicateEvaluationB, query.result))

        physicalGraph.addRelationProducer(query.result, storeA)
        physicalGraph.addRelationProducer(query.result, storeB)

        // Finally, the output stub receives results from the stores
        val outputStub = OutputStub(label(query.result), query.result)
        physicalGraph.outputStub = outputStub
        val storeAResultEdge = addEdge(storeA, outputStub, EdgeType.SHUFFLE)
        storeA.addRule(RelationSendRule(query.result, storeAResultEdge))
        val storeBResultEdge = addEdge(storeB, outputStub, EdgeType.SHUFFLE)
        storeB.addRule(RelationSendRule(query.result, storeBResultEdge))

        return OptimizationResult(physicalGraph, emptyCost())
    }
}