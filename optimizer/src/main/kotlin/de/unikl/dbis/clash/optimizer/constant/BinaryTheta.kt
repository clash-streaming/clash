package de.unikl.dbis.clash.optimizer.constant

import de.unikl.dbis.clash.datacharacteristics.DataCharacteristics
import de.unikl.dbis.clash.optimizer.GlobalStrategy
import de.unikl.dbis.clash.optimizer.OptimizationParameters
import de.unikl.dbis.clash.optimizer.OptimizationResult
import de.unikl.dbis.clash.optimizer.emptyCost
import de.unikl.dbis.clash.physical.BinaryPredicateEvaluation
import de.unikl.dbis.clash.physical.BinaryPredicateEvaluationLeftStored
import de.unikl.dbis.clash.physical.BinaryPredicateEvaluationRightStored
import de.unikl.dbis.clash.physical.EdgeType
import de.unikl.dbis.clash.physical.JoinResultRule
import de.unikl.dbis.clash.physical.OutputStub
import de.unikl.dbis.clash.physical.PhysicalGraph
import de.unikl.dbis.clash.physical.RelationReceiveRule
import de.unikl.dbis.clash.physical.RelationSendRule
import de.unikl.dbis.clash.physical.ThetaStore
import de.unikl.dbis.clash.physical.addEdge
import de.unikl.dbis.clash.physical.label
import de.unikl.dbis.clash.query.BinaryPredicate
import de.unikl.dbis.clash.query.Query
import java.lang.RuntimeException

/**
 * This is an example strategy that illustrates the construction of a physical graph.
 * It can only produce binary joins.
 */
class BinaryTheta : GlobalStrategy {
    override fun optimize(query: Query, dataCharacteristics: DataCharacteristics, params: OptimizationParameters): OptimizationResult {
        if (query.inputMap.size != 2) {
            throw RuntimeException("BinaryTheta strategy cannot handle ${query.inputMap.size} relations, has to be exactly 2.")
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
        val storeA = ThetaStore(label(relA), relA, 1)
        physicalGraph.relationStores[relA] = storeA
        val storeB = ThetaStore(label(relB), relB, 1)
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
        val predicateEvaluationA = predicatesForStore(storeA, query.result.binaryPredicates)
        storeA.addRule(JoinResultRule(probeAEdge, predicateEvaluationA, query.result))

        val probeBEdge = addEdge(inputA, storeB, EdgeType.ALL)
        val predicateEvaluationB = predicatesForStore(storeB, query.result.binaryPredicates)
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

    /**
     * We need to figure out, how a predicate is to be evaluated.
     * For example, a.x >= b.y is evaluated differently at the a-store and the b-store.
     * At the a-store, the arriving tuple is b and of the arriving tuple we access y and compare it to the stored x value.
     * At the b-store, we compare the x attribute of the arriving tuple with the y value of stored tuples.
     */
    private fun predicatesForStore(store: ThetaStore, binaryPredicates: Collection<BinaryPredicate>): Set<BinaryPredicateEvaluation> {
        return binaryPredicates.map { predicate ->
            if (store.relation.aliases.contains(predicate.leftRelationAlias))
                BinaryPredicateEvaluationLeftStored(predicate)
            else BinaryPredicateEvaluationRightStored(predicate)
        }.toSet()
    }
}
