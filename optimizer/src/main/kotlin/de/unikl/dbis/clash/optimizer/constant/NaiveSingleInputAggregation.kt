package de.unikl.dbis.clash.optimizer.constant

import de.unikl.dbis.clash.datacharacteristics.DataCharacteristics
import de.unikl.dbis.clash.optimizer.GlobalStrategy
import de.unikl.dbis.clash.optimizer.OptimizationParameters
import de.unikl.dbis.clash.optimizer.OptimizationResult
import de.unikl.dbis.clash.optimizer.emptyCost
import de.unikl.dbis.clash.physical.AggregateRule
import de.unikl.dbis.clash.physical.AggregationStore
import de.unikl.dbis.clash.physical.EdgeType
import de.unikl.dbis.clash.physical.OutputStub
import de.unikl.dbis.clash.physical.PhysicalGraph
import de.unikl.dbis.clash.physical.RelationSendRule
import de.unikl.dbis.clash.physical.SelectProjectNode
import de.unikl.dbis.clash.physical.SelectProjectRule
import de.unikl.dbis.clash.physical.UnaryPredicateEvaluation
import de.unikl.dbis.clash.physical.addEdge
import de.unikl.dbis.clash.physical.label
import de.unikl.dbis.clash.query.Projection
import de.unikl.dbis.clash.query.Query
import java.lang.RuntimeException

/**
 * This strategy only builds the following graph:
 *
 * INPUT -> SP-NODE -> AGGREGATION
 */
class NaiveSingleInputAggregation : GlobalStrategy {
    override fun optimize(
        query: Query,
        dataCharacteristics: DataCharacteristics,
        params: OptimizationParameters
    ): OptimizationResult {
        if (query.inputMap.size != 1) {
            throw RuntimeException("This optimizer cannot handle joins.")
        }

        val relationAlias = query.inputMap.keys.first()

        val inputRelation = query.result.justInput()
        val spRelation = query.result.withoutAggregation()
        val resultRelation = query.result

        val predicates = spRelation.filters
        val projection = spRelation.projections.toList()

        // Build the graph
        val physicalGraph = PhysicalGraph()
        val input = physicalGraph.addInputStubFor(inputRelation)
        val selectProjectNode = SelectProjectNode(label(spRelation), spRelation, 1)
        physicalGraph.addSelectProjectNode(selectProjectNode)
        val aggregationStore = AggregationStore(label(resultRelation) + "_agg", resultRelation, 1)
        physicalGraph.addAggregationStore(aggregationStore)
        val outputStub = OutputStub(label(query.result), query.result)
        physicalGraph.outputStub = outputStub
        physicalGraph.addRelationProducer(resultRelation, aggregationStore)

        // Connect the nodes
        val inputToSPEdge = addEdge(input, selectProjectNode, EdgeType.SHUFFLE)
        val spToAggregationEdge = addEdge(selectProjectNode, aggregationStore, EdgeType.GROUP_BY)
        val aggregationToOutputEdge = addEdge(aggregationStore, outputStub, EdgeType.SHUFFLE)

        // Configure the rules
        input.addRule(RelationSendRule(inputRelation, inputToSPEdge))
        selectProjectNode.addRule(SelectProjectRule(predicates, projection, inputToSPEdge, spToAggregationEdge))
        aggregationStore.addRule(AggregateRule(resultRelation.aggregations.toList(), spToAggregationEdge, aggregationToOutputEdge))

        return OptimizationResult(physicalGraph, emptyCost())
    }
}
