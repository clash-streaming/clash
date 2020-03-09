package de.unikl.dbis.clash.optimizer.constant

import de.unikl.dbis.clash.datacharacteristics.DataCharacteristics
import de.unikl.dbis.clash.optimizer.GlobalStrategy
import de.unikl.dbis.clash.optimizer.OptimizationParameters
import de.unikl.dbis.clash.optimizer.OptimizationResult
import de.unikl.dbis.clash.physical.AggregationStore
import de.unikl.dbis.clash.physical.EdgeType
import de.unikl.dbis.clash.physical.PhysicalGraph
import de.unikl.dbis.clash.physical.RelationSendRule
import de.unikl.dbis.clash.physical.SelectProjectNode
import de.unikl.dbis.clash.physical.SelectProjectRule
import de.unikl.dbis.clash.physical.UnaryPredicateEvaluation
import de.unikl.dbis.clash.physical.addEdge
import de.unikl.dbis.clash.physical.label
import de.unikl.dbis.clash.query.AttributeAccess
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
        val relation = query.result.subRelation(relationAlias)

        val predicates = setOf<UnaryPredicateEvaluation>() // TODO
        val projection = listOf<AttributeAccess>() // TODO

        val physicalGraph = PhysicalGraph()
        val input = physicalGraph.addInputStubFor(relation)

        // Attach select-project node to the input
        val selectProjectNode = SelectProjectNode(label(relation), relation, 1)
        physicalGraph.addSelectProjectNode(selectProjectNode)

        val inputToSPEdge = addEdge(input, selectProjectNode, EdgeType.SHUFFLE)
        input.addRule(RelationSendRule(relation, inputToSPEdge))
        selectProjectNode.addRule(SelectProjectRule(predicates, projection, inputToSPEdge))

        // Attach the aggregation to the select-project node
        val store = AggregationStore(label(relation), relation, 1)
        physicalGraph.addAggregationStore(store)

        val spToAggregationEdge = addEdge(selectProjectNode, store, EdgeType.GROUP_BY)
        selectProjectNode.addRule(RelationSendRule(relation, spToAggregationEdge))
        // store.addRule()
        TODO()
    }
}
