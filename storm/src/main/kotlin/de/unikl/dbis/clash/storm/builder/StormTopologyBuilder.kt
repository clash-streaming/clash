package de.unikl.dbis.clash.storm.builder

import de.unikl.dbis.clash.ClashConfig
import de.unikl.dbis.clash.physical.ControlOutRule
import de.unikl.dbis.clash.physical.Controller
import de.unikl.dbis.clash.physical.ControllerInput
import de.unikl.dbis.clash.physical.Dispatcher
import de.unikl.dbis.clash.physical.EdgeType
import de.unikl.dbis.clash.physical.InputStub
import de.unikl.dbis.clash.physical.Node
import de.unikl.dbis.clash.physical.OutRule
import de.unikl.dbis.clash.physical.PartitionedStore
import de.unikl.dbis.clash.physical.PhysicalGraph
import de.unikl.dbis.clash.physical.SimilarityStore
import de.unikl.dbis.clash.physical.Sink
import de.unikl.dbis.clash.physical.Spout
import de.unikl.dbis.clash.physical.Store
import de.unikl.dbis.clash.physical.ThetaStore
import de.unikl.dbis.clash.physical.TickOutRule
import de.unikl.dbis.clash.physical.TickSpout
import de.unikl.dbis.clash.query.InputName
import de.unikl.dbis.clash.query.RelationAlias
import de.unikl.dbis.clash.query.inputForRelation
import de.unikl.dbis.clash.storm.DataPathMessage
import de.unikl.dbis.clash.storm.StormEdgeLabel
import de.unikl.dbis.clash.storm.bolts.CommonSinkI
import de.unikl.dbis.clash.storm.bolts.ControlBolt
import de.unikl.dbis.clash.storm.bolts.DispatchBolt
import de.unikl.dbis.clash.storm.bolts.GeneralStore
import de.unikl.dbis.clash.storm.spouts.CommonSpout
import de.unikl.dbis.clash.storm.spouts.CommonSpoutI
import de.unikl.dbis.clash.storm.spouts.ControlSpout
import de.unikl.dbis.clash.support.KafkaConfig
import de.unikl.dbis.clash.workers.stores.ActualSimilarityStore
import de.unikl.dbis.clash.workers.stores.ActualStore
import de.unikl.dbis.clash.workers.stores.NaiveHashStore
import de.unikl.dbis.clash.workers.stores.NaiveNestedLoopStore
import org.apache.storm.generated.StormTopology
import org.apache.storm.topology.BoltDeclarer
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.tuple.Fields
import org.slf4j.LoggerFactory

class StormTopologyBuilder(
    val inputGraph: PhysicalGraph,
    val spouts: Map<InputName, CommonSpoutI> = mapOf(),
    val inputMap: Map<RelationAlias, InputName> = mapOf(),
    val sink: CommonSinkI,
    val config: ClashConfig = ClashConfig()
) {
    companion object {
        val LOG = LoggerFactory.getLogger(StormTopologyBuilder::class.java)!!
    }

    private lateinit var enhancedGraph: StormPhysicalGraph

    fun build(): StormTopology {
        val builder = TopologyBuilder()
        return build(builder)
    }

    fun build(builder: TopologyBuilder): StormTopology {
        val pge = PhysicalGraphEnhancer(
                this.inputGraph,
                this.config
        )
        this.enhancedGraph = pge.enhance()

        this.buildSpouts(builder)
        this.buildStores(builder)

        this.buildDispatcher(this.enhancedGraph.dispatcher, builder)
        this.buildSink(this.enhancedGraph.sink, builder)

        if (enhancedGraph.controller != null) {
            this.buildController(this.enhancedGraph.controllerInput!!, this.enhancedGraph.controller!!, builder)
        }
        if (this.enhancedGraph.tickSpout != null) {
            this.buildTickSpout(this.enhancedGraph.tickSpout!!, builder)
        }

        return builder.createTopology()
    }

    private fun declareGroupings(node: Node, declarer: BoltDeclarer) {
        for (label in node.incomingEdges.keys) {
            val sourceStream = label.label
            val sourceNode: String
            sourceNode = if (node.incomingEdges[label] is InputStub) {
                this.enhancedGraph.dispatcher.label
            } else {
                node.incomingEdges[label]!!.label
            }
            when (label.edgeType) {
                EdgeType.ALL -> declarer.allGrouping(sourceNode, sourceStream)
                EdgeType.SHUFFLE -> declarer.shuffleGrouping(sourceNode, sourceStream)
                EdgeType.GROUP_BY -> declarer.fieldsGrouping(
                        sourceNode,
                        sourceStream,
                        Fields(DataPathMessage.GROUPING_FIELD)
                )
            }
        }
    }

    internal fun buildSpouts(builder: TopologyBuilder) {
        for (spoutNode in this.enhancedGraph.spouts) {
            this.buildSpout(spoutNode, builder)
        }
    }

    internal fun buildSpout(spoutNode: Spout, builder: TopologyBuilder) {
        LOG.debug("Building spout {}...", spoutNode.label)
        val spout = spouts[inputForRelation(spoutNode.relation, inputMap)] ?: throw RuntimeException("Trying to build spout '" + spoutNode.label +
                "' but no spout with this label was registered.")
        spoutNode.rules.forEach {
            when (it) { is OutRule -> (spout as CommonSpout).addRule(it) }
        }
        builder.setSpout(spoutNode.label, spout)
        LOG.debug("Spout {} built.", spoutNode.label)
    }

    internal fun buildStores(builder: TopologyBuilder) {
        this.enhancedGraph
                .relationStores
                .values
                .forEach { this.buildStore(it, builder) }
    }

    internal fun buildStore(storeNode: Store, builder: TopologyBuilder) {
        val nodeLabel = storeNode.label
        LOG.debug("Building store {}...", nodeLabel)

        val store: ActualStore<StormEdgeLabel> = when (storeNode) {
            is PartitionedStore -> NaiveHashStore(config)
            is ThetaStore -> NaiveNestedLoopStore(config)
            is SimilarityStore -> ActualSimilarityStore(config)
            else -> NaiveNestedLoopStore(config)
        }
        val storeBolt = GeneralStore(nodeLabel, store)
        val declarer = builder
                .setBolt(nodeLabel, storeBolt, storeNode.parallelism)
        declareGroupings(storeNode, declarer)

        for (rule in storeNode.rules) {
            storeBolt.addRule(rule)
        }
        LOG.debug("Store {} built.", storeNode.label)
    }

    internal fun buildDispatcher(
        dispatcherNode: Dispatcher,
        builder: TopologyBuilder
    ) {
        LOG.debug("Building dispatcher {}...", dispatcherNode.label)
        val dispatcher = DispatchBolt(dispatcherNode.label)
        val declarer = builder
                .setBolt(dispatcherNode.label, dispatcher, this.config.dispatcherParallelism)
        for ((label, node) in dispatcherNode.incomingEdges) {
            if (node is Controller) {
                continue
            }
            val sourceStream = label.label
            val sourceNode = dispatcherNode.incomingEdges[label]!!.label
            when (label.edgeType) {
                EdgeType.ALL -> declarer.allGrouping(sourceNode, sourceStream)
                EdgeType.SHUFFLE -> declarer.shuffleGrouping(sourceNode, sourceStream)
                else -> throw RuntimeException("Unknown edge type used " + label.edgeType)
            }
        }

        for (rule in dispatcherNode.rules) {
            dispatcher.addRule(rule)
        }
        LOG.debug("Dispatcher {} built.", dispatcherNode.label)
    }

    internal fun buildSink(sinkNode: Sink, builder: TopologyBuilder) {
        LOG.debug("Building sink {}...", sinkNode.label)
        val declarer = builder.setBolt(sinkNode.label, sink, sinkNode.parallelism)
        declareGroupings(sinkNode, declarer)

        for (rule in sinkNode.rules) {
            sink.addRule(rule)
        }
        LOG.debug("Sink {} built.", sinkNode.label)
    }

    private fun buildController(controllerInput: ControllerInput, controller: Controller, builder: TopologyBuilder) {
        LOG.debug("Building controller {}...", controller.label)
        val kafkaConfig = KafkaConfig(config.kafkaBootstrapServers)
        val controlSpout = ControlSpout(controllerInput.label, kafkaConfig)
        builder.setSpout(controllerInput.label, controlSpout, controller.parallelism)
        controllerInput.rules.forEach { rule -> controlSpout.addRule(rule as ControlOutRule) }

        val controlBolt = ControlBolt(controller.label)
        builder.setBolt(controller.label, controlBolt)
        controller.rules.forEach { rule -> controlBolt.addRule(rule) }
        LOG.debug("Controller {} built.", controller.label)
    }

    private fun buildTickSpout(tickSpout: TickSpout, builder: TopologyBuilder) {
        LOG.debug("Building tick spout {} ...", tickSpout.label)
        val stormTickSpout = de.unikl.dbis.clash.storm.spouts.TickSpout(config)
        builder.setSpout(tickSpout.label, stormTickSpout, 1)
        tickSpout.rules.forEach { rule -> stormTickSpout.addRule(rule as TickOutRule) }
        LOG.debug("TickSpout {} built.", tickSpout.label)
    }
}
