package de.unikl.dbis.clash.storm.builder

import de.unikl.dbis.clash.ClashConfig
import de.unikl.dbis.clash.physical.*
import de.unikl.dbis.clash.query.Relation


class StormPhysicalGraph(internal var dispatcher: Dispatcher, internal var sink: Sink) {
    internal val spouts = mutableSetOf<Spout>()
    internal var reporter: Reporter? = null
    internal var controller: Controller? = null
    internal var controllerInput: ControllerInput? = null
    internal var tickSpout: TickSpout? = null
    internal val relationStores = mutableMapOf<Relation, Store>()

    fun streamNodes(): Collection<Node> {
        return spouts + listOf()
    }
}

/**
 * Given a PhysicalGraphImpl, here we additionally put sources, dispatcher and sink to the
 * equation.
 */
class PhysicalGraphEnhancer(
        internal val originalGraph: PhysicalGraph,
        internal val config: ClashConfig) {

    internal val stormPhysicalGraph: StormPhysicalGraph

    init {
        val dispatcher = Dispatcher(
                this.config.dispatcherName,
                this.config.dispatcherParallelism)
        val sinkName = "output" // TODO
        val sink = Sink(sinkName, originalGraph.outputStub!!.relation,  1)
        stormPhysicalGraph = StormPhysicalGraph(dispatcher, sink)
    }

    fun enhance(): StormPhysicalGraph {
        buildSpouts()
        buildDispatcher()
        copyStores()
        buildSink()

        if (this.config.reporterParallelism > 0) {
            buildReporter()
        }
        if (this.config.controllerEnabled) {
            buildController()
        }
        if (this.config.tickRate > 0) {
            buildTickSpout()
        }
        return this.stormPhysicalGraph
    }

    /**
     * Builds a spout for every input stub in the query plan.
     */
    private fun buildSpouts() {
        this.originalGraph.getInputStubs().values
                .map { Spout("${it.label}_SPOUT", it.relation, config.spoutParallelism)}
                .forEach {stormPhysicalGraph.spouts += it }
    }

    /**
     * Builds the one dispatcher and connects it to the spouts.
     */
    private fun buildDispatcher() {
        // Connect the spouts with the fresh dispatcher
        for (spout in stormPhysicalGraph.spouts) {
            val streamName = addEdge(spout, stormPhysicalGraph.dispatcher, EdgeType.SHUFFLE)
            val sendRule = RelationSendRule(spout.relation, streamName)
            spout.addRule(sendRule)
            val rule = RelationReceiveRule(spout.relation, streamName)
            stormPhysicalGraph.dispatcher.addRule(rule)
        }

        // connect the dispatcher to the receivers of the original input stubs
        for (inputStub in this.originalGraph.getInputStubs().values) {
            for ((edgeLabel, receiver) in inputStub.outgoingEdges) {
                val newStream = addEdge(
                        stormPhysicalGraph.dispatcher,
                        receiver,
                        edgeLabel.edgeType
                )
                val sendRule = RelationSendRule(inputStub.relation, newStream)
                stormPhysicalGraph.dispatcher.addRule(sendRule)
                receiver.replaceIncomingEdge(edgeLabel, newStream)
            }
        }
    }

    private fun copyStores() {
        val dispatcher = stormPhysicalGraph.dispatcher

        for ((relations, store) in originalGraph.relationStores) {
            stormPhysicalGraph.relationStores[relations] = store

            // replace inputStubs with dispatcher
            for((oldLabel, node) in store.incomingEdges.toMap()) {
                if(node is InputStub) {
                    val newLabel = addEdge(dispatcher, store, oldLabel.edgeType)
                    // TODO maybe                     dispatcher.outgoingEdges[newLabel] = store
                    dispatcher.replaceOutgoingEdge(oldLabel, newLabel)
                    store.replaceIncomingEdge(oldLabel, newLabel)
                }
            }
        }
    }

    private fun buildSink() {
        val resultCreators = this.originalGraph.relationProducers[stormPhysicalGraph.sink.relation]
        if (resultCreators == null || resultCreators.isEmpty()) {
            throw RuntimeException("Trying to connect relation producers for relation '"
                    + stormPhysicalGraph.sink.label + "' but none were specified.")
        }
        for (creator in resultCreators) {
            val edgeName = addEdge(creator, stormPhysicalGraph.sink, EdgeType.SHUFFLE)
            val sendRule = RelationSendRule(stormPhysicalGraph.sink.relation, edgeName)
            creator.addRule(sendRule)
            stormPhysicalGraph.sink.addRule(RelationReceiveRule(stormPhysicalGraph.sink.relation, edgeName))
        }
    }

    private fun buildReporter() {
        val reporter = Reporter(
                this.config.reporterName,
                this.config.reporterParallelism)
        this.stormPhysicalGraph.reporter = reporter

        val reporterSender = listOf(
                stormPhysicalGraph.sink,
                stormPhysicalGraph.dispatcher)
                .plus(stormPhysicalGraph.relationStores.values)

        reporterSender.forEach { node ->
            val edge = addEdge(node, reporter, EdgeType.SHUFFLE)
            node.addRule(ReportOutRule(edge))
            reporter.addRule(ReportInRule(edge))
        }
    }

    private fun buildController() {
        val controllerInput = ControllerInput("${this.config.controllerName}_SPOUT")
        val controller = Controller(this.config.controllerName)

        this.stormPhysicalGraph.controllerInput = controllerInput
        this.stormPhysicalGraph.controller = controller

        val inputEdge = addEdge(controllerInput, controller, EdgeType.SHUFFLE)
        controllerInput.addRule(ControlOutRule(inputEdge))
        controller.addRule(ControlInRule(inputEdge))

        val controlReceiver = listOf(
                stormPhysicalGraph.sink,
                stormPhysicalGraph.dispatcher)
                .asSequence()
                .plus(stormPhysicalGraph.relationStores.values)
                .toList()

        controlReceiver.forEach { node ->
            val edge = addEdge(controller, node, EdgeType.ALL)
            controller.addRule(ControlOutRule(edge))
            node.addRule(ControlInRule(edge))
        }
    }

    private fun buildTickSpout() {
        val tickSpout = TickSpout(this.config.tickSpoutName)
        this.stormPhysicalGraph.tickSpout = tickSpout

        val tickReceiver = listOf(
                stormPhysicalGraph.sink,
                stormPhysicalGraph.dispatcher)
                .asSequence()
                .plus(stormPhysicalGraph.relationStores.values)
                .plus(elementIfExits(stormPhysicalGraph.controller))
                .toList()

        tickReceiver.forEach {node ->
            val edge = addEdge(tickSpout, node, EdgeType.SHUFFLE)
            tickSpout.addRule(TickOutRule(edge))
            node.addRule(TickInRule(edge))
        }
    }
}

fun <T: Any> elementIfExits(element: T?): Iterable<T> = if (element == null) listOf() else listOf(element)