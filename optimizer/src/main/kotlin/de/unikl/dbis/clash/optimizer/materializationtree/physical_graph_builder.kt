package de.unikl.dbis.clash.optimizer.materializationtree

import de.unikl.dbis.clash.optimizer.probeorder.ProbeOrder
import de.unikl.dbis.clash.physical.*
import de.unikl.dbis.clash.query.BinaryPredicate
import de.unikl.dbis.clash.query.Relation


fun build(tree: MaterializationTree): PhysicalGraph {
    val physicalGraph = PhysicalGraph()
    val builders = mutableListOf<NodeBuilder>()
    tree.walkNodes().forEach {
        when(it) {
            is MatMultiStream -> builders.add(MatMultiStreamBuilder(it))
            is NonMatMultiStream -> builders.add(NonMatMultiStreamBuilder(it))
            is MatSource -> builders.add(MatSourceBuilder(it))
        }
    }

    builders.forEach { it.addStubs(physicalGraph) }
    builders.forEach { it.addStores(physicalGraph) }
    builders.forEach { it.markStores(physicalGraph) }
    builders.forEach { it.wireStores(physicalGraph) }

    return physicalGraph
}

interface NodeBuilder {
    fun addStubs(physicalGraph: PhysicalGraph) { }
    fun addStores(physicalGraph: PhysicalGraph) { }
    fun markStores(physicalGraph: PhysicalGraph) { }
    fun wireStores(physicalGraph: PhysicalGraph) { }
}

class MatMultiStreamBuilder(val multiStream: MatMultiStream) : NodeBuilder {
    override fun addStores(physicalGraph: PhysicalGraph) {
        val store = PartitionedStore(label(multiStream.relation), multiStream.partitioning, multiStream.relation, multiStream.parallelism)
        physicalGraph.relationStores[multiStream.relation] = store
    }

    override fun markStores(physicalGraph: PhysicalGraph) {
        for((probeOrder, _) in multiStream.probeOrders.inner.values) {
            val last = probeOrder.steps.last()
            physicalGraph.addRelationProducer(multiStream.relation, physicalGraph.getRelationStore(last.first.relation))
        }
    }

    override fun wireStores(physicalGraph: PhysicalGraph) {
        for((probeOrder, _) in multiStream.probeOrders.inner.values) {
            val lc = LinearConnector(probeOrder, physicalGraph, multiStream.relation)
            lc.connectAll()
        }
        for(node in physicalGraph.relationProducers[multiStream.relation]!!) {
            val store = physicalGraph.relationStores[multiStream.relation]!!
            val sourceEdge = addEdge(node, store, EdgeType.SHUFFLE)
            val sendRule = RelationSendRule(multiStream.relation, sourceEdge)
            node.addRule(sendRule)

            val rule = RelationReceiveRule(multiStream.relation, sourceEdge)
            store.addRule(rule)
        }
    }
}

/**
 * For now, the NonMatMultiStreamOld is always the root node, so it can be assigned an output stub.
 * If that changes, this builder has to be changed as well.
 */
class NonMatMultiStreamBuilder(val multiStream: NonMatMultiStream) : NodeBuilder {
    lateinit var outputStub: OutputStub

    override fun addStubs(physicalGraph: PhysicalGraph) {
        outputStub = OutputStub(label(multiStream.relation), multiStream.relation)
        physicalGraph.outputStub = outputStub
    }

    override fun markStores(physicalGraph: PhysicalGraph) {
        for((probeOrder, _)in multiStream.probeOrders.inner.values) {
            val last = probeOrder.steps.last()
            physicalGraph.addRelationProducer(multiStream.relation, physicalGraph.getRelationStore(last.first.relation))
        }
    }

    override fun wireStores(physicalGraph: PhysicalGraph) {
        for((probeOrder, _) in multiStream.probeOrders.inner.values) {
            val lc = LinearConnector(probeOrder, physicalGraph, multiStream.relation)
            lc.connectAll()
        }

        for(producer in physicalGraph.relationProducers.getOrDefault(multiStream.relation, emptyList<Node>())) {
            val edge = addEdge(producer, outputStub, EdgeType.SHUFFLE)
            val sendRule = RelationSendRule(multiStream.relation, edge)
            producer.addRule(sendRule)
        }
    }
}

class MatSourceBuilder(val matSource: MatSource) : NodeBuilder {
    override fun addStubs(physicalGraph: PhysicalGraph) {
        physicalGraph.addInputStubFor(matSource.relation)
    }

    override fun addStores(physicalGraph: PhysicalGraph) {
        val store = PartitionedStore(label(matSource.relation), matSource.partitioning, matSource.relation, matSource.parallelism)
        physicalGraph.relationStores[matSource.relation] = store

        physicalGraph.relationProducers[matSource.relation]!!.forEach {
            val sourceEdge = addEdge(it, store, EdgeType.SHUFFLE)
            val sendRule = RelationSendRule(matSource.relation, sourceEdge)
            it.addRule(sendRule)

            val rule = RelationReceiveRule(matSource.relation, sourceEdge)
            store.addRule(rule)
        }
    }

    override fun markStores(physicalGraph: PhysicalGraph) {
    }
}


/**
 * The LinearConnector is responsible for building a single intra-operator join order.
 *
 * Say, the order is R -> S -> T -> U. Then this means, a tuple from stream R arrives. This tuple is
 * somewhere created in the relationProducers of R, thus all these have to be connected with S; this
 * is done in the connectFirst method.
 *
 * Then the edges between the remaining tuples are created in the connectInner method, and the
 * according JoinRules are set.
 *
 * At last, in method connectLast(), a OldJoinResultRule is added.
 */
class LinearConnector(val probeOrder: ProbeOrder, val graph: PhysicalGraph, val producedRelation: Relation) {

    var currentStep = probeOrder.steps[0]
    var nextStep = probeOrder.steps[1]
    var streamsToCurrent: MutableSet<EdgeLabel> = mutableSetOf()
    var step = 0
    var finished = probeOrder.steps.size == 2

    fun connectAll() {
        connectFirst()
        while (!finished) {
            connectInner()
        }
        connectLast()
    }

    fun connectFirst() {
        val currentNodes = graph.relationProducers[currentStep.first.relation]!!
        val nextStore = graph.getRelationStore(nextStep.first.relation)

        streamsToCurrent = HashSet()
        for (current in currentNodes) {

            val edge = addEdge(current, nextStore, EdgeType.ALL)
            current.outgoingEdges[edge] = nextStore
            current.addRule(RelationSendRule(this.currentStep.first.relation, edge))

            streamsToCurrent.add(edge)
        }

        step()
    }

    fun connectInner() {
        val currentStore = graph.getRelationStore(currentStep.first.relation)
        val nextStore = graph.getRelationStore(nextStep.first.relation)
        val predicatesForCurrent = currentStep.second
        val predicateEvaluationsForCurrent = getPredicateEvaluation(currentStore, predicatesForCurrent)

        val outgoingStreamName = addEdge(currentStore, nextStore, EdgeType.ALL)
        currentStore.outgoingEdges[outgoingStreamName] = nextStore
        for (incomingStreamName in streamsToCurrent) {
            currentStore.addRule(IntermediateJoinRule(
                    incomingStreamName,
                    outgoingStreamName,
                    predicateEvaluationsForCurrent)
            )
        }
        streamsToCurrent = mutableSetOf(outgoingStreamName)
        step()
    }

    fun connectLast() {
        val currentStore = graph.getRelationStore(currentStep.first.relation)
        val predicatesForCurrent = currentStep.second
        val predicateEvaluationsForCurrent = getPredicateEvaluation(currentStore, predicatesForCurrent)

        for (incomingStreamName in streamsToCurrent) {
            currentStore.addRule(JoinResultRule(
                    incomingStreamName,
                    predicateEvaluationsForCurrent,
                    producedRelation)
            )
        }
        graph.addRelationProducer(producedRelation, currentStore)
    }

    fun step() {
        step++

        currentStep = probeOrder.steps[step]
        if (step == probeOrder.steps.size - 1) {
            finished = true
        } else {
            nextStep = probeOrder.steps[step + 1]
        }
    }

    fun getPredicateEvaluation(store: Store, predicates: Set<BinaryPredicate>): Set<BinaryPredicateEvaluation> {
        return predicates.map { predicate ->
            if(store.relation.aliases.contains(predicate.leftRelationAlias))
                BinaryPredicateEvaluationLeftStored(predicate)
            else BinaryPredicateEvaluationRightStored(predicate)
        }.toSet()
    }
}
