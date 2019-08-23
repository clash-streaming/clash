package de.unikl.dbis.clash.physical

import de.unikl.dbis.clash.query.Relation

/**
 * This is the implementation of a physical graph that is the result from optimization.
 *
 * Also, this is the only place where the outside world should manipulate the graph's components,
 * i.e., adding nodes, rules, etc.
 *
 * relation stores are indexed by the relation they store
 * the relationProducers map additionally marks where which relation is created
 */
open class PhysicalGraph {
    private val inputStubs = mutableMapOf<Relation, InputStub>()
    var outputStub: OutputStub? = null
    val relationStores = mutableMapOf<Relation, Store>()

    // TODO is this needed?    val relationConsumers = mutableMapOf<String, MutableSet<Node>>()
    val relationProducers = mutableMapOf<Relation, MutableSet<Node>>()

    /**
     * Add a relation producer. This is a node in the graph which computes tuples that belong to the
     * relation. It is possible, that the node does not produce the entire relation, however each
     * tuple that it produces is complete, i.e. output \subseteq relation.
     * @param relation the relation
     * @param producer the node that consumes the relation
     */
    fun addRelationProducer(relation: Relation, producer: Node) {
        val producers = this.relationProducers.getOrPut(relation) { mutableSetOf() }
        producers.add(producer)
    }

    /**
     * Convenience method if there is only one store registered for a relation.
     *
     * @param relation the label of the relation
     * @return The registered Store for this relation
     * @throws RuntimeException if no or more than one Store was registered
     */
    fun getRelationStore(relation: Relation): Store {
        return relationStores[relation] ?: throw RuntimeException("Wanted to access a store for relation '" + relation
                + "' but no Store is associated.")
    }

    /**
     * Add an input stub. This is a node in the graph that acts as source of tuples for the given
     * relation and is intended to be replaced later on.
     * @param relation the label of the relation
     * @return the input stub object
     */
    fun addInputStubFor(relation: Relation): InputStub {
        val inputStub = InputStub(label(relation), relation)
        this.inputStubs[relation] = inputStub
        this.addRelationProducer(relation, inputStub)
        return inputStub
    }

    fun getInputStubs(): Map<Relation, InputStub> {
        return inputStubs.toMap()
    }

    /**
     * Add an output stub. This is a node in the graph that acts as sink of tuples for the given
     * relation and is intended to be replaced later on.
     * @param relation the label of the relation
     * @return the output stub object
     */
    fun addOutputStubFor(relation: Relation): OutputStub {
        val outputStub = OutputStub(label(relation), relation)
        this.outputStub = outputStub
// TODO is this needed?        this.relationConsumers
// TODO is this needed?                .getOrPut(relation) { mutableSetOf() }
// TODO is this needed?                .add(outputStub)
        return outputStub
    }

    /**
     * Stream all nodes of that graph.
     *
     * @return Stream of all nodes of the graph.
     */
    open fun streamNodes(): Collection<Node> {
        return listOf<Node>().asSequence().plus(
                this.inputStubs.values
        ).plus(
                this.outputStub!!
        ).plus(
                relationStores.values
        ).toList()
    }

    fun streamEdges(): Collection<EdgeLabel> {
        return streamNodes().flatMap { it.incomingEdges.keys + it.outgoingEdges.keys }.distinct()
    }
}


/**
 * Adds an edge into the graph.
 *
 * @param from origin of the edge
 * @param to target of the edge
 * @param type type of the edge
 * @return a freshly created edge label
 */
fun addEdge(from: Node, to: Node, type: EdgeType): EdgeLabel {
    val edgeLabel = EdgeLabel(type, from, to)

    from.outgoingEdges[edgeLabel] = to
    to.incomingEdges[edgeLabel] = from

    return edgeLabel
}

/**
 * Adds a grouped edge into the graph.
 *
 * @param from origin of the edge
 * @param to target of the edge
 * @param group label of the attribute grouped by
 * @return a freshly created edge label
 */
fun addGroupedEdge(
        from: Node,
        to: Node,
        group: String): GroupedEdgeLabel {
    val edgeLabel = GroupedEdgeLabel(from, to, group)

    from.outgoingEdges[edgeLabel] = to
    to.incomingEdges[edgeLabel] = from

    return edgeLabel
}

fun label(relation: Relation): String {
    return relation.aliases.joinToString("")
}