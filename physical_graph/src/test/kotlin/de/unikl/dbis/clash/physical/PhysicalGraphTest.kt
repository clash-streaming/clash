package de.unikl.dbis.clash.physical

import org.junit.jupiter.api.Test

class PhysicalGraphTest {
    @Test
    fun `add single relation store`() {
        // TODO
//        val graph = PhysicalGraph()
//        val theStore = Store("x", emptySet(), relationOf("x"), 1)
//        graph.relationStores[relationOf("x")] = theStore
//        val expectedStore = Store("x", emptySet(), relationOf("x"), 1)

        // TODO assertThat(graph.relationStores).isEqualTo(mapOf("x" to listOf(expectedStore)))
    }

    @Test
    fun `add single relation producer`() {
        // TODO
//        val graph = PhysicalGraph()
//        graph.addRelationProducer(relationOf("x"), Store("x", emptySet(), relationOf("x"), 1))

        // TODO assertThat(graph.relationProducers).isEqualTo(mapOf("x" to listOf(Store("x", 1))))
    }

    @Test
    fun `add single relation consumer`() {
        val graph = PhysicalGraph()
        // graph.relationConsumers.getOrPut("x") { mutableSetOf() }.add(Store("x", 1))

        // TODO assertThat(graph.relationConsumers).isEqualTo(mapOf("x" to listOf(Store("x", 1))))
    }

    @Test
    fun `adding edge works`() {
        // TODO

//        val from = Store("x", emptySet(), relationOf("x"), 1)
//        val to = Store("y", emptySet(), relationOf("y"), 1)
//        val graph = PhysicalGraph()
//        val edgeLabel = addEdge(from, to, EdgeType.ALL)

        // TODO assertThat(from.outgoingEdges).isEqualTo(mapOf(edgeLabel to to))
        // TODO assertThat(from.incomingEdges).isEqualTo(mapOf(edgeLabel to from))
    }
}
