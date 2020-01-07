package de.unikl.dbis.clash.physical

class MTNodeTest {
    // TODO
//    val nodeFrom: Node = Store("myOrigin", emptySet(), relationOf("myOrigin"), 1)
//    val nodeTo: Node = Store("myTarget", emptySet(), relationOf("myTarget"), 1)
//    val edge: EdgeLabel = EdgeLabel(EdgeType.ALL, nodeFrom, nodeTo)
//
//    init {
//        nodeFrom.outgoingEdges[edge] = nodeTo
//        nodeTo.incomingEdges[edge] = nodeFrom
//    }

    /*
    //Actually, this code produced a BUG
    class TestNode extends MTNodeOld {
        TestNode(@NotNull String label, int parallelism) {
            super(label, parallelism)
        }
    }

    class TestInRule implements InRule {
        def label
        TestInRule(EdgeLabel label) {
            this.label = label
        }
        @Override
        EdgeLabel getIncomingEdgeLabel() {
            return label
        }
    }

    class TestOutRule implements OutRule {
        def label
        TestOutRule(EdgeLabel label) {
            this.label = label
        }
        @Override
        EdgeLabel getOutgoingEdgeLabel() {
            return label
        }
    }*/
//
//    MTNodeOld nodeFrom
//            MTNodeOld nodeTo
//            EdgeLabel edge
//
//            def setup() {
//        nodeFrom = new Store("myOrigin", 1)
//        nodeTo = new Store("myTarget", 1)
//        edge = new EdgeLabel(EdgeType.ALL, nodeFrom, nodeTo)
//        nodeFrom.addOutgoingEdge(edge, nodeTo)
//        nodeTo.addIncomingEdge(edge, nodeFrom)

//
//    @Test
//    fun `addRule for given inRule works correctly`() {
//        val rule = RelationReceiveRule(relationOf("x"), edge)
//
//        // no exception is thrown
//        assertThat(nodeTo.addRule(rule))
//    }
//
//    @Test
//    fun `addRule for given outRule works correctly`() {
//        val rule = RelationSendRule(relationOf("x"), edge)
//
//        // no exception is thrown
//        assertThat(nodeFrom.addRule(rule))
//    }
}
