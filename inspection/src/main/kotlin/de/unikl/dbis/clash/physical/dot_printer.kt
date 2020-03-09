package de.unikl.dbis.clash.physical

object DotPrinter {

    /**
     * Prints this topology graph as a DOT string.
     * This can then be used by, e.g., graphviz in order to render the graph.
     */
    fun toDotString(graph: PhysicalGraph): String {
        val sb = StringBuilder()
        sb.append("digraph G {\n")
        graph.streamNodes().forEach { x -> toDotLine(sb, x) }
        sb.append("}\n")
        return sb.toString()
    }
// TODO
//    /**
//     * Prints this topology graph as a DOT string.
//     * This can then be used by, e.g., graphviz in order to render the graph.
//     */
//    fun toDotString(graph: StormPhysicalGraph): String {
//        val sb = StringBuilder()
//        sb.append("digraph G {\n")
//        graph.streamNodes().forEach { x -> toDotLine(sb, x) }
//        sb.append("}\n")
//        return sb.toString()
//    }

    internal fun toDotLine(sb: StringBuilder, node: Node) {
        when (node) {
            is Controller -> toDotLine(sb, node)
            is Dispatcher -> toDotLine(sb, node)
            is InputStub -> toDotLine(sb, node)
            is OutputStub -> toDotLine(sb, node)
            is Reporter -> toDotLine(sb, node)
            is Sink -> toDotLine(sb, node)
            is Spout -> toDotLine(sb, node)
            is PartitionedStore -> toDotLine(sb, node)
            else -> throw RuntimeException("Cannot produce DotLine since I don't know the type of $node")
        }
    }

    internal fun toDotLine(sb: StringBuilder, controller: Controller) {
        for ((key, value) in controller.outgoingEdges) {
            toDotLine(sb, controller.label, key, value.label)
        }
    }

    internal fun toDotLine(sb: StringBuilder, dispatcher: Dispatcher) {
        for ((key, value) in dispatcher.outgoingEdges) {
            toDotLine(sb, dispatcher.label, key, value.label)
        }
    }

    internal fun toDotLine(sb: StringBuilder, inputStub: InputStub) {
        for ((key, value) in inputStub.outgoingEdges) {
            val fromLabel = inputStub.label + "_INPUT_STUB"
            val toLabel = value.label
            toDotLine(sb, fromLabel, key, toLabel)
        }
    }

    internal fun toDotLine(sb: StringBuilder, outputStub: OutputStub) {
        for ((key, value) in outputStub.outgoingEdges) {
            val fromLabel = outputStub.label + "_OUTPUT_STUB"
            val toLabel = value.label
            toDotLine(sb, fromLabel, key, toLabel)
        }
    }

    internal fun toDotLine(sb: StringBuilder, reporter: Reporter) {
        for ((key, value) in reporter.outgoingEdges) {
            toDotLine(sb, reporter.label, key, value.label)
        }
    }

    internal fun toDotLine(sb: StringBuilder, sink: Sink) {
        for ((key, value) in sink.outgoingEdges) {
            toDotLine(sb, sink.label, key, value.label)
        }
    }

    internal fun toDotLine(sb: StringBuilder, spout: Spout) {
        for ((key, value) in spout.outgoingEdges) {
            toDotLine(sb, spout.label, key, value.label)
        }
    }

    internal fun toDotLine(sb: StringBuilder, store: PartitionedStore) {
        createNodeWithOptions(sb, store.label, store.rules)
        for ((key, value) in store.outgoingEdges) {
            toDotLine(sb, store.label, key, value.label)
        }
    }

    internal fun toDotLine(sb: StringBuilder, fromLabel: String, label: EdgeLabel, toLabel: String) {
        sb.append("    \"")
        sb.append(fromLabel)
        sb.append("\" -> \"")
        sb.append(toLabel)

        sb.append("\"[label=\"")
        val key = label.label + "_" + label.edgeType.name
        sb.append(key)
        sb.append("\"];\n")
    }

    internal fun createNodeWithOptions(sb: StringBuilder, nodeLabel: String, rules: List<Rule>) {
        val dotInRules = rules.filterIsInstance<InRule>().map { it.dotLabel() }.joinToString("\\n")
        val dotOutRules = rules.filterIsInstance<OutRule>().map { it.dotLabel() }.joinToString("\\n")
        val dotLabel = "{$nodeLabel | $dotInRules | $dotOutRules}"
        sb.append("$nodeLabel [shape=record, label=\"$dotLabel\" ]\n")
    }
}

// TODO: IntermediateJoinRule

fun InRule.dotLabel(): String {
    val edgeLabel = this.incomingEdgeLabel.label
    val rest = when (this) {
        is ReportInRule -> this.dotLabel()
        is ControlInRule -> this.dotLabel()
        is TickInRule -> this.dotLabel()
        is RelationReceiveRule -> this.dotLabel()
        is JoinResultRule -> this.dotLabel()
        is StoreAndJoinRule -> this.dotLabel()
        else -> "InRule"
    }
    return "From $edgeLabel: $rest"
}

fun OutRule.dotLabel(): String {
    return when (this) {
        is ReportOutRule -> this.dotLabel()
        is ControlOutRule -> this.dotLabel()
        is TickOutRule -> this.dotLabel()
        is RelationSendRule -> this.dotLabel()
        else -> "OutRule"
    }
}

fun ReportInRule.dotLabel(): String {
    return "ReportInRule"
}

fun ReportOutRule.dotLabel(): String {
    return "ReportInRule"
}

fun ControlInRule.dotLabel(): String {
    return "ControlInRule"
}

fun ControlOutRule.dotLabel(): String {
    return "ControlOutRule"
}

fun TickInRule.dotLabel(): String {
    return "TickInRule"
}

fun TickOutRule.dotLabel(): String {
    return "TickOutRule"
}

fun RelationReceiveRule.dotLabel(): String {
    val rel = this.relation.inputAliases.joinToString("+")
    return "Receive: $rel"
}

fun RelationSendRule.dotLabel(): String {
    return "RelationSendRule"
}

fun JoinResultRule.dotLabel(): String {
    this.incomingEdgeLabel.label
    val pred = this.predicates.joinToString("∧")
    return "Join predicate $pred and take as result"
}

fun IntermediateJoinRule.dotLabel(): String {
    return "OldIntermediateJoinRule"
}

fun StoreAndJoinRule.dotLabel(): String {
    return "StoreAndJoinRule"
}
