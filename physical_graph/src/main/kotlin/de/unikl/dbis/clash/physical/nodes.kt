package de.unikl.dbis.clash.physical

import de.unikl.dbis.clash.query.AttributeAccessList
import de.unikl.dbis.clash.query.Relation
import java.io.Serializable
import org.slf4j.LoggerFactory

/**
 * Core abstraction for Nodes in the Physical Graph. Every other node type descends of this one.
 */
interface Node : Serializable {
    val label: String
    val parallelism: Long
    val rules: MutableList<Rule>
    val incomingEdges: MutableMap<EdgeLabel, Node>
    val outgoingEdges: MutableMap<EdgeLabel, Node>

    fun addRule(rule: Rule)

    fun replaceEdge(oldEdge: EdgeLabel, newEdge: EdgeLabel) {
        replaceIncomingEdge(oldEdge, newEdge)
        replaceOutgoingEdge(oldEdge, newEdge)
    }

    fun replaceIncomingEdge(oldEdge: EdgeLabel, newEdge: EdgeLabel)
    fun replaceOutgoingEdge(oldEdge: EdgeLabel, newEdge: EdgeLabel)

    companion object {
        val LOG = LoggerFactory.getLogger(Node::class.java)!!
    }
}

data class Controller(override val label: String) : Node by CommonNode(label, 1)
data class ControllerInput(override val label: String) : Node by CommonNode(label, 1)
data class Dispatcher(override val label: String, override val parallelism: Long) : Node by CommonNode(label, parallelism)
data class Reporter(override val label: String, override val parallelism: Long) : Node by CommonNode(label, parallelism)
data class TickSpout(override val label: String) : Node by CommonNode(label, 1)

data class Spout(override val label: String, val relation: Relation, override val parallelism: Long) : Node by CommonNode(label, parallelism)
data class Sink(
    override val label: String,
    val relation: Relation,
    override val parallelism: Long
) : Node by CommonNode(label, parallelism)

interface Store : Node {
    val relation: Relation
}

data class PartitionedStore(
    override val label: String,
    val partitionAttributes: AttributeAccessList,
    override val relation: Relation,
    override val parallelism: Long
) : Store, Node by CommonNode(label, parallelism)

data class InputStub(override val label: String, val relation: Relation) : Node by CommonNode(label, 1)
data class OutputStub(
    override val label: String,
    val relation: Relation
) : Node by CommonNode(label, 1)
data class ThetaStore(
    override val label: String,
    override val relation: Relation,
    override val parallelism: Long
) : Store, Node by CommonNode(label, parallelism)
data class SimilarityStore(
    override val label: String,
    override val relation: Relation,
    override val parallelism: Long
) : Store, Node by CommonNode(label, parallelism)
data class AggregationStore(
    override val label: String,
    override val relation: Relation,
    override val parallelism: Long
) : Store, Node by CommonNode(label, parallelism)
data class SelectProjectNode(
    override val label: String,
    val relation: Relation,
    override val parallelism: Long
) : Node by CommonNode(label, parallelism)

class CommonNode(
    override val label: String,
    override val parallelism: Long,
    override val rules: MutableList<Rule> = mutableListOf(),
    override val incomingEdges: MutableMap<EdgeLabel, Node> = mutableMapOf(),
    override val outgoingEdges: MutableMap<EdgeLabel, Node> = mutableMapOf()
) : Node {

    /**
     * Adds a rule to this node.
     * Thereby checks if this rule is valid.
     * @param rule The rule to be added
     */
    override fun addRule(rule: Rule) {
        Node.LOG.info("Adding to " + this.label + " the following rule: " + rule)

        val valid = incomingEdges
                .keys
                .asSequence()
                .plus(outgoingEdges.keys)
                .any {
                    when (rule) {
                        is InRule -> rule.incomingEdgeLabel == it
                        is OutRule -> rule.outgoingEdgeLabel == it
                        else -> { throw java.lang.RuntimeException("trying to add a Rule that is neither InRule nor OutRule") }
                    }
                }

        if (!valid) {
            throw RuntimeException("Trying to add a Rule that does not belong to any edge." + "Did you forget to call addOutgoingEdge or addIncomingEdge on the sender/receiver?")
        }
        this.rules.add(rule)
    }

    override fun replaceIncomingEdge(oldEdge: EdgeLabel, newEdge: EdgeLabel) {
        if (!this.incomingEdges.containsKey(oldEdge)) {
            Node.LOG.info("Edge " + oldEdge.label + " not available for replacement")
            return
        }

        // replace in incoming edges
        this.incomingEdges[newEdge] = this.incomingEdges[oldEdge]!!
        this.incomingEdges.remove(oldEdge)

        // replace in rules
        val ruleListIterator = this.rules.listIterator()
        while (ruleListIterator.hasNext()) {
            val rule = ruleListIterator.next()
            if (rule is InRule && rule.incomingEdgeLabel == oldEdge) {
                val newRule = rule.replaceIncomingEdgeLabel(newEdge)
                ruleListIterator.set(newRule)
            }
        }
    }

    override fun replaceOutgoingEdge(oldEdge: EdgeLabel, newEdge: EdgeLabel) {
        if (!this.outgoingEdges.containsKey(oldEdge)) {
            Node.LOG.info("Edge " + oldEdge.label + " not available for replacement")
            return
        }

        // replace in incoming edges
        this.outgoingEdges[newEdge] = this.outgoingEdges[oldEdge]!!
        this.outgoingEdges.remove(oldEdge)

        // replace in rules
        val ruleListIterator = this.rules.listIterator()
        while (ruleListIterator.hasNext()) {
            val rule = ruleListIterator.next()
            if (rule is OutRule && rule.outgoingEdgeLabel == oldEdge) {
                val newRule = rule.replaceOutgoingEdgeLabel(newEdge)
                ruleListIterator.set(newRule)
            }
        }
    }
}
