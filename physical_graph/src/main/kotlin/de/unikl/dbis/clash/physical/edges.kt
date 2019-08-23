package de.unikl.dbis.clash.physical

import java.io.Serializable


open class EdgeLabel
/**
 * Generates a new EdgeLabel.
 * @param edgeType  The type that corresponds to the distribution method
 * @param from      The node this edge originates
 * @param to        The node this edge arrives
 */
(val edgeType: EdgeType,
 val from: Node,
 val to: Node) : Serializable {
    val label: String

    init {
        this.label = "s_" + EdgeLabel.globalCounter++
    }


    override fun equals(other: Any?): Boolean {
        return (other != null
                && other is EdgeLabel
                && other.edgeType == this.edgeType
                && other.from == this.from
                && other.to == this.to
                && other.label == this.label)
    }

    override fun hashCode(): Int {
        return (this.label.hashCode()
                xor this.from.hashCode()
                xor this.to.hashCode()
                xor this.edgeType.hashCode())
    }

    override fun toString(): String {
        return String.format("%s (%s->%s, %s)",
                this.label,
                this.from.label,
                this.to.label,
                this.edgeType)
    }

    companion object {
        private var globalCounter = 0
    }
}


class GroupedEdgeLabel
/**
 * Generates a new GroupedEdgeLabel.
 *
 * @param group The attribute according to which the stream is grouped
 * @param from The node this edge originates
 * @param to The node this edge arrives
 */
(
        from: Node,
        to: Node,
        val group: String) : EdgeLabel(EdgeType.GROUP_BY, from, to) {

    override fun equals(other: Any?): Boolean {
        return (super.equals(other)
                && other is GroupedEdgeLabel
                && other.group == this.group)
    }

    override fun hashCode(): Int {
        return super.hashCode() xor this.group.hashCode()
    }

    override fun toString(): String {
        return String.format("%s (%s->%s, %s[%s])",
                this.label,
                super.from.label,
                this.to.label,
                this.edgeType,
                this.group)
    }
}


/**
 * Created by manuel on 08.11.16.
 */
enum class EdgeType {
    ALL,
    SHUFFLE,
    GROUP_BY
}
