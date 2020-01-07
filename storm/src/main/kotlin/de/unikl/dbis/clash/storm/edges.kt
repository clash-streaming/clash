package de.unikl.dbis.clash.storm

import de.unikl.dbis.clash.physical.EdgeLabel
import de.unikl.dbis.clash.physical.EdgeType
import de.unikl.dbis.clash.physical.GroupedEdgeLabel
import java.io.Serializable

class StormEdgeLabel : Serializable {

    val streamName: String
    val groupingAttribute: String
    internal var stormEdgeType: StormEdgeType

    val isGrouping: Boolean
        get() = this.stormEdgeType.isGrouping

    constructor(original: EdgeLabel) {
        this.streamName = original.label
        this.stormEdgeType = StormEdgeType(original.edgeType)
        if (original is GroupedEdgeLabel) {
            this.groupingAttribute = original.group
        } else {
            this.groupingAttribute = ""
        }
    }

    constructor(streamName: String, groupingAttribute: String, stormEdgeType: StormEdgeType) {
        this.streamName = streamName
        this.groupingAttribute = groupingAttribute
        this.stormEdgeType = stormEdgeType
    }

    override fun equals(other: Any?): Boolean {
        return (other is StormEdgeLabel &&
                other.streamName == this.streamName &&
                other.groupingAttribute == this.groupingAttribute &&
                other.stormEdgeType == this.stormEdgeType)
    }

    override fun hashCode(): Int {
        return (this.streamName.hashCode()
                xor this.groupingAttribute.hashCode()
                xor this.stormEdgeType.hashCode())
    }

    override fun toString(): String {
        return this.streamName + "[" + this.groupingAttribute + "][" + this.stormEdgeType + "]"
    }
}

class StormEdgeType : Serializable {

    internal var isAll = false
    internal var isShuffle = false
    internal var isGrouping = false

    constructor(edgeType: EdgeType) {
        if (edgeType == EdgeType.ALL) {
            this.isAll = true
        }
        if (edgeType == EdgeType.SHUFFLE) {
            this.isShuffle = true
        }
        if (edgeType == EdgeType.GROUP_BY) {
            this.isGrouping = true
        }
    }

    private constructor(isAll: Boolean, isShuffle: Boolean, isGrouping: Boolean) {
        this.isAll = isAll
        this.isShuffle = isShuffle
        this.isGrouping = isGrouping
    }

    override fun equals(other: Any?): Boolean {
        return (other is StormEdgeType &&
                other.isAll == this.isAll &&
                other.isShuffle == this.isShuffle &&
                other.isGrouping == this.isGrouping)
    }

    override fun hashCode(): Int {
        return (java.lang.Boolean.hashCode(this.isAll)
                xor java.lang.Boolean.hashCode(this.isGrouping)
                xor java.lang.Boolean.hashCode(this.isShuffle))
    }

    override fun toString(): String {
        return when {
            this.isGrouping -> "GROUPING"
            this.isShuffle -> "SHUFFLE"
            else -> // this.isAll
                "ALL"
        }
    }

    companion object {

        fun all(): StormEdgeType {
            return StormEdgeType(true, false, false)
        }

        fun shuffle(): StormEdgeType {
            return StormEdgeType(false, true, false)
        }

        fun grouping(): StormEdgeType {
            return StormEdgeType(false, false, true)
        }
    }
}
