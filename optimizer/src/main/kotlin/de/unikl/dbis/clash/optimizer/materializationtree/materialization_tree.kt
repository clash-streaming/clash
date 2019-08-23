package de.unikl.dbis.clash.optimizer.materializationtree

import de.unikl.dbis.clash.datacharacteristics.DataCharacteristics
import de.unikl.dbis.clash.estimator.estimateSize
import de.unikl.dbis.clash.optimizer.probeorder.ProbeOrderOptimizationStrategy
import de.unikl.dbis.clash.optimizer.probeorder.ProbeOrders
import de.unikl.dbis.clash.query.AttributeAccessList
import de.unikl.dbis.clash.query.BinaryPredicate
import de.unikl.dbis.clash.query.Relation
import de.unikl.dbis.clash.query.WindowDefinition
import de.unikl.dbis.clash.support.ceil
import java.util.*
import java.util.function.Consumer


/**
 * The MaterializationTree represents a query plan in form of a tree where the inner nodes denote
 * which relations are stored.
 *
 * The nodes of the tree contain information about the operator they represent:
 *
 * - to which relation do the tuples stored there belong?
 * -
 * - how many parallel instances of the operator are deployed?
 * - according to which attributes are the tuples grouped?
 *
 */
data class MaterializationTree(val root: MtNode) {
    /**
     * iterate through the nodes of this tree in a depth first fashion
     */
    fun walkNodes(): Iterator<MtNode> {
        return DepthFirstQueryIterable(root)
    }

    class DepthFirstQueryIterable internal constructor(root: MtNode) : Iterable<MtNode>, Iterator<MtNode> {

        private val nodes = ArrayDeque<MtNode>()

        init {
            postOrder(root)
        }

        private fun postOrder(node: MtNode) {
            node.children.forEach(Consumer<MtNode> { this.postOrder(it) })
            this.nodes.add(node)
        }

        override fun iterator(): Iterator<MtNode> = this

        override fun hasNext(): Boolean = !this.nodes.isEmpty()

        override fun next(): MtNode = this.nodes.remove()
    }
}

interface MtNode {
    val relation: Relation
    val children: List<MtNode>
    val parallelism: Long
    var partitioning: AttributeAccessList
    val storageCost: Double
}

interface MultiStream {
    val probeOrders: ProbeOrders
    val probeCost: Double
}

data class MultiStreamImpl(
        override val probeOrders: ProbeOrders,
        override val probeCost: Double
): MultiStream

data class MatMultiStream(
        override val relation: Relation,
        override val children: List<MtNode>,
        override val parallelism: Long,
        override var partitioning: AttributeAccessList,
        override val storageCost: Double,
        val multiStreamImpl: MultiStreamImpl
): MtNode, MultiStream by multiStreamImpl

data class NonMatMultiStream(
        override val relation: Relation,
        override val children: List<MtNode>,
        val multiStreamImpl: MultiStreamImpl
): MtNode, MultiStream by multiStreamImpl {
    override val parallelism: Long = 0
    override var partitioning: AttributeAccessList = listOf()
    override val storageCost: Double = 0.0
}

data class MatSource(
        override val relation: Relation,
        override val parallelism: Long,
        override var partitioning: AttributeAccessList,
        override val storageCost: Double
): MtNode {
    override val children: List<MtNode> = listOf()
}

fun parallelismFor(relation: Relation, dataCharacteristics: DataCharacteristics, taskCapacity: Long): Long {
    return (storageCostFor(relation, dataCharacteristics) / taskCapacity).ceil()
}

fun storageCostFor(relation: Relation, dataCharacteristics: DataCharacteristics): Double {
    val size = estimateSize(relation, dataCharacteristics)
    val multiplier = relation.windowDefinition.values.minBy { if(it.variant == WindowDefinition.Variant.None) Long.MAX_VALUE else it.amount  }!!.amount

    return if(multiplier == 0L) size else size * multiplier
}

fun createMultiStreamImpl(children: Collection<MtNode>,
                          predicates: Collection<BinaryPredicate>,
                          probeOrderOptimizationStrategy: ProbeOrderOptimizationStrategy,
                          dataCharacteristics: DataCharacteristics): MultiStreamImpl {
    val probeOrders = probeOrderOptimizationStrategy.optimize(dataCharacteristics, predicates, children)
    return MultiStreamImpl(probeOrders.first, probeOrders.second)
}

fun MaterializationTree.printParenthesis() {
    fun MtNode.ppstr(): String {
        return if(children.isEmpty()) {
            relation.aliases.joinToString(",")
        } else {
            this.children.joinToString(",", "(", ")") { it.ppstr() }
        }
    }

    println(root.ppstr())
}

