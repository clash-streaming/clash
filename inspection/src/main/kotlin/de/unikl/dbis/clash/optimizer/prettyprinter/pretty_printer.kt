package de.unikl.dbis.clash.optimizer.prettyprinter

import de.unikl.dbis.clash.datacharacteristics.DataCharacteristics
import de.unikl.dbis.clash.optimizer.materializationtree.MaterializationTree
import de.unikl.dbis.clash.optimizer.materializationtree.MtNode
import de.unikl.dbis.clash.optimizer.materializationtree.MultiStream
import de.unikl.dbis.clash.optimizer.probeTuplesSentForProbeOrder

fun MaterializationTree.prettyPrint() {
    root.prettyPrint()
}

fun MtNode.prettyPrint(indent: Int = 0) {
    val indentation = " ".repeat(indent)
    println("$indentation MTNode ${this.relation}:")

    if (children.isNotEmpty()) {
        println("$indentation - Children:")
        children.forEach { it.prettyPrint(indent + 2) }
    }

    if (this is MultiStream) {
        println("$indentation - Probe orders:")
        probeOrders.inner.forEach {
            val orderString = it.value.first.steps.joinToString(", ") { it.first.relation.toString() }
            println("$indentation ${it.key.relation}: $orderString")
        }
    }
}

fun MtNode.prettyPrintProbeTuplesSentByStore(dataCharacteristics: DataCharacteristics) {
    TODO()
//    val stats = probeTuplesSentByStore(dataCharacteristics, this)
//    stats.forEach { println("${it.key}: ${it.value}")}
}

fun MtNode.prettyPrintProbeTuplesSentForProbeOrder(dataCharacteristics: DataCharacteristics) {
    if (this is MultiStream) {
        this.probeOrders.inner.forEach {
            val stats = probeTuplesSentForProbeOrder(dataCharacteristics, it.value.first)
//            val elems = it.value.steps.map { "${it.value.first.relation}[${stats[it.first]}]" }.joinToString(", ")
//            println("${it.key.name}: $elems")
        }
    }
}
