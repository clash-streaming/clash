package de.unikl.dbis.clash.physical

import org.json.JSONObject

/**
 * A PhysicalGraph's JSON representation is as follows:
 * {
 *   "nodes": [
 *     { "label": "r-spout", "parallelism": 1, "rules": [...] },
 *     { "label": "r", "parallelism": 5, "rules": [...], "partitionAttributes": ["x.a"], "relation": "..." },
 *     ...
 *   ],
 *   "edges": [
 *      { "from": "r-spout", "to": "r" },
 *      ...
 *   ]
 *
 * }
 */

fun PhysicalGraph.toJson(): JSONObject {
    fun niceLabel(node: Node): String {
        return when (node) {
            is Spout, is InputStub -> "${node.label}-spout"
            is PartitionedStore -> "${node.label}-store"
            else -> node.label
        }
    }

    fun addPartitioning(obj: JSONObject, node: Node) {
        when (node) {
            is PartitionedStore -> obj.put("partitioning", node.partitionAttributes)
        }
    }

    fun nodeType(node: Node): String {
        return when (node) {
            is Spout, is InputStub -> "Spout"
            is PartitionedStore -> "Store"
            is Sink, is OutputStub -> "Sink"
            else -> "UNKNOWN"
        }
    }

    val nodes: List<JSONObject> = this.streamNodes().map {
        val obj = JSONObject()
        obj.put("label", niceLabel(it))
        obj.put("parallelism", it.parallelism)
        addPartitioning(obj, it)
        obj.put("nodetype", nodeType(it))
        obj
    }
    val edges = this.streamEdges().map {
        val obj = JSONObject()
        obj.put("from", niceLabel(it.from))
        obj.put("to", niceLabel(it.to))
        obj.put("edgetype", it.edgeType.name)
        obj
    }

    val obj = JSONObject()
    obj.put("nodes", nodes)
    obj.put("edges", edges)
    return obj
}
