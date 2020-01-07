package de.unikl.dbis.clash.optimizer.materializationtree

import de.unikl.dbis.clash.optimizer.probeorder.ProbeOrders
import de.unikl.dbis.clash.query.AttributeAccessList
import org.json.JSONObject

fun MaterializationTree.toJson(): JSONObject {
    return root.toJson()
}

/**
 * Returns an object for each type of MtNode.
 *
 * A NonMatMultiStream looks like this:
 *
 * {
 *  type: "NonMatMultiStream",
 *  relation: "(r[10], s[50], t[50]), ([...])",
 *  children: [ ... ],
 *  probeOrders: [[...], [...], ...],
 *  pCost: 100.0
 * }
 *
 * A MatMultiStream like this:
 *
 * {
 *   type: "MatMultiStream",
 *   relation: "(r[10], s[50], t[50]), ([...])",
 *   children: [ ... ],
 *   probeOrders: [[...], [...], ...],
 *   parallelism: 5,
 *   partitioning: ["r.a", "s.b"],
 *   sCost: 100.0,
 *   pCost: 50.0
 * }
 *
 * A MatSource like this:
 *
 * {
 *   type: "MatSource",
 *   relation: "(r[10]), ([...])",
 *   parallelism: 5,
 *   partitioning: ["r.a"]
 * }
 */
fun MtNode.toJson(): JSONObject {
    val result = JSONObject()

    return when (this) {
        is NonMatMultiStream -> nonMatMultiStreamToJson(this)
        is MatMultiStream -> matMultiStreamToJson(this)
        is MatSource -> matSourceToJson(this)
        else -> TODO("I am missing a toJson method for ${this.javaClass.name}")
    }
}

fun nonMatMultiStreamToJson(nonMatMultiStream: NonMatMultiStream): JSONObject {
    val result = JSONObject()
    result.put("type", "NonMatMultiStream")
    result.put("relation", nonMatMultiStream.relation)
    result.put("children", nonMatMultiStream.children.map { it.toJson() })
    result.put("probeOrder", probeOrderToJson(nonMatMultiStream.probeOrders))
    result.put("probeCost", nonMatMultiStream.multiStreamImpl.probeCost)
    return result
}

fun matMultiStreamToJson(matMultiStream: MatMultiStream): JSONObject {
    val result = JSONObject()
    result.put("type", "MatMultiStream")
    result.put("relation", matMultiStream.relation)
    result.put("children", matMultiStream.children.map { it.toJson() })
    result.put("probeOrder", probeOrderToJson(matMultiStream.probeOrders))
    result.put("parallelism", matMultiStream.parallelism)
    result.put("partitioning", partitioningToJson(matMultiStream.partitioning))
    result.put("storageCost", matMultiStream.storageCost)
    result.put("probeCost", matMultiStream.multiStreamImpl.probeCost)
    return result
}

fun matSourceToJson(matSource: MatSource): JSONObject {
    val result = JSONObject()
    result.put("type", "MatSource")
    result.put("relation", matSource.relation)
    result.put("parallelism", matSource.parallelism)
    result.put("partitioning", partitioningToJson(matSource.partitioning))
    result.put("storageCost", matSource.storageCost)
    return result
}

fun probeOrderToJson(probeOrders: ProbeOrders): Array<Array<String>> {
    return probeOrders.inner.values.map { it.first.steps.map { step -> step.first.relation.aliases.joinToString("-") }.toTypedArray() }.toTypedArray()
}

fun partitioningToJson(list: AttributeAccessList): String {
    return list.map { "${it.relationAlias.inner}.${it.attribute}" }.joinToString(", ")
}
