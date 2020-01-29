package de.unikl.dbis.clash.flexstorm

import java.io.Serializable

data class ProbeOrder(
    val entries: List<ProbeOrderEntry>
): Serializable

data class ProbeOrderEntry(
    val sendingAttribute: String,
    val targetStore: String,
    val probingAttribute: String
): Serializable

typealias LabelledProbeOrder = Pair<Int, ProbeOrder>

data class ProbeOrderCollection(
    val entries: Map<String, List<LabelledProbeOrder>>
) {
    /**
     * Returns the names of all (starting) relations.
     * If the set is constructed correctly,
     * the result contains each relation that is involved in a join.
     */
    fun relations() = entries.keys.distinct()

    /**
     * Returns all probing attributes for a certain relation R.
     * This means, that tuples arriving at the R-store could
     * probe based on these attributes and thus it might be beneficial
     * to create an index for those.
     */
    fun allProbingAttributesFor(relation: String): List<String> =
        entries.values
            .asSequence()
            .flatten()
            .map { it.second }
            .map { it.entries }
            .flatten()
            .filter { it.targetStore == relation }
            .map { it.probingAttribute }
            .distinct()
            .toList()
}

fun main() {
    val r1ProbeOrder = LabelledProbeOrder(1, ProbeOrder(listOf(
        ProbeOrderEntry("b", "S", "c"),
        ProbeOrderEntry("d", "T", "e")
    )))
    val r2ProbeOrder = LabelledProbeOrder(2, ProbeOrder(listOf(
        ProbeOrderEntry("d", "T", "e"),
        ProbeOrderEntry("b", "S", "c")
    )))
    val s1ProbeOrder = LabelledProbeOrder(3, ProbeOrder(listOf(
        ProbeOrderEntry("d", "T", "e"),
        ProbeOrderEntry("c", "R", "b")
    )))
    val s2ProbeOrder = LabelledProbeOrder(4, ProbeOrder(listOf(
        ProbeOrderEntry("c", "R", "b"),
        ProbeOrderEntry("d", "T", "e")
    )))
    val t1ProbeOrder = LabelledProbeOrder(5, ProbeOrder(listOf(
        ProbeOrderEntry("e", "S", "d"),
        ProbeOrderEntry("c", "R", "t")
    )))
    val t2ProbeOrder = LabelledProbeOrder(6, ProbeOrder(listOf(
        ProbeOrderEntry("c", "R", "a"),
        ProbeOrderEntry("e", "S", "d")
    )))

    val pc = ProbeOrderCollection(
        mapOf("R" to listOf(r1ProbeOrder, r2ProbeOrder),
            "S" to listOf(s1ProbeOrder, s2ProbeOrder),
            "T" to listOf(t1ProbeOrder, t2ProbeOrder))
    )

    println("Tada.")
    println("R: ${pc.allProbingAttributesFor("R")}")
    println("S: ${pc.allProbingAttributesFor("S")}")
    println("T: ${pc.allProbingAttributesFor("T")}")

}
