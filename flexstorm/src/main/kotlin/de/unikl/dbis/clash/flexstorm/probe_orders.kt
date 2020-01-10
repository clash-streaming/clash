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
