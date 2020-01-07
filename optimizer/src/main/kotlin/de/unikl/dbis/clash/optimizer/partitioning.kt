package de.unikl.dbis.clash.optimizer

import de.unikl.dbis.clash.query.AttributeAccessList
import de.unikl.dbis.clash.query.RelationAlias

typealias PartitioningAttributesSelection = Map<List<RelationAlias>, AttributeAccessList>

fun noPartitioning() = mapOf<List<RelationAlias>, AttributeAccessList>()
