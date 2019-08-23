package de.unikl.dbis.clash.query.operations

import de.unikl.dbis.clash.query.BinaryPredicate
import de.unikl.dbis.clash.query.Relation
import de.unikl.dbis.clash.query.extractAttributeAccesses

/**
 * Joins Relations relA and relB with all applicable predicates.
 */
fun joinRelations(relA: Relation, relB: Relation, predicates: Collection<BinaryPredicate>): Relation {
    val windowedRelations = relA.windowDefinition + relB.windowDefinition
    val predicates = relA.predicates + relB.predicates
    return Relation(windowedRelations, predicates, extractAttributeAccesses(predicates))
}