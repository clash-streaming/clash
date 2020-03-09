package de.unikl.dbis.clash.query.operations

import de.unikl.dbis.clash.query.BinaryPredicate
import de.unikl.dbis.clash.query.Relation
import de.unikl.dbis.clash.query.RelationAlias

/**
 * Joins Relations relA and relB with all applicable predicates.
 */
fun joinRelations(relA: Relation, relB: Relation, predicates: Collection<BinaryPredicate>, alias: RelationAlias = RelationAlias("${relA.alias}-${relB.alias}")): Relation {
    return Relation(
        relA.inputs + relB.inputs,
        relA.filters + relB.filters,
        relA.joinPredicates + relB.joinPredicates + predicates,
        relA.aggregations + relB.aggregations, // TODO: is this correct?
        relA.projections + relB.projections, // TODO: is this correct?
        alias,
        listOf() // TODO: delete me
    )
}
