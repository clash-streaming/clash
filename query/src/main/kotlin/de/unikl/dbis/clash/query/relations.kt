package de.unikl.dbis.clash.query

import de.unikl.dbis.clash.support.times
import java.io.Serializable

data class Relation(
    val inputs: Map<RelationAlias, WindowDefinition>,
    val filters: Collection<UnaryPredicate>,
    val joinPredicates: Collection<BinaryPredicate>,
    val aggregations: Collection<Aggregation>,
    val projections: Collection<Projection>,
    val alias: RelationAlias,
    @Deprecated("Use filters or join predicates") val predicates: Collection<Predicate>
) : Serializable {
    val inputAliases: Collection<RelationAlias> get() = inputs.keys
    @Deprecated("Use filters or join predicates") val unaryPredicates: Collection<UnaryPredicate> get() = predicates.filterIsInstance<UnaryPredicate>()
    @Deprecated("Use filters or join predicates") val binaryPredicates: Collection<BinaryPredicate> get() = predicates.filterIsInstance<BinaryPredicate>()

    /**
     * Creates a new relation as restriction of this one to the given aliases.
     * The output will have all according windows and predicates.
     */
    fun subRelation(vararg relationAliases: RelationAlias, alias: RelationAlias = RelationAlias(relationAliases.joinToString(","))): Relation {
        // val newWindows = inputs.filter { relationAliases.contains(it.key) }
        // val newPredicates: Collection<Predicate> = unaryPredicates.filter { relationAliases.contains(it.relationAlias) } +
        //         binaryPredicates.filter { relationAliases.contains(it.leftRelationAlias) && relationAliases.contains(it.rightRelationAlias) }
        // val newAttributeAccesses = extractAttributeAccesses(unaryPredicates + binaryPredicates)
        // return Relation(newWindows, newPredicates, newAttributeAccesses)
        return Relation(
            inputs.filter { relationAliases.contains(it.key) },
            filters.filter { relationAliases.contains(it.relationAlias) },
            joinPredicates.filter { relationAliases.contains(it.leftRelationAlias) && relationAliases.contains(it.rightRelationAlias) },
            listOf(), // TODO: Think about that
            projections.filter { relationAliases.contains(it.attributeAccess.relationAlias) },
            alias,
            listOf() // TODO delete me
        )
    }

    /**
     * Creates all base relations from this relation, i.e., the relations that
     * consist only of a single window definition and unary predicates with the according relationAlias.
     */
    fun baseRelations(): Collection<Relation> = inputs.map {
        Relation(
            mapOf(it.key to it.value),
            filters.filter { predicate -> predicate.relationAlias == it.key },
            listOf(),
            aggregations.filter { aggregation -> aggregation.attributeAccess.relationAlias == it.key },
            projections.filter { projection -> projection.attributeAccess.relationAlias == it.key },
            it.key,
            listOf() // TODO delete me
        )
    }

    /**
     * Creates a new relation composed of the join of this relation and the passed relations.
     * Additionally all predicates that are valid join predicates (i.e., where one side has an relationAlias
     * of this relation and the other side as an relationAlias to the passed relations) are added.
     */
    @Deprecated("use joinRelations instead")
    fun join(predicates: Collection<Predicate>, vararg relations: Relation): Relation {
        TODO()
    }

    // TODO adjust to new data structure
    override fun toString(): String {
        val windowPart = inputs.map { "${it.key.inner}${it.value}" }.joinToString(", ", "{", "}")
        val predicatePart = predicates.joinToString(", ", "{", "}")
        return "<$windowPart, $predicatePart>"
    }

    override fun equals(other: Any?): Boolean = other is Relation &&
        inputs == other.inputs &&
        filters == other.filters &&
        joinPredicates == other.joinPredicates &&
        aggregations == other.aggregations &&
        projections == other.projections
}

fun isCrossProductAlias(binaryPredicates: Collection<BinaryPredicate>, relationAliases: Collection<RelationAlias>, relationAlias: RelationAlias): Boolean {
    return isCrossProductAlias(binaryPredicates, relationAliases, listOf(relationAlias))
}

fun isCrossProductAlias(binaryPredicates: Collection<BinaryPredicate>, relationAliases1: Collection<RelationAlias>, relationAliases2: Collection<RelationAlias>): Boolean {
    return binaryPredicates.none {
        predicate -> relationAliases1.times(relationAliases2).any {
        predicate.leftRelationAlias == it.first && predicate.rightRelationAlias == it.second ||
                predicate.rightRelationAlias == it.first && predicate.leftRelationAlias == it.second
    }
    }
}

fun isCrossProduct(binaryPredicates: Collection<BinaryPredicate>, relations: Collection<Relation>, relation: Relation): Boolean {
    return isCrossProduct(binaryPredicates, relations, listOf(relation))
}

fun isCrossProduct(binaryPredicates: Collection<BinaryPredicate>, relations1: Collection<Relation>, relations2: Collection<Relation>): Boolean {
    val relationAliases1 = relations1.flatMap { it.inputAliases }
    val relationAliases2 = relations2.flatMap { it.inputAliases }
    return isCrossProductAlias(binaryPredicates, relationAliases1, relationAliases2)
}

data class Aggregation(val attributeAccess: AttributeAccess, val alias: String)
