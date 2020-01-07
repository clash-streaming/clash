package de.unikl.dbis.clash.query

import de.unikl.dbis.clash.support.times
import java.io.Serializable

data class Relation(
    val windowDefinition: Map<RelationAlias, WindowDefinition>,
    val predicates: Collection<Predicate>,
    val attributeAccesses: Collection<AttributeAccess> // TODO
) : Serializable {
    val aliases: Collection<RelationAlias> get() = windowDefinition.keys
    val unaryPredicates: Collection<UnaryPredicate> get() = predicates.filterIsInstance<UnaryPredicate>()
    val binaryPredicates: Collection<BinaryPredicate> get() = predicates.filterIsInstance<BinaryPredicate>()

    /**
     * Creates a new relation as restriction of this one to the given aliases.
     * The output will have all according windows and predicates.
     */
    fun subRelation(vararg relationAliases: RelationAlias): Relation {
        val newWindows = windowDefinition.filter { relationAliases.contains(it.key) }
        val newPredicates: Collection<Predicate> = unaryPredicates.filter { relationAliases.contains(it.relationAlias) } +
                binaryPredicates.filter { relationAliases.contains(it.leftRelationAlias) && relationAliases.contains(it.rightRelationAlias) }
        val newAttributeAccesses = extractAttributeAccesses(unaryPredicates + binaryPredicates)
        return Relation(newWindows, newPredicates, newAttributeAccesses)
    }

    /**
     * Creates all base relations from this relation, i.e., the relations that
     * consist only of a single window definition and unary predicates with the according relationAlias.
     */
    fun baseRelations(): Collection<Relation> = windowDefinition.map {
        Relation(
                mapOf(it.key to it.value),
                unaryPredicates.filter { predicate -> predicate.relationAlias == it.key },
                attributeAccesses.filter { attributeAccess -> attributeAccess.relationAlias == it.key })
    }

    /**
     * Creates a new relation composed of the join of this relation and the passed relations.
     * Additionally all predicates that are valid join predicates (i.e., where one side has an relationAlias
     * of this relation and the other side as an relationAlias to the passed relations) are added.
     */
    fun join(predicates: Collection<Predicate>, vararg relations: Relation): Relation {
        val joinPredicates = predicates.filterIsInstance<BinaryPredicate>().filter {
            !isCrossProduct(listOf(it), relations.toList(), this)
        }
        val relationsWindows = relations.flatMap { it.windowDefinition.entries }.associate { it.key to it.value }
        val newAttributeAccesses = extractAttributeAccesses(this.predicates + joinPredicates)
        return Relation(this.windowDefinition + relationsWindows, this.predicates + joinPredicates, newAttributeAccesses)
    }

    override fun toString(): String {
        val windowPart = windowDefinition.map { "${it.key.inner}${it.value}" }.joinToString(", ", "{", "}")
        val predicatePart = predicates.joinToString(", ", "{", "}")
        return "<$windowPart, $predicatePart>"
    }

    override fun equals(other: Any?): Boolean {
        if (!(other is Relation))
            return false
        return windowDefinition.equals(other.windowDefinition) &&
                predicates.equals(other.predicates) &&
                attributeAccesses.equals(other.attributeAccesses)
    }
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
    val relationAliases1 = relations1.flatMap { it.aliases }
    val relationAliases2 = relations2.flatMap { it.aliases }
    return isCrossProductAlias(binaryPredicates, relationAliases1, relationAliases2)
}
