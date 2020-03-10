package de.unikl.dbis.clash.query

import java.io.Serializable

interface Predicate

interface Tuple {
    operator fun get(attr: AttributeAccess): String?
}

interface UnaryPredicate : Predicate, Serializable {
    val relationAlias: RelationAlias
    fun evaluate(tuple: Tuple): Boolean
}

interface UnaryTuplePredicate : UnaryPredicate {
    override val relationAlias: RelationAlias
}

interface UnaryAttributePredicate : UnaryPredicate {
    val attributeAccess: AttributeAccess
    override val relationAlias: RelationAlias get() = attributeAccess.relationAlias
}

interface BinaryPredicate : Predicate, Serializable {
    val leftRelationAlias: RelationAlias
    val rightRelationAlias: RelationAlias

    fun joinable(left: Tuple, right: Tuple): Boolean

    companion object {
        fun toString(predicates: Set<BinaryPredicate>): String {
            return predicates.joinToString(", ")
        }
    }
}

interface BinaryTuplePredicate : BinaryPredicate {
    override val leftRelationAlias: RelationAlias
    override val rightRelationAlias: RelationAlias
}

interface BinaryAttributePredicate : BinaryPredicate {
    val leftAttributeAccess: AttributeAccess
    val rightAttributeAccess: AttributeAccess

    override val leftRelationAlias: RelationAlias get() = leftAttributeAccess.relationAlias
    override val rightRelationAlias: RelationAlias get() = rightAttributeAccess.relationAlias
}

fun extractAttributeAccesses(predicates: List<Predicate>): Collection<AttributeAccess> {
    val result = mutableListOf<AttributeAccess>()

    for (predicate in predicates) {
        when (predicate) {
            is UnaryAttributePredicate -> result.add(predicate.attributeAccess)
            is BinaryAttributePredicate -> {
                result.add(predicate.leftAttributeAccess)
                result.add(predicate.rightAttributeAccess)
            }
        }
    }

    return result
}

class OrList(val inner: List<Predicate>) : Predicate, Serializable

class Not(val inner: Predicate) : Predicate, Serializable
