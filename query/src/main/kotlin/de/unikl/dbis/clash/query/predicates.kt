package de.unikl.dbis.clash.query

import java.io.Serializable


interface Predicate

interface Tuple {
    operator fun get(attr: AttributeAccess): String?
}

interface UnaryPredicate : Predicate, Serializable {
    val attributeAccess: AttributeAccess
    fun evaluate(tuple: Tuple): Boolean
}

//fun Collection<BinaryPredicate>.restrict(relations: Collection<String>): Collection<BinaryPredicate> {
//    return filter { relations.contains(it.left) || relations.contains(it.right)}
//}

interface BinaryPredicate : Predicate, Serializable {
    val leftAttributeAccess: AttributeAccess
    val rightAttributeAccess: AttributeAccess

    fun joinable(left: Tuple, right: Tuple): Boolean

    companion object {
        fun toString(predicates: Set<BinaryPredicate>): String {
            return predicates.joinToString(", ")
        }

        fun fromString(joinPredicate: String): BinaryPredicate {
            val attributePairEqualityRegex = "(\\w+)\\.(\\w+)\\s*=\\s*(\\w+)\\.(\\w+)".toRegex()

            if(joinPredicate.matches(attributePairEqualityRegex)) {
                val matchResult = attributePairEqualityRegex.find(joinPredicate)!!
                val (leftRelation, leftAttr, rightRelation, rightAttr) = matchResult.destructured
                return BinaryEquality(AttributeAccess(leftRelation, leftAttr), AttributeAccess(rightRelation, rightAttr))
            }
            throw RuntimeException("Did not understand join predicate '$joinPredicate'")
        }
    }
}


fun extractAttributeAccesses(predicates: List<Predicate>): Collection<AttributeAccess> {
    val result = mutableListOf<AttributeAccess>()

    for(predicate in predicates) {
        when(predicate) {
            is UnaryPredicate -> result.add(predicate.attributeAccess)
            is BinaryPredicate -> {
                result.add(predicate.leftAttributeAccess)
                result.add(predicate.rightAttributeAccess)
            }
        }
    }

    return result
}