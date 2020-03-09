package de.unikl.dbis.clash.query.parser

import de.unikl.dbis.clash.query.AttributeAccess
import de.unikl.dbis.clash.query.AttributeGreaterThanConstant
import de.unikl.dbis.clash.query.AttributeGreaterThanOrEqualConstant
import de.unikl.dbis.clash.query.AttributeLessThanConstant
import de.unikl.dbis.clash.query.AttributeLessThanOrEqualConstant
import de.unikl.dbis.clash.query.BinaryEquality
import de.unikl.dbis.clash.query.BinaryPredicate
import de.unikl.dbis.clash.query.ConstantEquality
import de.unikl.dbis.clash.query.OrList
import de.unikl.dbis.clash.query.Predicate
import de.unikl.dbis.clash.query.UnaryLike
import de.unikl.dbis.clash.query.UnaryNotLike
import de.unikl.dbis.clash.query.UnaryPredicate

fun parsePredicate(s: String): Predicate? {
    return (parseUnaryPredicate(s) ?: parseBinaryPredicate(s)) as Predicate? ?: parseOrList(s)
}

fun parseUnaryPredicate(s: String): UnaryPredicate? {
    fun parseConstantEqualityInt(s: String): ConstantEquality<Int>? {
        return null
    }

    fun parseConstantEqualityLong(s: String): ConstantEquality<Long>? {
        return null
    }

    fun parseConstantEqualityDouble(s: String): ConstantEquality<Double>? {
        return null
    }

    /**
     * The four permutations that are found here are those:
     *
     * ct.kind = 'production companies'  => (1) => ct.kind, (3) => 'production companies'
     * kind = 'production companies'     => (1) => kind, (3) => 'production companies'
     * 'production companies' = kind     => (1) => 'production companies', (3) => kind
     * 'production companies' = ct.kind  => (1) => 'production companies', (3) => ct.kind
     *
     * So we check if the first match starts and ends with a semicolon and interpret accordingly.
     */
    val constantEqualityStringRegex = "('.*'|(.+\\.)?.+)\\s+=\\s+('.*'|(.+\\.)?.+)".toRegex()
    fun parseConstantEqualityString(s: String): ConstantEquality<Any>? {
        if (s.matches(constantEqualityStringRegex)) {
            val matchResult = constantEqualityStringRegex.find(s)!!
            val groupA = matchResult.groupValues[1]
            val groupB = matchResult.groupValues[3]
            if (groupA.startsWith("'") && groupA.endsWith("'")) {
                val attributeAccess = AttributeAccess(groupB)
                val constant = groupA.substring(1, groupA.length - 1)
                return ConstantEquality(attributeAccess, constant)
            }
            if (groupB.startsWith("'") && groupB.endsWith("'")) {
                val attributeAccess = AttributeAccess(groupA)
                val constant = groupB.substring(1, groupB.length - 1)
                return ConstantEquality(attributeAccess, constant)
            }
        }
        return null
    }

    val unaryLikeRegex = "^\\s*(\\S+\\.?\\S+)\\s+LIKE\\s+('.*')".toRegex()
    fun parseUnaryLike(s: String): UnaryLike? {
        if (s.matches(unaryLikeRegex)) {
            val matchResult = unaryLikeRegex.find(s)!!
            val groupA = matchResult.groupValues[1]
            val groupB = matchResult.groupValues[2]
            val attributeAccess = AttributeAccess(groupA)
            val likeExpr = groupB.substring(1, groupB.length - 1)
            return UnaryLike(attributeAccess, likeExpr)
        }
        return null
    }

    val unaryNotLikeRegex = "^\\s*(\\S+\\.?\\S+)\\s+NOT\\s+LIKE\\s+('.*')".toRegex()
    fun parseUnaryNotLike(s: String): UnaryNotLike? {
        if (s.matches(unaryNotLikeRegex)) {
            val matchResult = unaryNotLikeRegex.find(s)!!
            val groupA = matchResult.groupValues[1]
            val groupB = matchResult.groupValues[2]
            val attributeAccess = AttributeAccess(groupA)
            val likeExpr = groupB.substring(1, groupB.length - 1)
            return UnaryNotLike(attributeAccess, likeExpr)
        }
        return null
    }

    fun parseAttributeLessThanConstantInt(s: String): AttributeLessThanConstant<Int>? {
        return null
    }

    fun parseAttributeLessThanConstantLong(s: String): AttributeLessThanConstant<Long>? {
        return null
    }

    fun parseAttributeLessThanConstantDouble(s: String): AttributeLessThanConstant<Double>? {
        return null
    }

    fun parseAttributeLessThanConstantString(s: String): AttributeLessThanConstant<Any>? {
        return null
    }

    fun parseAttributeGreaterThanConstantInt(s: String): AttributeGreaterThanConstant<Int>? {
        return null
    }

    fun parseAttributeGreaterThanConstantLong(s: String): AttributeGreaterThanConstant<Long>? {
        return null
    }

    fun parseAttributeGreaterThanConstantDouble(s: String): AttributeGreaterThanConstant<Double>? {
        return null
    }

    fun parseAttributeGreaterThanConstantString(s: String): AttributeGreaterThanConstant<Any>? {
        return null
    }

    fun parseAttributeLessThanOrEqualConstantInt(s: String): AttributeLessThanOrEqualConstant<Int>? {
        return null
    }

    fun parseAttributeLessThanOrEqualConstantLong(s: String): AttributeLessThanOrEqualConstant<Long>? {
        return null
    }

    fun parseAttributeLessThanOrEqualConstantDouble(s: String): AttributeLessThanOrEqualConstant<Double>? {
        return null
    }

    fun parseAttributeLessThanOrEqualConstantString(s: String): AttributeLessThanOrEqualConstant<Any>? {
        return null
    }

    fun parseAttributeGreaterThanOrEqualConstantInt(s: String): AttributeGreaterThanOrEqualConstant<Int>? {
        return null
    }

    fun parseAttributeGreaterThanOrEqualConstantLong(s: String): AttributeGreaterThanOrEqualConstant<Long>? {
        return null
    }

    fun parseAttributeGreaterThanOrEqualConstantDouble(s: String): AttributeGreaterThanOrEqualConstant<Double>? {
        return null
    }

    fun parseAttributeGreaterThanOrEqualConstantString(s: String): AttributeGreaterThanOrEqualConstant<Any>? {
        return null
    }

    return parseConstantEqualityInt(s)
        ?: parseConstantEqualityLong(s)
        ?: parseConstantEqualityDouble(s)
        ?: parseConstantEqualityString(s)
        ?: parseUnaryLike(s)
        ?: parseUnaryNotLike(s)
        ?: parseAttributeLessThanConstantInt(s)
        ?: parseAttributeLessThanConstantLong(s)
        ?: parseAttributeLessThanConstantDouble(s)
        ?: parseAttributeLessThanConstantString(s)
        ?: parseAttributeGreaterThanConstantInt(s)
        ?: parseAttributeGreaterThanConstantLong(s)
        ?: parseAttributeGreaterThanConstantDouble(s)
        ?: parseAttributeGreaterThanConstantString(s)
        ?: parseAttributeLessThanOrEqualConstantInt(s)
        ?: parseAttributeLessThanOrEqualConstantLong(s)
        ?: parseAttributeLessThanOrEqualConstantDouble(s)
        ?: parseAttributeLessThanOrEqualConstantString(s)
        ?: parseAttributeGreaterThanOrEqualConstantInt(s)
        ?: parseAttributeGreaterThanOrEqualConstantLong(s)
        ?: parseAttributeGreaterThanOrEqualConstantDouble(s)
        ?: parseAttributeGreaterThanOrEqualConstantString(s)
}

fun parseBinaryPredicate(s: String): BinaryPredicate? {
    val binaryEqualityRegex = "(\\w+)\\.(\\w+)\\s*=\\s*(\\w+)\\.(\\w+)".toRegex()
    fun parseBinaryEquality(s: String): BinaryEquality? {
        if (s.matches(binaryEqualityRegex)) {
            val matchResult = binaryEqualityRegex.find(s)!!
            val (leftRelation, leftAttr, rightRelation, rightAttr) = matchResult.destructured
            return BinaryEquality(AttributeAccess(leftRelation, leftAttr), AttributeAccess(rightRelation, rightAttr))
        }
        return null
    }

    return parseBinaryEquality(s)
}

fun parseOrList(s: String): OrList? {
    if (s.startsWith("(") && s.endsWith(")")) {
        val predicates = s.substring(1, s.length - 1).split("OR")
            .map { it.trim() }
            .map { parsePredicate(it) }
        val notNull = predicates.filterNotNull()
        if (predicates.size != notNull.size) {
            return null
        }
        return OrList(notNull)
    }
    return null
}
