package de.unikl.dbis.clash.query.parser

import de.unikl.dbis.clash.query.AttributeAccess
import de.unikl.dbis.clash.query.BinaryEquality
import de.unikl.dbis.clash.query.BinaryLessThan
import de.unikl.dbis.clash.query.BinaryLessThanOrEqual
import de.unikl.dbis.clash.query.BinaryPredicate
import de.unikl.dbis.clash.query.OrList
import de.unikl.dbis.clash.query.Predicate
import net.sf.jsqlparser.expression.BinaryExpression
import net.sf.jsqlparser.expression.Expression
import net.sf.jsqlparser.expression.Parenthesis
import net.sf.jsqlparser.expression.operators.conditional.OrExpression
import net.sf.jsqlparser.expression.operators.relational.EqualsTo
import net.sf.jsqlparser.expression.operators.relational.GreaterThan
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals
import net.sf.jsqlparser.expression.operators.relational.LikeExpression
import net.sf.jsqlparser.expression.operators.relational.MinorThan
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals
import net.sf.jsqlparser.parser.CCJSqlParserUtil

fun parsePredicate(s: String): Predicate? {
    val expr = CCJSqlParserUtil.parseCondExpression(s)
    return parseExpression(expr)
}

fun parseExpression(expr: Expression): Predicate? {
    if (expr is Parenthesis) {
        return parseExpression(expr.expression)
    }
    if (expr is OrExpression) {
        return interpretOr(expr)
    }
    if (expr !is BinaryExpression) return null
    return when (isUnary(expr)) {
        true -> parseUnary(expr)
        false -> parseBinary(expr)
    }
}

fun parseUnary(expr: BinaryExpression): Predicate? {
    return when (expr) {
        is EqualsTo -> interpretUnaryEquals(expr)
        is LikeExpression -> interpretLike(expr)
        is GreaterThan -> interpretUnaryGreaterThan(expr)
        is GreaterThanEquals -> interpretUnaryGreaterThanOrEqual(expr)
        is MinorThan -> interpretUnaryLessThan(expr)
        is MinorThanEquals -> interpretUnaryLessThanOrEqual(expr)
        else -> null
    }
}

fun parseBinary(expr: BinaryExpression): Predicate? {
    return when (expr) {
        is EqualsTo -> interpretBinaryEquals(expr)
        is GreaterThan -> interpretBinaryGreaterThan(expr)
        is GreaterThanEquals -> interpretBinaryGreaterThanOrEqual(expr)
        is MinorThan -> interpretBinaryLessThan(expr)
        is MinorThanEquals -> interpretBinaryLessThanOrEqual(expr)
        else -> null
    }
}

fun parseJoinPredicate(s: String): BinaryPredicate? {
    val binaryEqualityRegex = "(\\w+)\\.(\\w+)\\s*=\\s*(\\w+)\\.(\\w+)".toRegex()
    fun parseBinaryEquality(s: String): BinaryEquality? {
        if (s.matches(binaryEqualityRegex)) {
            val matchResult = binaryEqualityRegex.find(s)!!
            val (leftRelation, leftAttr, rightRelation, rightAttr) = matchResult.destructured
            return BinaryEquality(AttributeAccess(leftRelation, leftAttr), AttributeAccess(rightRelation, rightAttr))
        }
        return null
    }

    val binaryLessThanRegex = "(\\w+)\\.(\\w+)\\s*<\\s*(\\w+)\\.(\\w+)".toRegex()
    fun parseBinaryLessThan(s: String): BinaryLessThan? {
        if (s.matches(binaryLessThanRegex)) {
            val matchResult = binaryEqualityRegex.find(s)!!
            val (leftRelation, leftAttr, rightRelation, rightAttr) = matchResult.destructured
            return BinaryLessThan(AttributeAccess(leftRelation, leftAttr), AttributeAccess(rightRelation, rightAttr))
        }
        return null
    }

    val binaryLessThanOrEqualRegex = "(\\w+)\\.(\\w+)\\s*<=\\s*(\\w+)\\.(\\w+)".toRegex()
    fun parseBinaryLessThanOrEqual(s: String): BinaryLessThanOrEqual? {
        if (s.matches(binaryLessThanOrEqualRegex)) {
            val matchResult = binaryEqualityRegex.find(s)!!
            val (leftRelation, leftAttr, rightRelation, rightAttr) = matchResult.destructured
            return BinaryLessThanOrEqual(AttributeAccess(leftRelation, leftAttr), AttributeAccess(rightRelation, rightAttr))
        }
        return null
    }

    return parseBinaryEquality(s)
        ?: parseBinaryLessThan(s)
        ?: parseBinaryLessThanOrEqual(s)
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
