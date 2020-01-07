package de.unikl.dbis.clash.query.parser

import de.unikl.dbis.clash.query.AttributeGreaterThanConstant
import de.unikl.dbis.clash.query.AttributeGreaterThanOrEqualConstant
import de.unikl.dbis.clash.query.AttributeLessThanConstant
import de.unikl.dbis.clash.query.AttributeLessThanOrEqualConstant
import de.unikl.dbis.clash.query.BinaryEquality
import de.unikl.dbis.clash.query.BinaryLessThan
import de.unikl.dbis.clash.query.BinaryLessThanOrEqual
import de.unikl.dbis.clash.query.ConstantEquality
import de.unikl.dbis.clash.query.Predicate
import de.unikl.dbis.clash.query.RelationAlias
import de.unikl.dbis.clash.query.Similarity
import de.unikl.dbis.clash.query.UnaryLike
import net.sf.jsqlparser.expression.DateTimeLiteralExpression
import net.sf.jsqlparser.expression.DoubleValue
import net.sf.jsqlparser.expression.Expression
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter
import net.sf.jsqlparser.expression.Function
import net.sf.jsqlparser.expression.LongValue
import net.sf.jsqlparser.expression.StringValue
import net.sf.jsqlparser.expression.operators.conditional.AndExpression
import net.sf.jsqlparser.expression.operators.relational.Between
import net.sf.jsqlparser.expression.operators.relational.EqualsTo
import net.sf.jsqlparser.expression.operators.relational.ExpressionList
import net.sf.jsqlparser.expression.operators.relational.GreaterThan
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals
import net.sf.jsqlparser.expression.operators.relational.InExpression
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression
import net.sf.jsqlparser.expression.operators.relational.LikeExpression
import net.sf.jsqlparser.expression.operators.relational.MinorThan
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo
import net.sf.jsqlparser.schema.Column

/**
 * This file contains functions for handling the WHERE part of a query
 */

fun extractPredicates(where: Expression?): List<Predicate> {
    if (where == null) {
        return listOf()
    }
    val result = mutableListOf<Predicate>()
    val isNulls = mutableListOf<IsNullExpression>()
    val betweenPredicates = mutableListOf<Between>()
    val ins = mutableListOf<InExpression>()
    val functions = mutableListOf<Function>()
    class PredicateVisitor : ExpressionVisitorAdapter() {
        override fun visit(expr: AndExpression) {
            expr.leftExpression.accept(this)
            expr.rightExpression.accept(this)
        }

        override fun visit(expr: EqualsTo) {
            result += when (isUnary(expr)) {
                true -> interpretUnaryEquals(expr)
                false -> interpretBinaryEquals(expr)
            }
        }

        override fun visit(expr: Between) {
            betweenPredicates.add(expr)
        }

        override fun visit(expr: GreaterThan) {
            result += when (isUnary(expr)) {
                true -> interpretUnaryGreaterThan(expr)
                false -> interpretBinaryGreaterThan(expr)
            }
        }

        override fun visit(expr: GreaterThanEquals) {
            result += when (isUnary(expr)) {
                true -> interpretUnaryGreaterThanOrEqual(expr)
                false -> interpretBinaryGreaterThanOrEqual(expr)
            }
        }

        override fun visit(expr: MinorThan) {
            result += when (isUnary(expr)) {
                true -> interpretUnaryLessThan(expr)
                false -> interpretBinaryLessThan(expr)
            }
        }

        override fun visit(expr: MinorThanEquals) {
            result += when (isUnary(expr)) {
                true -> interpretUnaryLessThanOrEqual(expr)
                false -> interpretBinaryLessThanOrEqual(expr)
            }
        }

        override fun visit(expr: NotEqualsTo) {
        }

        override fun visit(expr: LikeExpression) {
            result += interpretLike(expr)
        }

        override fun visit(expr: IsNullExpression) {
        }

        override fun visit(expr: InExpression) {
        }

        override fun visit(expr: Function) {
            result += interpretFunction(expr)
        }
    }

    where.accept(PredicateVisitor())
    return result
}

fun interpretUnaryEquals(expr: EqualsTo): ConstantEquality<Any> {
    val (col, constant) = if (isColumn(expr.leftExpression)) {
        Pair(expr.leftExpression as Column, expr.rightExpression)
    } else {
        Pair(expr.rightExpression as Column, expr.leftExpression)
    }
    val attributeAccess = toAttributeAccess(col)
    return when (constant) {
        is StringValue -> ConstantEquality(attributeAccess, constant.value)
        is LongValue -> ConstantEquality(attributeAccess, constant.value)
        else -> throw QueryParseException("Equality with type ${constant::class.java} is not implemented.")
    }
}

fun interpretBinaryEquals(expr: EqualsTo): Predicate {
    val left = expr.leftExpression as Column
    val right = expr.rightExpression as Column

    val leftAccess = toAttributeAccess(left)
    val rightAccess = toAttributeAccess(right)

    return BinaryEquality(leftAccess, rightAccess)
}

fun interpretLike(expr: LikeExpression): UnaryLike {
    val (col, like) = when (expr.leftExpression is Column && expr.rightExpression is StringValue) {
        true -> Pair(expr.leftExpression as Column, expr.rightExpression as StringValue)
        else -> throw QueryParseException("LIKE statement should be: [column] LIKE [string]")
    }

    val attributeAccess = toAttributeAccess(col)

    return UnaryLike(attributeAccess, like.value)
}

fun interpretUnaryGreaterThan(expr: GreaterThan): AttributeGreaterThanConstant<Any> {
    val (col, constant) = if (isColumn(expr.leftExpression)) {
        Pair(expr.leftExpression as Column, expr.rightExpression)
    } else {
        Pair(expr.rightExpression as Column, expr.leftExpression)
    }
    val attributeAccess = toAttributeAccess(col)
    return when (constant) {
        is StringValue -> AttributeGreaterThanConstant(attributeAccess, constant.value)
        is LongValue -> AttributeGreaterThanConstant(attributeAccess, constant.value)
        is DoubleValue -> AttributeGreaterThanConstant(attributeAccess, constant.value)
        is DateTimeLiteralExpression -> AttributeGreaterThanConstant(attributeAccess, constant.value)
        else -> throw QueryParseException("GreaterThan with type ${constant::class.java} is not implemented.")
    }
}

fun interpretUnaryGreaterThanOrEqual(expr: GreaterThanEquals): AttributeGreaterThanOrEqualConstant<Any> {
    val (col, constant) = if (isColumn(expr.leftExpression)) {
        Pair(expr.leftExpression as Column, expr.rightExpression)
    } else {
        Pair(expr.rightExpression as Column, expr.leftExpression)
    }
    val attributeAccess = toAttributeAccess(col)
    return when (constant) {
        is StringValue -> AttributeGreaterThanOrEqualConstant(attributeAccess, constant.value)
        is LongValue -> AttributeGreaterThanOrEqualConstant(attributeAccess, constant.value)
        is DoubleValue -> AttributeGreaterThanOrEqualConstant(attributeAccess, constant.value)
        is DateTimeLiteralExpression -> AttributeGreaterThanOrEqualConstant(attributeAccess, constant.value)
        else -> throw QueryParseException("GreaterThanOrEqual with type ${constant::class.java} is not implemented.")
    }
}

fun interpretBinaryGreaterThan(expr: GreaterThan): Predicate {
    val left = expr.leftExpression as Column
    val right = expr.rightExpression as Column

    val leftAccess = toAttributeAccess(left)
    val rightAccess = toAttributeAccess(right)

    // reverse the greater-than direction
    return BinaryLessThan(rightAccess, leftAccess)
}

fun interpretBinaryGreaterThanOrEqual(expr: GreaterThanEquals): Predicate {
    val left = expr.leftExpression as Column
    val right = expr.rightExpression as Column

    val leftAccess = toAttributeAccess(left)
    val rightAccess = toAttributeAccess(right)

    // reverse the greater-than direction
    return BinaryLessThanOrEqual(rightAccess, leftAccess)
}

fun interpretUnaryLessThan(expr: MinorThan): AttributeLessThanConstant<Any> {
    val (col, constant) = if (isColumn(expr.leftExpression)) {
        Pair(expr.leftExpression as Column, expr.rightExpression)
    } else {
        Pair(expr.rightExpression as Column, expr.leftExpression)
    }
    val attributeAccess = toAttributeAccess(col)
    return when (constant) {
        is StringValue -> AttributeLessThanConstant(attributeAccess, constant.value)
        is LongValue -> AttributeLessThanConstant(attributeAccess, constant.value)
        is DoubleValue -> AttributeLessThanConstant(attributeAccess, constant.value)
        is DateTimeLiteralExpression -> AttributeLessThanConstant(attributeAccess, constant.value)
        else -> throw QueryParseException("GreaterThan with type ${constant::class.java} is not implemented.")
    }
}

fun interpretUnaryLessThanOrEqual(expr: MinorThanEquals): AttributeLessThanOrEqualConstant<Any> {
    val (col, constant) = if (isColumn(expr.leftExpression)) {
        Pair(expr.leftExpression as Column, expr.rightExpression)
    } else {
        Pair(expr.rightExpression as Column, expr.leftExpression)
    }
    val attributeAccess = toAttributeAccess(col)
    return when (constant) {
        is StringValue -> AttributeLessThanOrEqualConstant(attributeAccess, constant.value)
        is LongValue -> AttributeLessThanOrEqualConstant(attributeAccess, constant.value)
        is DoubleValue -> AttributeLessThanOrEqualConstant(attributeAccess, constant.value)
        is DateTimeLiteralExpression -> AttributeLessThanOrEqualConstant(attributeAccess, constant.value)
        else -> throw QueryParseException("GreaterThanOrEqual with type ${constant::class.java} is not implemented.")
    }
}

fun interpretBinaryLessThan(expr: MinorThan): Predicate {
    val left = expr.leftExpression as Column
    val right = expr.rightExpression as Column

    val leftAccess = toAttributeAccess(left)
    val rightAccess = toAttributeAccess(right)

    return BinaryLessThan(leftAccess, rightAccess)
}

fun interpretBinaryLessThanOrEqual(expr: MinorThanEquals): Predicate {
    val left = expr.leftExpression as Column
    val right = expr.rightExpression as Column

    val leftAccess = toAttributeAccess(left)
    val rightAccess = toAttributeAccess(right)

    return BinaryLessThanOrEqual(leftAccess, rightAccess)
}

fun interpretFunction(expr: Function): Predicate {
    fun parseSimilarity(expr: Function): Similarity {
        val leftRelationName = (expr.parameters.expressions[0] as Column).columnName
        val rightRelationName = (expr.parameters.expressions[1] as Column).columnName
        return Similarity(RelationAlias(leftRelationName), RelationAlias(rightRelationName))
    }

    return when (expr.name) {
        "similar" -> parseSimilarity(expr)
        else -> throw QueryParseException("Function with name ${expr.name} is not implemented.")
    }
}

fun extractParameters(expressionList: ExpressionList): List<String> {
    if (expressionList.expressions.any { e -> e !is StringValue }) {
        throw QueryParseException("Parameters of table functions must be Strings!")
    }

    return expressionList.expressions.map { e -> e as StringValue; e.value }
}
