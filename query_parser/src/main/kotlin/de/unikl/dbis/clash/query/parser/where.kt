package de.unikl.dbis.clash.query.parser

import de.unikl.dbis.clash.query.*
import net.sf.jsqlparser.expression.*
import net.sf.jsqlparser.expression.operators.conditional.AndExpression
import net.sf.jsqlparser.expression.operators.relational.*
import net.sf.jsqlparser.schema.Column


/**
 * This file contains functions for handling the WHERE part of a query
 */

fun extractPredicates(where: Expression?): List<Predicate> {
    if(where == null) {
        return listOf()
    }
    val result = mutableListOf<Predicate>()
    val binaryPredicates = mutableListOf<BinaryExpression>()
    val isNulls = mutableListOf<IsNullExpression>()
    val betweenPredicates = mutableListOf<Between>()
    val ins = mutableListOf<InExpression>()
    class PredicateVisitor : ExpressionVisitorAdapter() {
        override fun visit(expr: AndExpression) {
            expr.leftExpression.accept(this)
            expr.rightExpression.accept(this)
        }

        override fun visit(expr: EqualsTo) {
            result += when(isUnary(expr)) {
                true -> interpretUnaryEquals(expr)
                false -> interpretBinaryEquals(expr)
            }
        }

        override fun visit(expr: Between) {
            betweenPredicates.add(expr)
        }

        override fun visit(expr: GreaterThan) {
            result += when(isUnary(expr)) {
                true -> interpretUnaryGreaterThan(expr)
                false -> interpretBinaryGreaterThan(expr)
            }
        }

        override fun visit(expr: GreaterThanEquals) {
            binaryPredicates.add(expr)
        }

        override fun visit(expr: MinorThan) {
            result += when(isUnary(expr)) {
                true -> interpretUnaryLesserThan(expr)
                false -> interpretBinaryLesserThan(expr)
            }
        }

        override fun visit(expr: MinorThanEquals) {
            binaryPredicates.add(expr)
        }

        override fun visit(expr: NotEqualsTo) {
            binaryPredicates.add(expr)
        }

        override fun visit(expr: LikeExpression) {
            binaryPredicates.add(expr)
            result += interpretLike(expr)
        }

        override fun visit(expr: IsNullExpression) {
            isNulls.add(expr)
        }

        override fun visit(expr: InExpression) {
            ins.add(expr)
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
    return when(constant) {
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
    val (col, like) = when(expr.leftExpression is Column && expr.rightExpression is StringValue) {
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
    return when(constant) {
        is StringValue -> AttributeGreaterThanConstant(attributeAccess, constant.value)
        is LongValue -> AttributeGreaterThanConstant(attributeAccess, constant.value)
        is DateTimeLiteralExpression -> AttributeGreaterThanConstant(attributeAccess, constant.value)
        else -> throw QueryParseException("GreaterThan with type ${constant::class.java} is not implemented.")
    }
}

fun interpretBinaryGreaterThan(expr: GreaterThan): Predicate {
    val left = expr.leftExpression as Column
    val right = expr.rightExpression as Column

    val leftAccess = toAttributeAccess(left)
    val rightAccess = toAttributeAccess(right)

    return BinaryGreaterThan(leftAccess, rightAccess)
}

fun interpretUnaryLesserThan(expr: MinorThan): AttributeLesserThanConstant<Any> {
    val (col, constant) = if (isColumn(expr.leftExpression)) {
        Pair(expr.leftExpression as Column, expr.rightExpression)
    } else {
        Pair(expr.rightExpression as Column, expr.leftExpression)
    }
    val attributeAccess = toAttributeAccess(col)
    return when(constant) {
        is StringValue -> AttributeLesserThanConstant(attributeAccess, constant.value)
        is LongValue -> AttributeLesserThanConstant(attributeAccess, constant.value)
        is DateTimeLiteralExpression -> AttributeLesserThanConstant(attributeAccess, constant.value)
        else -> throw QueryParseException("GreaterThan with type ${constant::class.java} is not implemented.")
    }
}

fun interpretBinaryLesserThan(expr: MinorThan): Predicate {
    val left = expr.leftExpression as Column
    val right = expr.rightExpression as Column

    val leftAccess = toAttributeAccess(left)
    val rightAccess = toAttributeAccess(right)

    // reverse the greater-than direction
    return BinaryGreaterThan(rightAccess, leftAccess)
}

fun extractParameters(expressionList: ExpressionList): List<String> {
    if(expressionList.expressions.any { e -> e !is StringValue }) {
        throw QueryParseException("Parameters of table functions must be Strings!")
    }

    return expressionList.expressions.map { e -> e as StringValue; e.value }
}
