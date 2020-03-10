package de.unikl.dbis.clash.query.parser

import de.unikl.dbis.clash.query.Predicate
import de.unikl.dbis.clash.query.RelationAlias
import de.unikl.dbis.clash.query.Similarity
import net.sf.jsqlparser.expression.Expression
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter
import net.sf.jsqlparser.expression.Function
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
