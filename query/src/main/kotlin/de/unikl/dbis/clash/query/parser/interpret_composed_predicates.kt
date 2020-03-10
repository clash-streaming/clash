package de.unikl.dbis.clash.query.parser

import de.unikl.dbis.clash.query.OrList
import de.unikl.dbis.clash.query.Predicate
import net.sf.jsqlparser.expression.operators.conditional.OrExpression

fun interpretOr(expr: OrExpression): OrList {
    val left = parseExpression(expr.leftExpression)
    val right = parseExpression(expr.rightExpression)

    val result = mutableListOf<Predicate>()
    if (left is OrExpression) {
        val leftResult = interpretOr(left)
        result.addAll(leftResult.inner)
    } else {
        result.add(left!!)
    }

    if (right is OrExpression) {
        val rightResult = interpretOr(right)
        result.addAll(rightResult.inner)
    } else {
        result.add(right!!)
    }

    return OrList(result)
}
