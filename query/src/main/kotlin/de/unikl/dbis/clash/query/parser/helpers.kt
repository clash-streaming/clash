package de.unikl.dbis.clash.query.parser

import de.unikl.dbis.clash.query.AttributeAccess
import net.sf.jsqlparser.expression.BinaryExpression
import net.sf.jsqlparser.expression.DateTimeLiteralExpression
import net.sf.jsqlparser.expression.DateValue
import net.sf.jsqlparser.expression.DoubleValue
import net.sf.jsqlparser.expression.Expression
import net.sf.jsqlparser.expression.HexValue
import net.sf.jsqlparser.expression.LongValue
import net.sf.jsqlparser.expression.NullValue
import net.sf.jsqlparser.expression.SignedExpression
import net.sf.jsqlparser.expression.StringValue
import net.sf.jsqlparser.expression.TimeValue
import net.sf.jsqlparser.expression.TimestampValue
import net.sf.jsqlparser.schema.Column

/**
 * This function determines the type of the function.
 * If it takes one parameter, it is UNARY, e.g.:
 *
 *  x.a > 5
 *  y.a IN ['foo', 'bar']
 *
 * If it takes two parameters, it is BINARY, e.g.:
 *
 *  x.f = z.f
 *
 * If it is something else, an exception is thrown.
 */
fun isUnary(expr: BinaryExpression): Boolean {
    val potentialUnary = (isConstant(expr.leftExpression) && isColumn(
        expr.rightExpression
    )) ||
            (isConstant(expr.rightExpression) && isColumn(
                expr.leftExpression
            ))
    if (isConstant(expr.leftExpression) && isConstant(
            expr.rightExpression
        )
    ) {
        throw QueryParseException("Predicate with both operands being constant is not allowed.")
    }
    return potentialUnary
}

fun isConstant(expr: Expression): Boolean {
    return expr is DateValue ||
            expr is DateTimeLiteralExpression ||
            expr is DoubleValue ||
            expr is HexValue ||
            expr is LongValue ||
            expr is NullValue ||
            expr is SignedExpression ||
            expr is StringValue ||
            expr is TimestampValue ||
            expr is TimeValue
}

fun isColumn(expr: Expression): Boolean {
    return expr is Column
}

fun toAttributeAccess(col: Column): AttributeAccess {
    if (col.table == null) {
        throw QueryParseException("Trying to formulate predicate without qualifying the base relation.")
    }
    return AttributeAccess(col.table.name, col.columnName)
}
