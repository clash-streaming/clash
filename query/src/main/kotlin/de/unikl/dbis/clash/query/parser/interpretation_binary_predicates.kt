package de.unikl.dbis.clash.query.parser

import de.unikl.dbis.clash.query.BinaryEquality
import de.unikl.dbis.clash.query.BinaryLessThan
import de.unikl.dbis.clash.query.BinaryLessThanOrEqual
import de.unikl.dbis.clash.query.Predicate
import net.sf.jsqlparser.expression.operators.relational.EqualsTo
import net.sf.jsqlparser.expression.operators.relational.GreaterThan
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals
import net.sf.jsqlparser.expression.operators.relational.MinorThan
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals
import net.sf.jsqlparser.schema.Column

fun interpretBinaryEquals(expr: EqualsTo): Predicate {
    val left = expr.leftExpression as Column
    val right = expr.rightExpression as Column

    val leftAccess = toAttributeAccess(left)
    val rightAccess = toAttributeAccess(right)

    return BinaryEquality(leftAccess, rightAccess)
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
