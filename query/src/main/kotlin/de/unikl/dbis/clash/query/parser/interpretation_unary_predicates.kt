package de.unikl.dbis.clash.query.parser

import de.unikl.dbis.clash.query.AttributeGreaterThanConstant
import de.unikl.dbis.clash.query.AttributeGreaterThanOrEqualConstant
import de.unikl.dbis.clash.query.AttributeLessThanConstant
import de.unikl.dbis.clash.query.AttributeLessThanOrEqualConstant
import de.unikl.dbis.clash.query.ConstantEquality
import de.unikl.dbis.clash.query.Predicate
import de.unikl.dbis.clash.query.UnaryLike
import de.unikl.dbis.clash.query.UnaryNotLike
import net.sf.jsqlparser.expression.DateTimeLiteralExpression
import net.sf.jsqlparser.expression.DoubleValue
import net.sf.jsqlparser.expression.LongValue
import net.sf.jsqlparser.expression.StringValue
import net.sf.jsqlparser.expression.operators.relational.EqualsTo
import net.sf.jsqlparser.expression.operators.relational.GreaterThan
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals
import net.sf.jsqlparser.expression.operators.relational.LikeExpression
import net.sf.jsqlparser.expression.operators.relational.MinorThan
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals
import net.sf.jsqlparser.schema.Column

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

fun interpretLike(expr: LikeExpression): Predicate {
    val (col, like) = when (expr.leftExpression is Column && expr.rightExpression is StringValue) {
        true -> Pair(expr.leftExpression as Column, expr.rightExpression as StringValue)
        else -> throw QueryParseException("LIKE statement should be: [column] LIKE [string]")
    }

    val attributeAccess = toAttributeAccess(col)

    return if (expr.isNot) {
        UnaryNotLike(attributeAccess, like.value)
    } else {
        UnaryLike(attributeAccess, like.value)
    }
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
