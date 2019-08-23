package de.unikl.dbis.clash.query.parser

import de.unikl.dbis.clash.query.AttributeAccess
import de.unikl.dbis.clash.query.Projection
import de.unikl.dbis.clash.query.ProjectionList
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.statement.select.SelectExpressionItem
import net.sf.jsqlparser.statement.select.SelectItem
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter


/**
 * This file contains functions for handling the SELECT part of a query
 */

fun extractSelects(selectItems: List<SelectItem>): ProjectionList {
    return selectItems.mapNotNull { extractSelect(it) }.toList()
}

fun extractSelect(selectItem: SelectItem): Projection? {
    var projection: Projection? = null

    class SelectVisitor : SelectItemVisitorAdapter() {
        override fun visit(item: SelectExpressionItem) {
            var attributeAccess: AttributeAccess? = null
            class SelectExpressionVisitor : ExpressionVisitorAdapter() {
                override fun visit(column: Column) {
                    attributeAccess = toAttributeAccess(column)
                }
            }
            item.accept(SelectExpressionVisitor())
            val alias = item.alias?.name ?: attributeAccess!!.attribute
            projection = Projection(attributeAccess!!, alias)
        }
    }

    selectItem.accept(SelectVisitor())

    return projection
}
