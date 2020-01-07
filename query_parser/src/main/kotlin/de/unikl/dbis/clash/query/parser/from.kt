package de.unikl.dbis.clash.query.parser

import de.unikl.dbis.clash.query.InputName
import de.unikl.dbis.clash.query.RelationAlias
import de.unikl.dbis.clash.query.WindowDefinition
import net.sf.jsqlparser.schema.Table
import net.sf.jsqlparser.statement.select.FromItem
import net.sf.jsqlparser.statement.select.FromItemVisitorAdapter
import net.sf.jsqlparser.statement.select.Join
import net.sf.jsqlparser.statement.select.TableFunction

/**
 * This file contains functions for handling the FROM part of a query
 */

data class BasicFrom(val inputName: InputName, val windowArgs: List<String>, val relationAlias: RelationAlias) {
    val window: WindowDefinition
        get() {
            if (windowArgs.isEmpty()) {
                return WindowDefinition.infinite()
            }
            if (windowArgs.size != 3) {
                throw QueryParseException("Could not parse window definition $windowArgs")
            }
            if (windowArgs[0] != "sliding") {
                throw QueryParseException("Window type '${windowArgs[0]}' is not supported.")
            }
            if (windowArgs[1].toLongOrNull() == null ||
                    windowArgs[1].toLong() <= 0) {
                throw QueryParseException("Do not understand window size ${windowArgs[1]}.")
            }
            return when (windowArgs[2]) {
                "seconds", "second" -> WindowDefinition.seconds(windowArgs[1].toLong())
                "minutes", "minute" -> WindowDefinition.minutes(windowArgs[1].toLong())
                "hours", "hour" -> WindowDefinition.hours(windowArgs[1].toLong())
                else -> throw QueryParseException("Do not understand window duration ${windowArgs[2]}")
            }
        }
}

fun extractFrom(
    stmt: FromItem,
    joins: List<Join>
): MutableList<BasicFrom> {

    val fromElements = mutableListOf<BasicFrom>()

    class JoinVisitor : FromItemVisitorAdapter() {
        override fun visit(table: Table) {
            val rawAlias = table.alias?.name ?: table.name
            val alias = RelationAlias(rawAlias)
            val inputName = InputName(table.name)
            fromElements += BasicFrom(inputName, listOf(), alias)
        }
        override fun visit(tableFunction: TableFunction) {
            val rawAlias = tableFunction.alias?.name ?: tableFunction.function.name
            val alias = RelationAlias(rawAlias)
            val inputName = InputName(tableFunction.function.name)
            fromElements += BasicFrom(inputName, extractParameters(tableFunction.function.parameters), alias)
        }
    }
    stmt.accept(JoinVisitor())
    joins.forEach { it.rightItem.accept(JoinVisitor()) }

    return fromElements
}
