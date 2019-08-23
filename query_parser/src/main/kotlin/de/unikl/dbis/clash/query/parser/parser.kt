package de.unikl.dbis.clash.query.parser

import de.unikl.dbis.clash.query.*
import net.sf.jsqlparser.expression.Expression
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.StatementVisitorAdapter
import net.sf.jsqlparser.statement.select.*

class QueryParseException(e: String) : RuntimeException(e)

fun parseQuery(query: String): Query {
    val stmt = CCJSqlParserUtil.parse(query)
    if(stmt !is Select) {
        throw QueryParseException("Query expected to be SELECT statement, but was ${stmt.javaClass}")
    }
    class ClashQueryVisitor(
            var selectItems: List<SelectItem>? = null,
            var fromItem: FromItem? = null,
            var joins: List<Join>? = null,
            var where: Expression? = null
    ) : SelectVisitor, StatementVisitorAdapter() {
        override fun visit(plainSelect: PlainSelect) {
            selectItems = plainSelect.selectItems
            fromItem = plainSelect.fromItem
            joins = plainSelect.joins
            where = plainSelect.where
        }

        override fun visit(setOpList: SetOperationList) { }

        override fun visit(withItem: WithItem) { }

        fun verify() {
            if(selectItems == null || fromItem == null) {
                throw QueryParseException("Query must at least consist of select and from!")
            }
        }
    }
    val visitor = ClashQueryVisitor()
    stmt.selectBody.accept(visitor)
    visitor.verify()

    val selects = extractSelects(visitor.selectItems!!)
    val froms = extractFrom(visitor.fromItem!!, if (visitor.joins == null) listOf() else visitor.joins!!)
    val aliases = froms.map { it.relationAlias to it.window }.toMap()
    val predicates = extractPredicates(visitor.where)
    val associated = froms.associate { it.relationAlias to it.inputName }
    val inputMap = InputMap(associated)

    validateAliases(selects, predicates, aliases)

    val attributeAccessList = extractAttributeAccesses(predicates)
    val rel = Relation(
            aliases,
            predicates,
            attributeAccessList
    )
    return Query(rel, inputMap)
}


fun validateAliases(selects: ProjectionList, predicates: List<Predicate>, aliases: Map<RelationAlias, WindowDefinition>) {
    for(select in selects) {
        val alias = select.attributeAccess.relationAlias
        if(!aliases.containsKey(alias)) {
            throw QueryParseException("Select statement referenced alias $alias which is not defined in from clause.")
        }
    }

    for(predicate in predicates) {
        if(predicate is UnaryPredicate) {
            val alias = predicate.attributeAccess.relationAlias
            if(!aliases.containsKey(alias)) {
                throw QueryParseException("Predicate referenced alias $alias which is not defined in from clause.")
            }
        }
        if(predicate is BinaryPredicate) {
            val lalias = predicate.leftAttributeAccess.relationAlias
            val ralias = predicate.rightAttributeAccess.relationAlias
            if(!aliases.containsKey(lalias)) {
                throw QueryParseException("Predicate referenced alias $lalias which is not defined in from clause.")
            }
            if(!aliases.containsKey(ralias)) {
                throw QueryParseException("Predicate referenced alias $ralias which is not defined in from clause.")
            }
        }
    }
}
