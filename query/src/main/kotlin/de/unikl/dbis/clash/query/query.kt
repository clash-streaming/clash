package de.unikl.dbis.clash.query

import java.io.Serializable

data class Query(
    val result: Relation,
    val inputMap: InputMap
) : Serializable

data class RelationAlias(val inner: String) : Serializable {
    override fun toString(): String = inner
}

typealias Attribute = String
typealias AttributeList = List<String>
typealias AttributeAccessList = List<AttributeAccess>

data class AttributeAccess(val relationAlias: RelationAlias, val attribute: Attribute) : Serializable {
    constructor(string: String, attribute: Attribute) : this(RelationAlias(string), attribute)
    constructor(string: String) : this(string.split(".")[0], string.split(".")[1])

    override fun toString(): String {
        return "$relationAlias.$attribute"
    }
}

typealias ProjectionList = List<Projection>
data class Projection(val attributeAccess: AttributeAccess, val alias: String)

fun relationOf(string: String): Relation {
    return Relation(mapOf(RelationAlias(string) to WindowDefinition.infinite()), listOf(), listOf())
}
