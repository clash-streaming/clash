package de.unikl.dbis.clash.query

import java.io.Serializable

data class InputName(val inner: String) : Serializable
class InputMap(map: Map<RelationAlias, InputName>) : HashMap<RelationAlias, InputName>(map) {
    constructor() : this(emptyMap())
}

fun inputForRelation(relation: Relation, inputMap: Map<RelationAlias, InputName>): InputName {
    return inputMap.getValue(relation.aliases.first())
}
