package de.unikl.dbis.clash.documents

import de.unikl.dbis.clash.query.AttributeAccess
import de.unikl.dbis.clash.query.RelationAlias
import de.unikl.dbis.clash.query.Tuple
import java.util.Arrays
import org.json.JSONObject

class Document : HashMap<AttributeAccess, String>, Tuple {

    constructor() : super()

    private constructor(other: Document) : super(other)

    constructor(relation: RelationAlias, jsonObject: JSONObject) {
        for (key in jsonObject.keySet()) {
            val attributeAccess = AttributeAccess(relation, key)
            this[attributeAccess] = jsonObject.getString(key)
        }
    }

    fun createJoint(other: Document): Document {
        val result = Document(this)
        for ((key, value) in other) {
            result[key] = value
        }
        return result
    }

    fun toJson(): JSONObject {
        return JSONObject(this)
    }

    operator fun get(relation: RelationAlias, attribute: String): String? {
        return get(AttributeAccess(relation, attribute))
    }

    operator fun get(expression: String): String? {
        val tokens = expression.split(".")
        return when (tokens.size) {
            1 -> {
                get(getAccessPathForAttribute(tokens[0]))
            }
            2 -> get(RelationAlias(tokens[0]), tokens[1])
            else -> throw RuntimeException("Access not understood: $expression")
        }
    }

    operator fun set(relation: RelationAlias, attribute: String, value: String) {
        this[AttributeAccess(relation, attribute)] = value
    }

    fun containsKey(s: String): Boolean {
        return getAccessPathForAttribute(s) != null
    }

    fun getAccessPathForAttribute(s: String): AttributeAccess? {
        return keys.find { it.attribute == s }
    }

    override fun toString(): String {
        return super.entries
                .sortedBy { it.key.toString() }
                .joinToString(", ", "<", ">") { "${it.key}: ${it.value}" }
    }
}

fun fromKVList(vararg list: String): Document {
    val iterator = Arrays.stream(list).iterator()
    val document = Document()
    while (iterator.hasNext()) {
        val (table, attribute) = iterator.next().split(".")
        document[AttributeAccess(RelationAlias(table), attribute)] = iterator.next()
    }
    return document
}
