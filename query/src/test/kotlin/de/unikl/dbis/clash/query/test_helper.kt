package de.unikl.dbis.clash.query


class TestTuple(attributeAccess: AttributeAccess, value: String) : HashMap<AttributeAccess, String>(), Tuple {
    init {
        this[attributeAccess] = value
    }
}