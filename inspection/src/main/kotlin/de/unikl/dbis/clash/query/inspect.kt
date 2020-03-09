package de.unikl.dbis.clash.query

const val SOURCE_CODE_INDENT = 8

fun Query.inspect(): String {
    return this.result.inspect()
}

fun Relation.inspect(depth: Int = 0): String {
    return """
        BaseRelationName: ${this.inputAliases}
        UnaryPredicates: ${this.unaryPredicates}
        Window: ${this.inputs}
    """
}
