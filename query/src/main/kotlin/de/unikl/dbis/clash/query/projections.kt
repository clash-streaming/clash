package de.unikl.dbis.clash.query

typealias ProjectionList = List<Projection>
data class Projection(val attributeAccess: AttributeAccess, val alias: String) {
    companion object {
        val projectionWithAliasRegex = "(\\w+)\\.(\\w+)\\s+(\\w+)".toRegex()
        val projectionWithoutAliasRegex = "(\\w+)\\.(\\w+)".toRegex()

        fun fromString(string: String): Projection? {
            if (string.matches(projectionWithAliasRegex)) {
                val matchResult = projectionWithAliasRegex.find(string)!!
                val relationAlias = matchResult.groupValues[0]
                val attribute = matchResult.groupValues[1]
                val attributeAccess = AttributeAccess(relationAlias, attribute)
                val alias = matchResult.groupValues[2]
                return Projection(attributeAccess, alias)
            }
            if (string.matches(projectionWithoutAliasRegex)) {
                val matchResult = projectionWithoutAliasRegex.find(string)!!
                val relationAlias = matchResult.groupValues[0]
                val attribute = matchResult.groupValues[1]
                val attributeAccess = AttributeAccess(relationAlias, attribute)
                return Projection(attributeAccess, attribute)
            }
            return null
        }
    }
}
