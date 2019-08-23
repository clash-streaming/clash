package de.unikl.dbis.clash.query


class QueryBuilder {
    internal val from: MutableMap<RelationAlias, WindowDefinition> = mutableMapOf()
    internal val to: MutableList<String> = mutableListOf()
    internal val joinPredicates: MutableList<BinaryPredicate> = mutableListOf()
    internal val inputMap: InputMap = InputMap()

    fun from(s: String): QueryBuilder {
        return from(RelationAlias(s), WindowDefinition.infinite())
    }

    fun from(s: RelationAlias): QueryBuilder {
        return from(s, WindowDefinition.infinite())
    }

    fun from(s: String, w: WindowDefinition): QueryBuilder {
        return from(RelationAlias(s), w)
    }

    fun from(s: RelationAlias, w: WindowDefinition): QueryBuilder {
        from[s] = w
        inputMap[s] = InputName(s.inner)
        return this
    }

    fun where(joinPredicate: BinaryPredicate): QueryBuilder {
        this.joinPredicates.add(joinPredicate)
        return this
    }

    fun where(joinPredicate: String): QueryBuilder {
        return this.where(BinaryPredicate.fromString(joinPredicate))
    }

    fun to(to: String): QueryBuilder {
        this.to.add(to)
        return this
    }

    fun build(): Query {
        val relation = Relation(from, joinPredicates, extractAttributeAccesses(joinPredicates))
        return Query(relation, inputMap)
    }

    inner class InvalidQueryException : Exception()
}