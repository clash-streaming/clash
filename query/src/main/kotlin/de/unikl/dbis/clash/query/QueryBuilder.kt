package de.unikl.dbis.clash.query

class QueryBuilder {
    internal val from: MutableMap<RelationAlias, WindowDefinition> = mutableMapOf()
    internal val to: MutableList<String> = mutableListOf()
    internal val unaryPredicates: MutableList<UnaryPredicate> = mutableListOf()
    internal val joinPredicates: MutableList<BinaryPredicate> = mutableListOf()
    internal val inputMap: InputMap = InputMap()

    fun from(s: String): QueryBuilder {
        return from(RelationAlias(s), WindowDefinition.infinite())
    }

    fun from(s: RelationAlias): QueryBuilder {
        return from(s, WindowDefinition.infinite())
    }

    fun from(i: String, s: String): QueryBuilder {
        return from(InputName(i), RelationAlias(s), WindowDefinition.infinite())
    }

    fun from(s: String, w: WindowDefinition): QueryBuilder {
        return from(RelationAlias(s), w)
    }

    fun from(s: RelationAlias, w: WindowDefinition): QueryBuilder {
        return from(InputName(s.inner), s, w)
    }

    fun from(i: String, s: String, w: WindowDefinition): QueryBuilder {
        return from(InputName(i), RelationAlias(s), w)
    }

    fun from(i: String, s: RelationAlias, w: WindowDefinition): QueryBuilder {
        return from(InputName(i), s, w)
    }

    fun from(i: InputName, s: RelationAlias, w: WindowDefinition): QueryBuilder {
        from[s] = w
        inputMap[s] = i
        return this
    }

    fun where(predicate: Predicate): QueryBuilder {
        when(predicate) {
            is UnaryPredicate -> this.unaryPredicates.add(predicate)
            is BinaryPredicate -> this.joinPredicates.add(predicate)
        }
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
