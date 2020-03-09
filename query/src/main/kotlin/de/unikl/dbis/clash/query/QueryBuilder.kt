package de.unikl.dbis.clash.query

class QueryBuilder {
    internal val from: MutableMap<RelationAlias, WindowDefinition> = mutableMapOf()
    internal val to: MutableList<String> = mutableListOf()
    internal val filters: MutableList<UnaryPredicate> = mutableListOf()
    internal val joinPredicates: MutableList<BinaryPredicate> = mutableListOf()
    internal val inputMap: InputMap = InputMap()
    internal val aggregations: MutableList<Aggregation> = mutableListOf()
    internal val projections: MutableList<Projection> = mutableListOf()
    internal var alias: RelationAlias = RelationAlias("_result")

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
        when (predicate) {
            is UnaryPredicate -> this.filters.add(predicate)
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

    fun select(projection: Projection): QueryBuilder {
        this.projections.add(projection)
        return this
    }

    fun groupBy(aggregation: Aggregation): QueryBuilder {
        this.aggregations.add(aggregation)
        return this
    }

    fun to(to: String): QueryBuilder {
        this.to.add(to)
        return this
    }

    fun alias(alias: RelationAlias): QueryBuilder {
        this.alias = alias
        return this
    }

    fun build(): Query {
        val relation = Relation(
            from,
            filters,
            joinPredicates,
            aggregations,
            projections,
            alias
        )
        return Query(relation, inputMap)
    }

    inner class InvalidQueryException : Exception()
}
