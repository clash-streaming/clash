package de.unikl.dbis.clash.query

data class Similarity(
        override val leftRelationAlias: RelationAlias,
        override val rightRelationAlias: RelationAlias
) : BinaryPredicate {
    

    override fun joinable(left: Tuple, right: Tuple): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun toString(): String {
        return "similar(${leftRelationAlias.inner}, ${rightRelationAlias.inner})"
    }
}