package de.unikl.dbis.clash.datacharacteristics

import de.unikl.dbis.clash.query.BinaryPredicate
import de.unikl.dbis.clash.query.Relation
import de.unikl.dbis.clash.query.RelationAlias

private val DEFAULT_RATE = 10000.0

class AllCross(private val rate: Double = DEFAULT_RATE) : DataCharacteristics {

    override fun getRate(relation: Relation): Double = this.rate

    override fun getRate(relationAlias: RelationAlias): Double = this.rate

    override fun setRate(relationAlias: RelationAlias, rate: Double): Unit =
            throw NotImplementedError("Setting Values in AllCross Characteristics does not make sense.")

    override fun getSelectivity(r1: RelationAlias, r2: RelationAlias): Double = 1.0

    override fun getSelectivity(predicate: BinaryPredicate): Double = 1.0

    override fun setSelectivity(r1: RelationAlias, r2: RelationAlias, selectivity: Double): Unit =
            throw NotImplementedError("Setting Values in AllCross Characteristics does not make sense.")

    override fun setSelectivity(predicate: BinaryPredicate, selectivity: Double): Unit =
            throw NotImplementedError("Setting Values in AllCross Characteristics does not make sense.")
}