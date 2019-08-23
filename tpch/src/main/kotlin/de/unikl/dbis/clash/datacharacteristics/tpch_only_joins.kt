package de.unikl.dbis.clash.datacharacteristics

import de.unikl.dbis.clash.query.BinaryPredicate
import de.unikl.dbis.clash.query.Relation
import de.unikl.dbis.clash.query.RelationAlias


class TpchOnlyJoinsCharacteristics
internal constructor(query: String) : DataCharacteristics {
    private val rates = mutableMapOf<RelationAlias, Double>()
    private val selectivities = mutableMapOf<Pair<RelationAlias, RelationAlias>, Double>()

    override fun getRate(relation: Relation): Double {
        TODO("Not implemented for arbitrary relations, use an Estimator.")
    }

    init {
        when (query) {
            Q2 -> initializeQ2()
            Q3 -> initializeQ3()
            Q5 -> initializeQ5()
            Q11 -> initializeQ11()
        }
    }

    override fun getRate(relationAlias: RelationAlias): Double = rates[relationAlias]!!

    override fun setRate(relationAlias: RelationAlias, rate: Double): Unit =
            throw RuntimeException("Not implemented")

    override fun getSelectivity(r1: RelationAlias, r2: RelationAlias): Double =
            if (r1.inner > r2.inner) {
                getSelectivity(r2, r1)
            } else {
                selectivities[Pair(r1, r2)] ?: 1.0
            }

    override fun getSelectivity(predicate: BinaryPredicate): Double =
            this.getSelectivity(predicate.leftAttributeAccess.relationAlias, predicate.rightAttributeAccess.relationAlias)

    override fun setSelectivity(r1: RelationAlias, r2: RelationAlias, selectivity: Double) {
        if (r1.inner > r2.inner) {
            setSelectivity(r2, r1, selectivity)
            return
        }

        selectivities[Pair(r1, r2)] = selectivity
    }

    override fun setSelectivity(predicate: BinaryPredicate, selectivity: Double): Unit =
            throw RuntimeException("Not implemented")

    private fun initializeQ2() {
        this.rates[RelationAlias("part")] = 200_000.0
        this.rates[RelationAlias("partsupp")] = 800_000.0
        this.rates[RelationAlias("supplier")] = 10_000.0
        this.rates[RelationAlias("nation")] = 25.0
        this.rates[RelationAlias("region")] = 5.0
        this.setSelectivity(RelationAlias("part"), RelationAlias("partsupp"), 0.000005)
        this.setSelectivity(RelationAlias("partsupp"), RelationAlias("supplier"), 0.0001)
        this.setSelectivity(RelationAlias("supplier"), RelationAlias("nation"), 0.04)
        this.setSelectivity(RelationAlias("nation"), RelationAlias("region"), 0.2)
    }

    private fun initializeQ3() {
        this.rates[RelationAlias("customer")] = 150_000.0
        this.rates[RelationAlias("orders")] = 1_500_000.0
        this.rates[RelationAlias("lineitem")] = 6_001_215.0
        this.setSelectivity(RelationAlias("customer"), RelationAlias("orders"), 0.0000066667)
        this.setSelectivity(RelationAlias("orders"), RelationAlias("lineitem"), 0.0000006667)
    }

    private fun initializeQ5() {
        this.rates[RelationAlias("customer")] = 150_000.0
        this.rates[RelationAlias("orders")] = 1_500_000.0
        this.rates[RelationAlias("lineitem")] = 6_001_215.0
        this.rates[RelationAlias("supplier")] = 10_000.0
        this.rates[RelationAlias("nation")] = 25.0
        this.rates[RelationAlias("region")] = 5.0
        this.setSelectivity(RelationAlias("customer"), RelationAlias("orders"), 0.0000066667)
        this.setSelectivity(RelationAlias("orders"), RelationAlias("lineitem"), 0.0000006667)
        this.setSelectivity(RelationAlias("lineitem"), RelationAlias("supplier"), 0.0001)
        this.setSelectivity(RelationAlias("customer"), RelationAlias("nation"), 0.04)
        this.setSelectivity(RelationAlias("supplier"), RelationAlias("nation"), 0.04)
        this.setSelectivity(RelationAlias("nation"), RelationAlias("region"), 0.2)
    }

    private fun initializeQ11() {
        this.rates[RelationAlias("partsupp")] = 800_000.0
        this.rates[RelationAlias("supplier")] = 10_000.0
        this.rates[RelationAlias("nation")] = 5.0
        this.setSelectivity(RelationAlias("partsupp"), RelationAlias("supplier"), 0.0001)
        this.setSelectivity(RelationAlias("supplier"), RelationAlias("nation"), 0.04)
    }

    companion object {
        val Q2 = "Q2"
        val Q3 = "Q3"
        val Q5 = "Q5"
        val Q11 = "Q11"

        fun q2(): DataCharacteristics {
            return TpchOnlyJoinsCharacteristics(Q2)
        }

        fun q3(): DataCharacteristics {
            return TpchOnlyJoinsCharacteristics(Q3)
        }

        fun q5(): DataCharacteristics {
            return TpchOnlyJoinsCharacteristics(Q5)
        }

        fun q11(): DataCharacteristics {
            return TpchOnlyJoinsCharacteristics(Q11)
        }
    }

}