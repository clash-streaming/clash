package de.unikl.dbis.clash.datacharacteristics

import de.unikl.dbis.clash.query.BinaryPredicate
import de.unikl.dbis.clash.query.Relation
import de.unikl.dbis.clash.query.RelationAlias
import java.util.HashMap


fun sort(r1: RelationAlias, r2: RelationAlias): Array<RelationAlias> {
    val comparison = r1.inner.compareTo(r2.inner)
    return when {
        comparison < 0 -> arrayOf(r1, r2)
        comparison == 0 -> throw RuntimeException(
                "Sorting selectivity between a stream with itself is utterly undefined.")
        else -> arrayOf(r2, r1)
    }
}

class ManualCharacteristics : DataCharacteristics {
    private val rates = HashMap<RelationAlias, Double>()
    private val predicatewiseSelectivities = HashMap<BinaryPredicate, Double>()
    private val pairwiseSelectivities = HashMap<RelationAlias, MutableMap<RelationAlias, Double>>()

    override fun getRate(relation: Relation): Double {
        TODO("Not implemented for arbitrary relations, use an Estimator.")
    }

    override fun getRate(relationAlias: RelationAlias): Double = rates[relationAlias]!!

    override fun setRate(relationAlias: RelationAlias, rate: Double) {
        rates[relationAlias] = rate
    }

    override fun getSelectivity(r1: RelationAlias, r2: RelationAlias): Double {
        val sorted = sort(r1, r2)
        val ss1 = sorted[0]
        val ss2 = sorted[1]

        val innerMap = pairwiseSelectivities[ss1] ?: mutableMapOf()
        return innerMap[ss2] ?: 1.0
    }

    /**
     * If no predicate was registered, returns the estimation for the stream names.
     *
     * @param predicate the join predicate
     * @return the selectivity of the join over this predicate
     */
    override fun getSelectivity(predicate: BinaryPredicate): Double =
            predicatewiseSelectivities.getOrDefault(predicate, getSelectivity(predicate.leftRelationAlias, predicate.rightRelationAlias))

    override fun setSelectivity(r1: RelationAlias, r2: RelationAlias, selectivity: Double) {
        val (ss1, ss2) = sort(r1, r2)
        val innerMap = pairwiseSelectivities.getOrPut(ss1) { mutableMapOf() }
        innerMap[ss2] = selectivity
    }

    override fun setSelectivity(predicate: BinaryPredicate, selectivity: Double) {
        predicatewiseSelectivities[predicate] = selectivity
    }

    /**
     * Computes the output rate of the join between all relation with given names.
     *
     * @param names base relation that are joined together
     * @return rate of the join between names
     */
    fun computeJoinSize(vararg names: RelationAlias): Double {
        var rate = this.getRate(names[0])
        for (i in 1 until names.size) {
            rate *= this.getRate(names[i])

            for (j in 0 until i) {
                rate *= this.getSelectivity(names[i], names[j])
            }
        }
        return rate
    }
}