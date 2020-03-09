package de.unikl.dbis.clash.estimator

import de.unikl.dbis.clash.datacharacteristics.DataCharacteristics
import de.unikl.dbis.clash.query.Relation
import de.unikl.dbis.clash.query.RelationAlias
import de.unikl.dbis.clash.support.subsetsOfSize

class ClassicEstimator(val dataCharacteristics: DataCharacteristics) {
    fun estimateSize(relation: Relation): Double {
        val rate = relation.inputAliases.map { dataCharacteristics.getRate(it) }.reduce { acc, rate -> acc * rate }
        val relationPairs = relation.inputAliases.toList().subsetsOfSize(2)
        val joinSelectivity = relation.binaryPredicates.map { dataCharacteristics.getSelectivity(it) }.fold(1.0) { acc, rate -> acc * rate }

        return rate * joinSelectivity
    }

    fun estimateSize(relationAliases: List<RelationAlias>): Double {
        val rate = relationAliases.map { dataCharacteristics.getRate(it) }.reduce { acc, rate -> acc * rate }
        val relationPairs = relationAliases.toList().subsetsOfSize(2)
        val joinSelectivity = relationPairs.map {
            val asList = it.toList()
            val x1 = asList.get(0)
            val x2 = asList.get(1)
            dataCharacteristics.getSelectivity(x1, x2)
        }.fold(1.0) { acc, rate -> acc * rate }

        return rate * joinSelectivity
    }
}

fun estimateSize(relation: Relation, dataCharacteristics: DataCharacteristics): Double {
    val estimator = ClassicEstimator(dataCharacteristics)
    return estimator.estimateSize(relation)
}

fun estimateSize(relationAliases: List<RelationAlias>, dataCharacteristics: DataCharacteristics): Double {
    val estimator = ClassicEstimator(dataCharacteristics)
    return estimator.estimateSize(relationAliases)
}
