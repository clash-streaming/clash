package de.unikl.dbis.clash.datacharacteristics

import de.unikl.dbis.clash.query.BinaryPredicate
import de.unikl.dbis.clash.query.Relation
import de.unikl.dbis.clash.query.RelationAlias
import org.json.JSONObject


/**
 * Parses a JSON Object like
 *
 * {
 *   "rates": { "x": 100, "y": 66.67 },
 *   "selectivities": {
 *     "x": {"y": 0.03}
 *   }
 * }
 */
data class SymmetricJSONCharacteristics(
        val rates: Map<RelationAlias, Double>,
        val selectivities: Map<Pair<RelationAlias, RelationAlias>, Double>) : DataCharacteristics {

    constructor(json: JSONObject) : this(ratesFromJson(json), selectivitiesFromJson(json))

    override fun getRate(relation: Relation): Double {
        TODO("Not implemented for arbitrary relations, use an Estimator.")
    }

    override fun getRate(relationAlias: RelationAlias): Double = rates.getValue(relationAlias)

    override fun setRate(relationAlias: RelationAlias, rate: Double): Unit =
            throw NotImplementedError("No need of explicitly setting properties")

    override fun getSelectivity(r1: RelationAlias, r2: RelationAlias): Double =
            this.selectivities[ordered(r1, r2)] ?: 1.0

    override fun getSelectivity(predicate: BinaryPredicate): Double =
            getSelectivity(predicate.leftRelationAlias, predicate.rightRelationAlias)

    override fun setSelectivity(r1: RelationAlias, r2: RelationAlias, selectivity: Double): Unit =
            throw NotImplementedError("No need of explicitly setting properties")

    override fun setSelectivity(predicate: BinaryPredicate, selectivity: Double): Unit =
            throw NotImplementedError("No need of explicitly setting properties")

}

/**
 * From a json object with the format
 *
 * {
 *   "rates": {
 *     "x": 100.0,
 *     "y": 200.0,
 *     "z": 500.0
 *
 *   },
 *   ...
 * }
 *
 * Extracts the map of relation to the according rates
 */
private fun ratesFromJson(json: JSONObject): Map<RelationAlias, Double> {
    val ratesKey = "rates"
    val ratesObject = json[ratesKey]
    val result = mutableMapOf<RelationAlias, Double>()
    when(ratesObject) {
        is JSONObject -> {
            ratesObject.toMap().forEach { rel, rate -> run {
                when(rate) {
                    is Double -> { result[RelationAlias(rel)] = rate }
                    is Number -> { result[RelationAlias(rel)] = rate.toDouble() }
                    is String -> { result[RelationAlias(rel)] = rate.toDouble() }
                }
            }}
        }
    }
    return result
}


/**
 * From a json object with the format
 *
 * {
 *   "selectivities": {
 *     "x": {
 *       "y": 0.04,
 *       "z": 0.001,
 *     }
 *     "y": {
 *       "z": 0.1
 *     }
 *   },
 *   ...
 * }
 *
 * Extracts the map of relation pairs to the according selectivities
 */
private fun selectivitiesFromJson(json: JSONObject): Map<Pair<RelationAlias, RelationAlias>, Double> {
    val selectivitiesKey = "selectivities"
    val selectivitiesObject = json[selectivitiesKey]
    val result = mutableMapOf<Pair<RelationAlias, RelationAlias>, Double>()
    when(selectivitiesObject) {
        is JSONObject -> {
            selectivitiesObject.toMap().forEach { rel_a, inner -> run {
                if (inner is HashMap<*, *>) {
                    inner.forEach { rel_b, selectivity -> run {
                        if(rel_b is String && selectivity is Number) {
                            val aliasA = RelationAlias(rel_a)
                            val aliasB = RelationAlias(rel_b)
                            result[ordered(aliasA, aliasB)] = selectivity.toDouble()
                        }
                        if(rel_b is String && selectivity is String) {
                            val aliasA = RelationAlias(rel_a)
                            val aliasB = RelationAlias(rel_b)
                            result[ordered(aliasA, aliasB)] = selectivity.toDouble()
                        }
                    }}
                }
            }}
        }
    }
    return result
}

private fun ordered(r1: RelationAlias, r2: RelationAlias): Pair<RelationAlias, RelationAlias> {
    return if (r1.inner < r2.inner) Pair(r1, r2) else Pair(r2, r1)
}