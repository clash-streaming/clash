package de.unikl.dbis.clash.query

import java.io.Serializable
import java.lang.RuntimeException

/**
 *
 */
data class Aggregation(
    val attributeAccesses: List<AttributeAccess>,
    val aggregationInstructions: List<AggregationOperation>
) : Serializable {
    companion object {

        fun fromStrings(group: List<String>, agg: List<String>): Aggregation {
            val groupAttributeAccesses = group.map {
                AttributeAccess(it)
            }
            val aggregations = agg.map {
                val result = AggregationOperation.fromString(it)
                if (result == null) throw RuntimeException("Could not understand aggregation `$it`")
                result!!
            }
            return Aggregation(groupAttributeAccesses, aggregations)
        }
    }
}

open class AggregationOperation(val attributeAccess: AttributeAccess, val alias: String) : Serializable {
    companion object {
        val aggregationnWithAliasRegex = "(\\w+)\\((\\w+)\\.(\\w+|\\*)\\)\\s+(\\w+)".toRegex()
        val aggregationWithoutAliasRegex = "(\\w+)\\((\\w+)\\.(\\w+)\\)".toRegex()

        fun fromString(string: String): AggregationOperation? {
            fun construct(
                aggregationString: String,
                relationAlias: String,
                attribute: String,
                alias: String
            ): AggregationOperation? {
                val attributeAccess = AttributeAccess(relationAlias, attribute)
                return when (aggregationString.toLowerCase()) {
                    "sum" -> AggregationSum(attributeAccess, alias)
                    "avg" -> AggregationAverage(attributeAccess, alias)
                    "count" -> AggregationCount(attributeAccess, alias)
                    else -> null
                }
            }

            if (string.matches(aggregationnWithAliasRegex)) {
                val matchResult = aggregationnWithAliasRegex.find(string)!!
                return construct(
                    matchResult.groupValues[1],
                    matchResult.groupValues[2],
                    matchResult.groupValues[3],
                    matchResult.groupValues[4]
                )
            }
            if (string.matches(aggregationWithoutAliasRegex)) {
                val matchResult = aggregationWithoutAliasRegex.find(string)!!
                return construct(
                    matchResult.groupValues[1],
                    matchResult.groupValues[2],
                    matchResult.groupValues[3],
                    matchResult.groupValues[3]
                )
            }
            return null
        }
    }
}

class AggregationSum(attributeAccess: AttributeAccess, alias: String) : AggregationOperation(attributeAccess, alias)
class AggregationAverage(attributeAccess: AttributeAccess, alias: String) : AggregationOperation(attributeAccess, alias)
class AggregationCount(attributeAccess: AttributeAccess, alias: String) : AggregationOperation(attributeAccess, alias)
