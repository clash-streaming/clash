package de.unikl.dbis.clash.optimizer

import de.unikl.dbis.clash.datacharacteristics.DataCharacteristics
import de.unikl.dbis.clash.estimator.estimateSize
import de.unikl.dbis.clash.query.Query
import de.unikl.dbis.clash.query.Relation
import de.unikl.dbis.clash.query.RelationAlias
import de.unikl.dbis.clash.query.WindowDefinition
import de.unikl.dbis.clash.support.ceil
import kotlin.math.ceil


fun joinSize(dataCharacteristics: DataCharacteristics, relations: Collection<RelationAlias>): Double {
    return when(relations.size) {
        0 -> 0.0
        1 -> dataCharacteristics.getRate(relations.first())
        else -> {
            val first = relations.first()
            val remainder = relations.minus(first)
            var size = joinSize(dataCharacteristics, remainder)
            for(rel in remainder) {
                size *= dataCharacteristics.getSelectivity(first, rel)
            }
            size * dataCharacteristics.getRate(first)
        }
    }
}



fun joinSize(dataCharacteristics: DataCharacteristics, relation: Relation): Double {
    val baseRelations = relation.aliases.toMutableList()
    return when(baseRelations.size) {
        0 -> 0.0
        1 -> dataCharacteristics.getRate(relation.aliases.first())
        else -> {
            val first = baseRelations.first()
            val remainder = baseRelations.minus(first)

            var size = joinSize(dataCharacteristics, remainder)
            for(rel in remainder) {
                size *= dataCharacteristics.getSelectivity(first, rel)
            }
            size * dataCharacteristics.getRate(first)
        }
    }
}


fun minimalRequiredTasks(query: Query, dataCharacteristics: DataCharacteristics, taskCapacity: Long): Long {
    return minimalRequiredTasksPerMaterializedRelation(query, dataCharacteristics, taskCapacity).values.sum()
}

fun minimalRequiredTasksPerMaterializedRelation(query: Query, dataCharacteristics: DataCharacteristics, taskCapacity: Long): Map<RelationAlias, Long> {
    return tuplesMaterializedPerMaterializedRelation(query, dataCharacteristics, taskCapacity).map { it.key to (it.value / taskCapacity.toDouble()).ceil() }.toMap()
}


fun tuplesMaterializedPerMaterializedRelation(query: Query, dataCharacteristics: DataCharacteristics, taskCapacity: Long): Map<RelationAlias, Long> {
    return query.inputMap.keys.associateBy({ it }, {
        val subRelation = query.result.subRelation(it)
        tuplesMaterializedForRelation(query.result.subRelation(it), dataCharacteristics)
    })
}

fun tuplesMaterializedForRelation(relation: Relation, dataCharacteristics: DataCharacteristics): Long {
    val size = estimateSize(relation, dataCharacteristics)
    val multiplier = relation.windowDefinition.values.minBy { if(it.variant == WindowDefinition.Variant.None) Long.MAX_VALUE else it.amount  }!!.amount
    return if(multiplier == 0L) size.ceil() else (size * multiplier).ceil()
}

fun minimalRequiredTasksForMaterializedRelation(dataCharacteristics: DataCharacteristics, taskCapacity: Long, relationAliases: Collection<RelationAlias>): Int {
    val size = joinSize(dataCharacteristics, relationAliases)
    return ceil(size/taskCapacity).toInt()
}