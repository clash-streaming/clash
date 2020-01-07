package de.unikl.dbis.clash.datagenerator.joins

import kotlin.math.roundToLong

/**
 * Generates two list of keys of sizes sizeR1 and sizeR2 such that the join result of these keys
 * is approximately sizeR1 * sizeR2 * selectivity.
 */
fun generateTwoRelationKeys(sizeR1: Long, sizeR2: Long, selectivity: Double): Pair<List<Long>, List<Long>> {
    val (smallerSize, largerSize) = if (sizeR1 < sizeR2) Pair(sizeR1, sizeR2) else Pair(sizeR2, sizeR1)

    var smallerKey = 0.0
    var largerKey = 0.0
    val smallerKeys = mutableListOf<Long>()
    val largerKeys = mutableListOf<Long>()
    val smallerStepSize = (1.0 / (selectivity * smallerSize))
    val largerStepSize = (1.0 / (selectivity * largerSize))

    for (i in 0 until smallerSize) {
        smallerKey += smallerStepSize
        val addedKey = smallerKey.roundToLong()
        smallerKeys.add(addedKey)
    }

    for (i in 0 until largerSize) {
        largerKey += largerStepSize
        val addedKey = largerKey.roundToLong()
        largerKeys.add(addedKey)
    }

    return if (sizeR1 < sizeR2) Pair(smallerKeys, largerKeys) else Pair(largerKeys, smallerKeys)
}
