package de.unikl.dbis.clash.workers.stores

import de.unikl.dbis.clash.datagenerator.joins.generateTwoRelationKeys
import kotlin.math.roundToLong

const val STORE_TUPLE = false
const val PROBE_TUPLE = true

/**
 * The storeFraction indicates how the ratio between stores and probes is.
 * For example, a storeFraction of 0.6 means, 60 percent of the operations in a window are stores.
 *
 * The windowSize says, how many tuples are stored initially and also
 * where the join partners are located relative to the corresponding probe.
 *
 * The selectivity gives the fraction of how many join partners a single probe has.
 * Note that this differs from the every-day definition of selectivity:
 * Here a selectivity of 1 means, a probe tuple finds windowSize many partners,
 * a selectivity of 1/windowSize means, a probe tuple finds a single partner.
 *
 * TotalTuples gives the number of tuples this generator should produce.
 *
 * In general this generator tries to be as uniform as possible.
 */
class Generator(
    val storeFraction: Double,
    val windowSize: Long,
    val selectivity: Double,
    val totalTuples: Long
) {
    val internalSelectivity = selectivity * 2.0

    /**
     * The Booleans of the output are flags.
     * If it is false, the tuple is a store tuple, otherwise a probe tuple.
     * For example, this output:
     * [(False, t1), (True, t2), (False, t3)]
     * Means, that t1 is to be stored
     */
    fun generate(): List<Pair<Boolean, Tuple>> {
        val result = mutableListOf<Pair<Boolean, Tuple>>()

        // first generate a window full of stores =: S0
        // then a window with probesPerWindow probes that join with S0
        // in the same window with storesPerWindow stores =: S1
        // in the next window probesPerWindow probes that join with S1

        var tsCounter = 0L
        var (stores0, probes) = generateTwoRelationKeys(windowSize, probesPerWindow(), internalSelectivity)
        stores0.forEach { result.add(Pair(STORE_TUPLE, Tuple(it.toString(), tsCounter++, Object()))) }

        for (i in 1 until numberOfWindows()) {
            val (stores, probesNext) = generateTwoRelationKeys(storesPerWindow(), probesPerWindow(), internalSelectivity)

            val (storeStepSize, probeStepSize) = if (storeFraction < 0.5) {
                Pair(1.0, 1.0 / (1.0 / storeFraction - 1))
            } else {
                Pair((1.0 / storeFraction - 1), 1.0)
            }
            var currentStoreStep = 0.0
            var currentStoreFullStep = 1
            var currentProbeStep = 0.0
            var currentProbeFullStep = 1

            val sit = stores.iterator()
            val pit = probes.iterator()
            while (sit.hasNext() || pit.hasNext()) {
                while (currentStoreStep < currentStoreFullStep) {
                    currentStoreStep += storeStepSize
                    if (sit.hasNext()) {
                        val key = sit.next()
                        result.add(Pair(STORE_TUPLE, Tuple(key.toString(), tsCounter++, Object())))
                    }
                }
                currentStoreFullStep += 1
                while (currentProbeStep < currentProbeFullStep) {
                    currentProbeStep += probeStepSize
                    if (pit.hasNext()) {
                        val key = pit.next()
                        result.add(Pair(PROBE_TUPLE, Tuple(key.toString(), tsCounter++, Object())))
                    }
                }
                currentProbeFullStep += 1
            }

            probes = probesNext
        }

        return result
    }

    fun probesPerWindow() = ((1 - storeFraction) * windowSize).roundToLong()
    fun storesPerWindow() = (storeFraction * windowSize).roundToLong()
    fun numberOfWindows() = totalTuples / windowSize
}

fun main() {
    val x = Generator(0.7, 10000, 0.001, 20000)

    val generatedTuples = x.generate()
    val storeTuples = generatedTuples.filter { it.first == STORE_TUPLE }
    val probeTuples = generatedTuples.filter { it.first == PROBE_TUPLE }

    println("Genrated ${generatedTuples.size} tuples, ${storeTuples.size} for storing and ${probeTuples.size} for probing.")
    var joinResults = 0

    for ((_, s) in storeTuples) {
        for ((_, p) in probeTuples) {
            if (p.key1 == s.key1 && p.ts - x.windowSize >= s.ts) {
                joinResults += 1
            }
        }
    }
    val actualSelectivity = joinResults.toDouble() / (storeTuples.size.toDouble() * probeTuples.size.toDouble())

    println("The join selectivity should be ${x.selectivity} and is actually $actualSelectivity")
}
