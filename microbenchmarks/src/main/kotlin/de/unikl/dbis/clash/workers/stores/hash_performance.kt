package de.unikl.dbis.clash.workers.stores

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.choice
import com.github.ajalt.clikt.parameters.types.double
import com.github.ajalt.clikt.parameters.types.long
import de.unikl.dbis.clash.ClashConfig
import de.unikl.dbis.clash.documents.TupleKey1Ts
import de.unikl.dbis.clash.physical.BinaryPredicateEvaluationLeftStored
import de.unikl.dbis.clash.query.AttributeAccess
import de.unikl.dbis.clash.query.BinaryEquality
import de.unikl.dbis.clash.query.RelationAlias
import java.lang.RuntimeException
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.math.roundToInt
import kotlin.random.Random

val rAttr = AttributeAccess("r.a")
val sAttr = AttributeAccess("s.a")
val wSize = 1_000
val rng = Random(12345)

typealias Tuple = TupleKey1Ts<String, Any>

/**
 * Call this with the following arguments:
 *
 * --storeFraction X    how many tuples in a window should be stores? X \in [0,1]
 * --windowSize X       how many tuples should be contained in a window? X \in N
 * --selectivity X      how many join results should be created? X \in [0,1]
 * --totalTuples X      how many tuples should be generated in total?
 * --method X           which method is used for joining? The following methods are supported:
 *                       - LibraryHashMapKeyedByAttribute
 *                       - LibraryTreeMapKeyedByTimeStamp
 *
 * Note that the first window will be filled with stores only,
 * so setting totalTuples == windowSize is probably a bad idea.
 */
fun main(args: Array<String>) = HashPerformance().main(args)

class HashPerformance : CliktCommand() {
    val storeFraction by option("-s", "--storeFraction").double().required()
    val windowSize by option("-w", "--windowSize").long().required()
    val selectivity by option("-f", "--selectivity").double().required()
    val totalTuples by option("-t", "--totalTuples").long().required()
    val method by option("-m", "--method").choice(
            "LibraryHashMapKeyedByAttribute",
            "LibraryTreeMapKeyedByTimeStamp"
    ).required()
    val verbose by option("-v", "--verbose").flag()


    override fun run() {
        printv("Experiment:")
        printv("- windowSize is $windowSize:")
        printv("- in a window $storeFraction of tuples are stores")
        printv("- the selectivity is $selectivity")

        val timeBeforePrepare = System.currentTimeMillis()
        val documents = prepare(storeFraction, windowSize, selectivity, totalTuples)
        val timeAfterPrepare = System.currentTimeMillis()
        printv("Prepared $totalTuples tuples in ${timeAfterPrepare - timeBeforePrepare} ms")

        val wrapper = getWrapper()

        val timeBefore = System.currentTimeMillis()
        var resultCounter = 0L
        for(document in documents) {
            if(document.first == STORE_TUPLE) {
                wrapper.store(document.second)
            } else {
                resultCounter += wrapper.probe(document.second).size
            }
        }
        val timeAfter = System.currentTimeMillis()
        val duration = timeAfter - timeBefore
        printv("Finished in $duration milliseconds.")
        printv("In total, $resultCounter join results were observed.")
        printv("This means, per action in average ${duration.toDouble() / totalTuples} ms per operation.")

        println("""
            {
              "storeFraction": $storeFraction,
              "windowSize": $windowSize,
              "selectivity": $selectivity,
              "totalTuples": $totalTuples,
              "method": "$method",
              "setupTime": ${timeAfterPrepare - timeBeforePrepare},
              "experimentTime": $duration,
              "joinResults": $resultCounter,
              "averageDuration": ${duration.toDouble() / totalTuples}
            }
        """.trimIndent())
    }

    fun printv(string: String) {
        if(verbose)
            println(string)
    }

    fun getWrapper(): Wrapper {
        return when(method) {
            "LibraryTreeMapKeyedByTimeStamp" -> LibraryTreeMapKeyedByTimeStampWrapper()
            "LibraryHashMapKeyedByAttribute" -> LibraryHashMapKeyedByAttributeWrapper()
            else -> throw RuntimeException("No method with name `$method' is available.")
        }
    }
}


fun prepare(storeFraction: Double,
            windowSize: Long,
            selectivity: Double,
            totalTuples: Long): List<Pair<Boolean, Tuple>> {
    val generator = Generator(storeFraction, windowSize, selectivity, totalTuples)
    return generator.generate()
}

fun main1(initialStores: Int, rounds: Int, stores: Int, probes: Int, storeDocuments: List<Tuple>, probeDocuments: List<Tuple>) {
    val theIndex = HashMap<String, Tuple>()
}

fun main2(initialStores: Int, rounds: Int, stores: Int, probes: Int, storeDocuments: List<Tuple>, probeDocuments: List<Tuple>) {
    val config = ClashConfig()
    val hashStore = NaiveHashStore<Any>(config)

    val relationAlias = RelationAlias("r")
    val laterProbe = listOf("")

    val predicate = BinaryEquality(AttributeAccess("r", "a"), AttributeAccess("r", "b"))
    val predicateEvaluation = BinaryPredicateEvaluationLeftStored(predicate)
}

interface Wrapper {
    fun store(tuple: Tuple);
    fun probe(tuple: Tuple): List<Tuple>;
}

/**
 * Here we are storing the tuples in a default java hash map.
 * The key is the join attribute.
 */
class LibraryHashMapKeyedByAttributeWrapper() : Wrapper {
    private val theIndex = HashMap<String, MutableList<Tuple>>()

    override fun store(tuple: Tuple) {
        val key = tuple.key1
        theIndex.putIfAbsent(key, mutableListOf())
        theIndex[key]!!.add(tuple)
    }

    override fun probe(tuple: Tuple): List<Tuple> {
        val key = tuple.key1
        val result = theIndex.getOrDefault(key, listOf<Tuple>()).filter { it.ts > tuple.ts - wSize }.toList()
        return result
    }
}

class LibraryTreeMapKeyedByTimeStampWrapper() : Wrapper {
    private val theIndex = TreeMap<Long, Tuple>()
    override fun store(tuple: Tuple) {
        val key = tuple.ts
        theIndex[key] = tuple
    }

    override fun probe(tuple: Tuple): List<Tuple> {
        val potentialJoinPartners = theIndex.subMap(tuple.ts - wSize, tuple.ts)
        return potentialJoinPartners.values.filter { it.key1 == tuple.key1 }
    }
}