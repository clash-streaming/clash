package de.unikl.dbis.clash.workers.stores

import de.unikl.dbis.clash.ClashConfig
import de.unikl.dbis.clash.documents.Document
import de.unikl.dbis.clash.physical.BinaryPredicateEvaluationLeftStored
import de.unikl.dbis.clash.query.AttributeAccess
import de.unikl.dbis.clash.query.BinaryEquality

/**
 * Measure the naive hash store.
 *
 * Parameters: M, N, O, P, D_S, D_P
 *
 * We first fill the store with M objects.
 * Then we alternate P times between storing N and probing O tuples.
 *
 * For example, M = 2, P = 3, N = O = 1:
 *
 *   - initialStore
 *   - initialStore
 *   - store
 *   - probe
 *   - store
 *   - probe
 *   - store
 *   - probe
 *
 * Each document-list that is stored contains D_S many documents,
 * each probed one contains D_P many documents.
 *
 */
fun main() {
    val results = mutableListOf<Long>()
    repeat(10) {
        val config = ClashConfig()
        config[ClashConfig.CLASH_PROBE_LOG_ENABLED] = false

        val initialStores = 1000
        val repeat = 5000
        val stores = 1
        val probes = 1
        val storeLength = 1
        val probeLength = 1

//        val store = NaiveNestedLoopStore<EmptyReceiver>(config) // 4181
        val store = NaiveHashStore<EmptyReceiver>(config)
        val tm = RandomDocMeasurement(initialStores, repeat, stores, probes, storeLength, probeLength, store)

        val res = tm.run()
        println("Run took $res")
        results += res
    }
    println("Average runtime ${results.average()}")
}

class RandomDocMeasurement(
    val initialStores: Int,
    val repeat: Int,
    val stores: Int,
    val probes: Int,
    val storeLength: Int,
    val probeLength: Int,
    val store: ActualStore<EmptyReceiver>
) {
    var currentAts = 0L

    val predicates = setOf(BinaryPredicateEvaluationLeftStored(BinaryEquality(AttributeAccess("x.a"), AttributeAccess("y.a"))))
    val receivers = setOf<EmptyReceiver>()

    val rdg = RandomDocumentGenerator()

    fun run(): Long {
        println("I will do now ${initialStores + repeat * stores} stores and ${repeat * probes} probes.")

        val time = System.currentTimeMillis()

        repeat(initialStores) {
            store.store(currentAts++, initialStore())
        }

        repeat(repeat) {
            repeat(stores) {
                val storeResult = store()
                store.store(storeResult.ats, storeResult.documents)
            }
            repeat(probes) {
                val probeResult = probe()
                store.probe(probeResult.ats, probeResult.ats, probeResult.documents, predicates, receivers)
            }
        }

        return System.currentTimeMillis() - time
    }

    fun initialStore(): List<Document> {
        return listOf(rdg.get())
    }

    fun store(): StoreResult {
        val storeDocuments = mutableListOf<Document>()
        repeat(storeLength) {
            storeDocuments += rdg.get()
        }
        return StoreResult(currentAts++, storeDocuments)
    }

    fun probe(): ProbeResult {
        val probeDocuments = mutableListOf<Document>()
        repeat(probeLength) {
            probeDocuments += rdg.get()
        }
        return ProbeResult(currentAts++, probeDocuments)
    }
}
