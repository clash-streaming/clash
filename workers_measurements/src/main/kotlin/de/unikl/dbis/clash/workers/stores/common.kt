package de.unikl.dbis.clash.workers.stores

import de.unikl.dbis.clash.documents.Document
import de.unikl.dbis.clash.query.AttributeAccess

class EmptyReceiver

data class StoreResult(
        val ats: Long,
        val documents: List<Document>
)

data class ProbeResult(
        val ats: Long,
        val documents: List<Document>
)


class RandomDocumentGenerator {
    val accesses = listOf(AttributeAccess("R.x"), AttributeAccess("R.y"), AttributeAccess("R.z"))
    val accessesLength = accesses.size
    var accessPointer = 0
    val values = listOf("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m")
    val valuesLength = values.size
    var valuesPointer = 0

    fun get(): Document {
        val d = Document() // // with: 9063 without: 5772
//        d[accesses[accessPointer++]] = values[valuesPointer++]
//
//        accessPointer %= accessesLength
//        valuesPointer %= valuesLength

        return d
    }
}
