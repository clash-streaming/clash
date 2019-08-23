package de.unikl.dbis.clash.storm.spouts

import de.unikl.dbis.clash.documents.Document
import org.apache.storm.utils.Utils
import java.util.*


class ExplicitTupleSpout : CommonSpout() {

    private val documents = Stack<Document>()

    override fun nextTuple() {
        try {
            val document = this.documents.pop()
            super.emit(document)
        } catch (e: EmptyStackException) {
            Utils.sleep(60000)
        }

    }

    fun put(document: Document): ExplicitTupleSpout {
        documents.push(document)
        return this
    }
}
