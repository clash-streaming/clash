package de.unikl.dbis.clash.storm

import de.unikl.dbis.clash.documents.Document
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values

enum class MessageVariant {
    Control,
    DataPath,
    Tick;

    val schema: Fields
        get() {
            return when (this) {
                DataPath -> DataPathMessage.schema
                Control -> ControlMessage.schema
                Tick -> TickMessage.schema
            }
        }

    companion object {

        fun getVariantFor(tuple: Tuple, ruleSet: SiRuleSet): MessageVariant {
            val receivingStream = tuple.sourceStreamId
            val stormInRule = ruleSet.inRuleFor(receivingStream)

            return when(stormInRule) {
                is StormControlInRule -> Control
                is StormIntermediateJoinRule,
                is StormJoinResultRule,
                is StormStoreAndJoinRule,
                is StormRelationReceiveRule -> DataPath
                is StormTickInRule -> Tick
                else -> throw RuntimeException("Cannot handle stormInRule $stormInRule")
            }
        }
    }
}


abstract class AbstractMessage {

    abstract fun asValues(): Values

    open fun asValues(groupingAttribute: String): Values {
        return this.asValues()
    }
}


/**
 * It is possible to sent multiple messages over the data path, which all share sequence numbers and
 * timestamps. See the white paper for details.
 */
abstract class DataPathMessage internal constructor(val seq: Long, val timestamp: Long) : AbstractMessage() {

    abstract override fun asValues(): Values

    abstract override fun asValues(groupingAttribute: String): Values

    companion object {

        val GROUPING_FIELD = "_grouping_key"
        /* message type indicators */
        internal val DOCUMENT_LIST_TYPE = 0
        internal val SINGLE_DOCUMENT_TYPE = 1
        internal val PUNCTUATION_TYPE = 2
        internal val FINISHED_TYPE = 3

        /* Fields and schema */
        private val MESSAGE_TYPE_FIELD = "_type"
        private val MESSAGE_TYPE_INDEX = 0
        private val SEQ_FIELD = "_seq"
        private val SEQ_INDEX = 1
        private val TIMESTAMP_FIELD = "_ts"
        private val TIMESTAMP_INDEX = 2
        private val PAYLOAD_FIELD = "_payload"
        private val PAYLOAD_INDEX = 3
        private val GROUPING_INDEX = 4

        val schema: Fields
            get() = Fields(
                    MESSAGE_TYPE_FIELD,
                    SEQ_FIELD,
                    TIMESTAMP_FIELD,
                    PAYLOAD_FIELD,
                    GROUPING_FIELD
            )

        fun fromTuple(tuple: Tuple): DataPathMessage {
            val seq = tuple.getLong(SEQ_INDEX)!!
            val timestamp = tuple.getLong(TIMESTAMP_INDEX)!!

            val type = tuple.getInteger(MESSAGE_TYPE_INDEX)!!
            return when (type) {
                DOCUMENT_LIST_TYPE -> {
                    val documents = tuple.getValue(PAYLOAD_INDEX) as List<Document>
                    DocumentsMessage(seq, timestamp, documents)
                }
                SINGLE_DOCUMENT_TYPE -> {
                    val document = tuple.getValue(PAYLOAD_INDEX) as Document
                    DocumentsMessage(seq, timestamp, document)
                }
                FINISHED_TYPE -> FinishedMessage(seq, timestamp)
                else -> throw RuntimeException("No Message type $type known")
            }
        }
    }
}


class ControlMessage(instruction: String) : AbstractMessage() {
    val instruction: Instruction

    init {
        this.instruction = Instruction.fromString(instruction)
    }

    override fun asValues(): Values {
        return Values(this.instruction.toString())
    }

    enum class Instruction constructor(private val serInstruction: String) {
        RESET_STATE(RESET_STATE_CODE),
        REPORT(REPORT_CODE);


        companion object {

            internal fun fromString(s: String): Instruction {
                return when (s) {
                    RESET_STATE_CODE -> RESET_STATE
                    REPORT_CODE -> REPORT
                    else -> throw ControlMessageCommandException(s)
                }
            }
        }
    }

    companion object {
        private val RESET_STATE_CODE = "RESET_STATE"
        private val REPORT_CODE = "REPORT"

        /* Fields and schema */
        private val INSTRUCTION_FIELD = "_type"
        private val INSTRUCTION_INDEX = 0

        fun fromTuple(tuple: Tuple): ControlMessage {
            return ControlMessage(tuple.getString(INSTRUCTION_INDEX))
        }

        val schema: Fields
            get() = Fields(INSTRUCTION_FIELD)
    }
}

class ControlMessageCommandException(code: String) : RuntimeException("Don't know how to produce instruction $code")

class TickMessage(val seq: Long, val timestamp: Long) : AbstractMessage() {
    override fun asValues(): Values {
        return Values(seq, timestamp)
    }

    companion object {
        /* Fields and schema */
        private val SEQ_FIELD = "_seq"
        private val SEQ_INDEX = 0
        private val TIMESTAMP_FIELD = "_ts"
        private val TIMESTAMP_INDEX = 1

        fun fromTuple(tuple: Tuple): TickMessage {
            return TickMessage(tuple.getLong(SEQ_INDEX), tuple.getLong(TIMESTAMP_INDEX))
        }

        val schema: Fields
            get() = Fields(SEQ_FIELD, TIMESTAMP_FIELD)
    }
}

class DocumentsMessage(seq: Long,
                       timestamp: Long,
                       val documents: List<Document>) : DataPathMessage(seq, timestamp) {

    constructor(seq: Long,
                timestamp: Long,
                document: Document) : this(seq, timestamp, listOf<Document>(document))

    override fun asValues(groupingAttribute: String): Values {
        val groupingValue = this.documents[0][groupingAttribute] ?: "_"
        return if (documents.size == 1) {
            Values(
                    SINGLE_DOCUMENT_TYPE,
                    this.seq,
                    this.timestamp,
                    this.documents[0],
                    groupingValue
            )
        } else {
            Values(
                    DOCUMENT_LIST_TYPE,
                    this.seq,
                    this.timestamp,
                    this.documents,
                    groupingValue
            )
        }
    }

    override fun asValues(): Values {
        return this.asValues("_")
    }
}


class FinishedMessage(seq: Long, timestamp: Long) : DataPathMessage(seq, timestamp) {
    override fun asValues(groupingAttribute: String): Values {
        return Values(FINISHED_TYPE, this.seq, this.timestamp, 0, groupingAttribute)
    }

    override fun asValues(): Values {
        return asValues("")
    }
}

class PunctuationMessage(seq: Long, timestamp: Long) : DataPathMessage(seq, timestamp) {
    override fun asValues(): Values {
        return asValues("")
    }

    override fun asValues(groupingAttribute: String): Values {
        return Values(PUNCTUATION_TYPE, this.seq, this.timestamp, 0, groupingAttribute)
    }
}
