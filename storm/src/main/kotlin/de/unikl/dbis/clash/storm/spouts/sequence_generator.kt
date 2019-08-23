package de.unikl.dbis.clash.storm.spouts

import de.unikl.dbis.clash.documents.Document
import de.unikl.dbis.clash.query.RelationAlias
import org.apache.storm.utils.Utils
import java.io.Serializable
import java.util.HashMap


class SequenceGeneratorSpout(val baseRelation: RelationAlias, val delay: Long, val fields: Map<String, SequenceGeneratorField>) : CommonSpout(delay.toInt()) {

    private val fieldState = HashMap<String, FieldState>()

    private var alreadyFinished = false

    init {

        for (key in fields.keys) {
            this.fieldState[key] = FieldState(fields.getValue(key))
        }
    }

    override fun nextTuple() {
        if (this.alreadyFinished) {
            Utils.sleep(5000)
            return
        }

        val document = Document()
        for (key in this.fieldState.keys) {
            val state = this.fieldState[key]!!
            if (state.hasNext()) {
                document[baseRelation, key] = "" + state.next()
            } else {
                this.alreadyFinished = true
                break
            }
        }

        if (!this.alreadyFinished) {
            super.emit(document)
        }
        super.delayNext()
    }

    override fun toString(): String {
        return "SequenceGeneratorSpout"
    }
}

class FieldState private constructor(private var currentValue: Int, private val end: Int, private val step: Int, private var repetitionsForThisValue: Int) : Serializable {
    private val repeat = repetitionsForThisValue

    constructor(sequenceGeneratorField: SequenceGeneratorField) : this(sequenceGeneratorField.start, sequenceGeneratorField.end, sequenceGeneratorField.step,
            sequenceGeneratorField.repeat)

    operator fun hasNext(): Boolean {
        return this.currentValue <= this.end
    }

    operator fun next(): Int {
        val resultValue = currentValue

        repetitionsForThisValue--
        if (repetitionsForThisValue == 0) {
            currentValue += step
            repetitionsForThisValue = repeat
        }

        return resultValue
    }
}

class SequenceGeneratorField
/**
 * Generates a new sequence generator field.
 *
 * @param start the number at which the generator starts
 * @param end the number at which the generator ends
 * @param step the step between each number
 * @param repeat how often the same value is output
 */
(val start: Int, val end: Int, val step: Int, val repeat: Int) : Serializable
