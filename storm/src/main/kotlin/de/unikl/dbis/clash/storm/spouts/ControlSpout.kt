package de.unikl.dbis.clash.storm.spouts

import de.unikl.dbis.clash.query.InputName
import de.unikl.dbis.clash.query.RelationAlias
import de.unikl.dbis.clash.storm.ControlMessage
import de.unikl.dbis.clash.storm.ControlMessageCommandException
import de.unikl.dbis.clash.support.KafkaConfig

class ControlSpout(topicName: String, config: KafkaConfig) : KafkaSpout(InputName("NONE"), RelationAlias("NONE"), topicName, config) {

    override fun nextTuple() {
        val records = this.consumer!!.poll(100)
        for (record in records) {
            val instruction = record.value()

            // Attention: Here sequence numbers are maybe unnecessary
            try {
                val message = ControlMessage(instruction)
                super.emit(message)
            } catch (e: ControlMessageCommandException) {
                System.err.println(e)
            }
        }
    }
}
