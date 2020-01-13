package de.unikl.dbis.clash.flexstorm

import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values
import org.json.simple.JSONObject
import org.json.simple.parser.JSONParser

typealias TimestampValue = Long
typealias PayloadValue = JSONObject
typealias RelationValue = String

enum class MessageType {
    SpoutOutput,
    Store,
    Probe,
    Control
}

fun getMessageType(tuple: Tuple): MessageType = tuple.getValue(0) as MessageType

val SPOUT_OUTPUT_SCHEMA = Fields("type", "timestamp", "payload", "relation")
fun createSpoutOutput(timestamp: TimestampValue, payload: PayloadValue, relation: RelationValue)
    = Values(MessageType.SpoutOutput, timestamp, payload, relation)
fun getSpoutOutput(tuple: Tuple) = Triple(
    tuple.getLong(1),
    tuple.getValue(2) as JSONObject,
    tuple.getString(3)
)

val STORE_SCHEMA = Fields("type", "timestamp", "payload", "relation")
fun createStoreOutput(timestamp: TimestampValue, payload: PayloadValue, relation: RelationValue)
    = Values(MessageType.Store, timestamp, payload, relation)
fun getStoreOutput(tuple: Tuple) = Triple(
    tuple.getLong(1),
    tuple.getValue(2) as JSONObject,
    tuple.getString(3)
)

val PROBE_SCHEMA = Fields("type", "timestamp", "payload", "relation", "offset")
fun createProbeOutput(timestamp: TimestampValue, payload: PayloadValue, relation: RelationValue, offset: Int)
    = Values(MessageType.Probe, timestamp, payload, relation, offset)
fun getProbeOutput(tuple: Tuple) = ProbeTuple(
    tuple.getLong(1),
    tuple.getValue(2) as JSONObject,
    tuple.getString(3),
    tuple.getInteger(4)
)
data class ProbeTuple(
    val timestamp: TimestampValue,
    val payload: PayloadValue,
    val relation: RelationValue,
    val offset: Int
)

val CONTROL_SCHEMA = Fields("type", "timestamp", "value")
fun createControlOutput(timestamp: TimestampValue, value: String) = Values(MessageType.Control, timestamp, value)
fun getControlOutput(tuple: Tuple) = Pair(tuple.getLong(1), tuple.getString(2))
