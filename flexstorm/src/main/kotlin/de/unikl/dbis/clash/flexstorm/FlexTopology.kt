package de.unikl.dbis.clash.flexstorm

import de.unikl.dbis.clash.flexstorm.control.CONTROL_BOLT_NAME
import de.unikl.dbis.clash.flexstorm.control.CONTROL_SPOUT_NAME
import de.unikl.dbis.clash.flexstorm.control.CONTROL_SPOUT_TO_ALL_STREAM_NAME
import de.unikl.dbis.clash.flexstorm.control.CONTROL_SPOUT_TO_CONTROL_BOLT_STREAM_NAME
import de.unikl.dbis.clash.flexstorm.control.ControlBolt
import de.unikl.dbis.clash.flexstorm.control.ControlSpout
import de.unikl.dbis.clash.flexstorm.control.FORWARD_TO_CONTROL_BOLT_STREAM_NAME
import de.unikl.dbis.clash.flexstorm.partitioning.NaiveHashPartitioning
import org.apache.storm.Config
import org.apache.storm.LocalCluster
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.utils.Utils
import java.time.Instant

const val STORE_STREAM_ID = "store"
const val PROBE_STREAM_ID = "probe"
const val NUMBER_OF_FLEX_BOLTS = 3

/**
 * Compute the join:
 * R(a,b), S(c,d), T(e,f), b=c, d=e
 */
fun main() {
    val builder = TopologyBuilder()
    val startTime = Instant.now().plusSeconds(5)
    // val rSpout = createRSpout(startTime)
    // val sSpout = createSSpout(startTime)
    // val tSpout = createTSpout(startTime)
    val rSpout = KafkaSpout("R", "R")
    val sSpout = KafkaSpout("S", "S")
    val tSpout = KafkaSpout("T", "T")


    val clashState = ClashState()
    clashState.numberOfFlexBolts = NUMBER_OF_FLEX_BOLTS
    clashState.epochs = theEpoch()

    val bolt = FlexBolt(clashState)

    builder.setSpout(CONTROL_SPOUT_NAME,
        ControlSpout("http://localhost:8080")
    )

    builder.setSpout("r", rSpout)
    builder.setSpout("s", sSpout)
    builder.setSpout("t", tSpout)
    builder.setBolt(FLEX_BOLT_NAME, bolt)
        .shuffleGrouping("r")
        .shuffleGrouping("s")
        .shuffleGrouping("t")
        .directGrouping(FLEX_BOLT_NAME, STORE_STREAM_ID)
        .directGrouping(FLEX_BOLT_NAME, PROBE_STREAM_ID)
        .allGrouping(CONTROL_SPOUT_NAME, CONTROL_SPOUT_TO_ALL_STREAM_NAME)
        .setNumTasks(NUMBER_OF_FLEX_BOLTS)
    builder.setBolt(CONTROL_BOLT_NAME, ControlBolt("http://localhost:8080", clashState))
        .allGrouping(CONTROL_SPOUT_NAME, CONTROL_SPOUT_TO_CONTROL_BOLT_STREAM_NAME)
        .allGrouping(FLEX_BOLT_NAME, FORWARD_TO_CONTROL_BOLT_STREAM_NAME)
        .allGrouping(CONTROL_SPOUT_NAME, FORWARD_TO_CONTROL_BOLT_STREAM_NAME)

    val localCluster = LocalCluster()
    val conf = Config()
    localCluster.submitTopology("testTopology", conf, builder.createTopology())
    Utils.sleep(20_000)
    localCluster.killTopology("testTopology")
    localCluster.close()
}

fun createRSpout(startTime: Instant) = TestSpout(
    startTime,
    "R",
    mapOf(
        1000L to """{"a": "R1", "b": "1"}""",
        1500L to """{"a": "R2", "b": "2"}""",
        1700L to """{"a": "R3", "b": "3"}""",
        1900L to """{"a": "R4", "b": "4"}""",
        2200L to """{"a": "R5", "b": "5"}""",
        2900L to """{"a": "R6", "b": "6"}""",
        3700L to """{"a": "R7", "b": "7"}""",
        3900L to """{"a": "R8", "b": "7"}"""
    )
)

fun createSSpout(startTime: Instant) = TestSpout(
    startTime,
    "S",
    mapOf(
        1100L to """{"c": "1", "d": "101"}""",
        1300L to """{"c": "3", "d": "102"}""",
        1800L to """{"c": "3", "d": "103"}""",
        2100L to """{"c": "3", "d": "104"}""",
        2600L to """{"c": "11", "d": "103"}""",
        2901L to """{"c": "6", "d": "105"}""",
        3400L to """{"c": "7", "d": "106"}""",
        3600L to """{"c": "7", "d": "222"}"""
    )
)

fun createTSpout(startTime: Instant) = TestSpout(
    startTime,
    "T",
    mapOf(
        1200L to """{"e": "101", "f": "T1"}""",
        1400L to """{"e": "201", "f": "T2"}""",
        2000L to """{"e": "103", "f": "T3"}""",
        2300L to """{"e": "202", "f": "T4"}""",
        2500L to """{"e": "105", "f": "T5"}""",
        3001L to """{"e": "105", "f": "T6"}""",
        3100L to """{"e": "203", "f": "T7"}""",
        3300L to """{"e": "106", "f": "T8"}"""
    )
)

fun theEpoch(): Epochs {
    val rProbeOrder = ProbeOrder(listOf(
        ProbeOrderEntry("b", "S", "c"),
        ProbeOrderEntry("d", "T", "e")
    ))
    val sProbeOrder = ProbeOrder(listOf(
        ProbeOrderEntry("d", "T", "e"),
        ProbeOrderEntry("c", "R", "b")
    ))
    val tProbeOrder = ProbeOrder(listOf(
        ProbeOrderEntry("e", "S", "d"),
        ProbeOrderEntry("c", "R", "b")
    ))

    val probeOrders = mapOf("R" to rProbeOrder, "S" to sProbeOrder, "T" to tProbeOrder)
    val partitioning = mapOf(
        "R" to NaiveHashPartitioning(3, "b"),
        "S" to NaiveHashPartitioning(3, "c"),
        "T" to NaiveHashPartitioning(3, "e"))
    val accessRules = mapOf(
        "R" to listOf(AccessRule("b")),
        "S" to listOf(AccessRule("c"), AccessRule("d")),
        "T" to listOf(AccessRule("e"))
    )

    val epoch = Epoch(probeOrders, partitioning, accessRules)
    val epochs = Epochs()
    epochs.addEpoch(0, epoch)
    return epochs
}
