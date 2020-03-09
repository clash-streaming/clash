package de.unikl.dbis.clash.manager.sampledata

import de.unikl.dbis.clash.manager.api.COMMAND_PING
import de.unikl.dbis.clash.manager.api.COMMAND_RESET
import de.unikl.dbis.clash.manager.api.MESSAGE_PONG
import de.unikl.dbis.clash.manager.api.MESSAGE_RESET
import de.unikl.dbis.clash.manager.api.MESSAGE_TOPOLOGY_ALIVE
import de.unikl.dbis.clash.manager.db.executeMigrations
import de.unikl.dbis.clash.manager.model.ReceivedMessage
import de.unikl.dbis.clash.manager.model.SentCommand
import java.time.Instant

fun main() {
    executeMigrations()

    val t1 = Instant.now()
    val t2 = Instant.now().plusSeconds(20)
    val t3 = Instant.now().plusSeconds(30)
    val t4 = Instant.now().plusSeconds(40)
    val t5 = Instant.now().plusSeconds(50)

    // alive message for start of topology
    val m1 = ReceivedMessage(t1, MESSAGE_TOPOLOGY_ALIVE, "ctrl1")

    // three rounds of ping-pong (1/3)
    val c1 = SentCommand(t2, COMMAND_PING)
    val c1m1 = ReceivedMessage(t2.plusMillis(40), MESSAGE_PONG, "fb1", t2)
    val c1m2 = ReceivedMessage(t2.plusMillis(50), MESSAGE_PONG, "fb2", t2)
    val c1m3 = ReceivedMessage(t2.plusMillis(60), MESSAGE_PONG, "fb3", t2)

    // three rounds of ping-pong (2/3)
    val c2 = SentCommand(t3, COMMAND_PING)
    val c2m1 = ReceivedMessage(t3.plusMillis(30), MESSAGE_PONG, "fb1", t3)
    val c2m2 = ReceivedMessage(t3.plusMillis(52), MESSAGE_PONG, "fb2", t3)
    val c2m3 = ReceivedMessage(t3.plusMillis(60), MESSAGE_PONG, "fb3", t3)

    // three rounds of ping-pong (3/3)
    val c3 = SentCommand(t4, COMMAND_PING)
    val c3m1 = ReceivedMessage(t4.plusMillis(40), MESSAGE_PONG, "fb1", t4)
    val c3m2 = ReceivedMessage(t4.plusMillis(42), MESSAGE_PONG, "fb2", t4)
    val c3m3 = ReceivedMessage(t4.plusMillis(44), MESSAGE_PONG, "fb3", t4)

    // a reset to everybody
    val c4 = SentCommand(t5, COMMAND_RESET)
    val c4m1 = ReceivedMessage(t4.plusMillis(120), MESSAGE_RESET, "fb1", t5)
    val c4m2 = ReceivedMessage(t4.plusMillis(132), MESSAGE_RESET, "fb2", t5)
    val c4m3 = ReceivedMessage(t4.plusMillis(144), MESSAGE_RESET, "fb3", t5)

    // put them into the db
    val commands = listOf(
        c1, c2, c3, c4
    )
    val messages = listOf(
        m1,
        c1m1, c1m2, c1m3,
        c2m1, c2m2, c2m3,
        c3m1, c3m2, c3m3,
        c4m1, c4m2, c4m3
    )

    commands.forEach { it.insert() }
    messages.forEach { it.insert() }
}
