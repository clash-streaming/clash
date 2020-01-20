package de.unikl.dbis.clash.manager.db

fun main() {
    executeMigrations()
}

val migrations = sortedMapOf(
    1 to """
        CREATE TABLE sent_commands(
            timestamp DATETIME NOT NULL,
            command TEXT NOT NULL,
            payload TEXT -- json object
        );
        CREATE TABLE received_messages(
            timestamp DATETIME NOT NULL,
            message TEXT NOT NULL,
            sender TEXT NOT NULL,
            answer_to DATETIME,
            payload TEXT -- json object
        );
    """.trimIndent()
)
