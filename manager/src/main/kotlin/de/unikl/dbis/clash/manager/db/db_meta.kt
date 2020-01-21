package de.unikl.dbis.clash.manager.db

import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.time.Instant
import java.time.ZoneId

import java.time.format.DateTimeFormatter

fun<T>statement(block: (Statement) -> T): T {
    return connection {
        val statement = it.createStatement()
        block.invoke(statement)
    }
}

fun<T>connection(block: (Connection) -> T): T {
    try {
        Class.forName("org.sqlite.JDBC")
    } catch (e: ClassNotFoundException) {
        error("Please install sqlite java drivers.")
    }

    return DriverManager.getConnection("jdbc:sqlite:$SQLITE_DB_PATH").use(block)
}

fun Instant.toSqlite(): String {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
        .withZone(ZoneId.systemDefault())
    return formatter.format(this)
}
