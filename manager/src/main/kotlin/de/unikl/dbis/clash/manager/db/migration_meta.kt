package de.unikl.dbis.clash.manager.db

import java.sql.Connection
import java.sql.DriverManager

const val SCHEMA_VERSION = "schema_version"
const val SQLITE_DB_PATH = "manager.db"

fun executeMigrations() {
    val schemaBefore = currentSchemaVersion()
    println("Current schema version is $schemaBefore")
    migrations.tailMap(schemaBefore + 1).forEach {
        executeMigration(it.key, it.value)
        println("Executing migration #${it.key} with text: ${it.value}")
    }
    val schemaAfter = currentSchemaVersion()
    println("After running all due migrations, schema version is $schemaAfter.")
}

fun executeMigration(version: Int, sqlString: String) {
    connection {
        val stmt = it.createStatement()
        stmt.executeUpdate(sqlString)
    }
    setSchemaVersionTo(version)
}

fun currentSchemaVersion(): Int {
    return connection {
        if(!schemaTableExists()) {
            createSchemaTable()
        }

        val stmt = it.createStatement()
        val result = stmt.executeQuery("SELECT MAX($SCHEMA_VERSION) FROM $SCHEMA_VERSION LIMIT 1")
        result.next()
        val version = result.getInt(1)
        version
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

fun schemaTableExists(): Boolean {
    return connection {
        val stmt = it.createStatement()
        val result = stmt.executeQuery("SELECT name FROM sqlite_master WHERE type='table' AND name='$SCHEMA_VERSION';")
        result.next()
    }
}

fun createSchemaTable() {
    connection {
        val stmt = it.createStatement()
        stmt.executeUpdate("CREATE TABLE $SCHEMA_VERSION ($SCHEMA_VERSION int primary key);")
        setSchemaVersionTo(-1)
    }
}

fun setSchemaVersionTo(version: Int) {
    connection {
        val stmt = it.createStatement()
        stmt.executeUpdate("INSERT INTO $SCHEMA_VERSION($SCHEMA_VERSION) VALUES ($version)")
    }
}
