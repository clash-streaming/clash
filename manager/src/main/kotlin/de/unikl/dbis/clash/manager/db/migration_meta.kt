package de.unikl.dbis.clash.manager.db

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
    statement {
        it.executeUpdate(sqlString)
    }
    setSchemaVersionTo(version)
}

fun currentSchemaVersion(): Int {
    if (!schemaTableExists()) {
        createSchemaTable()
    }
    return statement {

        val result = it.executeQuery("SELECT MAX($SCHEMA_VERSION) FROM $SCHEMA_VERSION LIMIT 1")
        result.next()
        val version = result.getInt(1)
        version
    }
}

fun schemaTableExists(): Boolean {
    return statement {
        val result = it.executeQuery("SELECT name FROM sqlite_master WHERE type='table' AND name='$SCHEMA_VERSION';")
        result.next()
    }
}

fun createSchemaTable() {
    statement {
        it.executeUpdate("CREATE TABLE $SCHEMA_VERSION ($SCHEMA_VERSION int primary key);")
        setSchemaVersionTo(-1)
    }
}

fun setSchemaVersionTo(version: Int) {
    statement {
        it.executeUpdate("INSERT INTO $SCHEMA_VERSION($SCHEMA_VERSION) VALUES ($version)")
    }
}
