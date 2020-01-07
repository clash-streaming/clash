package de.unikl.dbis.clash.support

import java.io.Serializable
import java.sql.DriverManager
import java.sql.SQLException
import java.util.Properties

data class PostgresConfig(
    val hostname: String,
    val port: String,
    val dbName: String,
    val user: String,
    val password: String
) : Serializable {
    val jdbcConnectionString: String
        get() {
            return "jdbc:postgresql://$hostname:$port/$dbName"
        }

    val jdbcProperties: Properties
        get() {
            val properties = Properties()
            properties.setProperty("user", user)
            properties.setProperty("password", password)
            return properties
        }
}

fun checkPostgres(postgresConfig: PostgresConfig): Boolean {
    try {
        val conn = DriverManager.getConnection(postgresConfig.jdbcConnectionString, postgresConfig.jdbcProperties)
        conn.close()
        return true
    } catch (e: SQLException) {
        System.err.println(e)
    }

    return false
}

fun initTable(create: String, insert: String = "", config: PostgresConfig) {
    val conn = DriverManager.getConnection(config.jdbcConnectionString, config.jdbcProperties)
    val statement = conn.createStatement()
    statement.execute(create)
    val warnings = statement.warnings?.count() ?: 0
    if (warnings == 0) {
        // the table was created freshly
        statement.execute(insert)
    }
    conn.close()
}

fun truncateTable(tableName: String, config: PostgresConfig) {
    val conn = DriverManager.getConnection(config.jdbcConnectionString, config.jdbcProperties)
    val statement = conn.createStatement()
    statement.execute("TRUNCATE TABLE $tableName")
    conn.close()
}

fun executePostgres(sql: String, config: PostgresConfig) {
    try {
        val conn = DriverManager.getConnection(config.jdbcConnectionString, config.jdbcProperties)
        val stmt = conn.createStatement()
        stmt.execute(sql)
        stmt.closeOnCompletion()
        conn.close()
    } catch (e: SQLException) {
        System.err.println(e)
    }
}

fun postgresTableExists(config: PostgresConfig, tableName: String): Boolean {
    var result = false
    try {
        val conn = DriverManager.getConnection(config.jdbcConnectionString, config.jdbcProperties)
        val meta = conn.getMetaData()
        val res = meta.getTables(null, null, tableName, arrayOf("TABLE"))
//        result = res.next()
        while (res.next()) {
            System.out.println(
                    "   " + res.getString("TABLE_CAT") +
                            ", " + res.getString("TABLE_SCHEM") +
                            ", " + res.getString("TABLE_NAME") +
                            ", " + res.getString("TABLE_TYPE") +
                            ", " + res.getString("REMARKS"))
        }

        conn.close()
    } catch (e: SQLException) {
        System.err.println(e)
    }
    return result
}
