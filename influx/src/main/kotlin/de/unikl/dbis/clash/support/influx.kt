package de.unikl.dbis.clash.support

import org.influxdb.InfluxDBFactory
import org.influxdb.InfluxDBIOException

data class InfluxConfig(
        val influxDbName: String,
        val influxUsername: String,
        val influxPassword: String,
        val influxUrl: String
)

fun checkInflux(config: InfluxConfig): Boolean {
    val user = config.influxUsername
    val password = config.influxPassword
    val url = config.influxUrl

    val influxDB = InfluxDBFactory.connect(url, user, password)
    try {
        influxDB.ping()
    } catch (e: InfluxDBIOException) {
        println("Could not ping to influx instance under $url")
        return false
    }

    val dbName = config.influxDbName
    if(!influxDB.databaseExists(dbName)) {
        influxDB.createDatabase(dbName)
        println("Created InfluxDB $dbName")
    }

    return true
}