package de.unikl.dbis.clash

import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.io.InputStream
import java.io.InputStreamReader
import java.net.URL
import java.util.ArrayList
import java.util.HashSet
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.SafeConstructor

class ClashConfig : HashMap<String, Any>() {

    companion object {
        // Controller
        const val CLASH_CONTROLLER_ENABLED = "clash.controller.enabled"
        const val DEFAULT_CONTROLLER_ENABLED = false

        const val CLASH_CONTROLLER_NAME = "clash.topology.controller_name"
        const val DEFAULT_CONTROLLER_NAME = "CONTROLLER"

        const val CLASH_CONTROLLER_TOPIC = "clash.topology.controller_topic"
        const val DEFAULT_CONTROLLER_TOPIC = "ctrl"

        // Probe log
        const val CLASH_PROBE_LOG_ENABLED = "clash.probe_log.enabled"
        const val DEFAULT_PROBE_LOG_ENABLED = true
        const val CLASH_PROBE_LOG_MAX_KEYS = "clash.probe_log.max_keys"
        const val DEFAULT_PROBE_LOG_MAX_KEYS = 0

        // ClashConfig keys
        const val CLASH_INFLUX_URL = "clash.influx.url"
        const val CLASH_INFLUX_USERNAME = "clash.influx.username"
        const val CLASH_INFLUX_PASSWORD = "clash.influx.password"
        const val CLASH_INFLUX_DB_NAME = "clash.influx.db_name"

        const val CLASH_KAFKA_BOOTSTRAP_SERVERS = "clash.kafka.bootstrap"

        const val CLASH_SPOUT_PARALLELISM = "clash.topology.spout_parallelism"
        const val CLASH_DISPATCHER_PARALLELISM = "clash.topology.num_dispatchers"
        const val CLASH_SINK_PARALLELISM = "clash.topology.num_sinks"

        const val CLASH_DISPATCHER_NAME = "clash.topology.dispatcher_name"

        const val CLASH_OPTIMIZATION_STRATEGY = "clash.optimizer.strategy"

        const val CLASHD_WORKER_PORT = "clashd.worker.port"
        const val CLASHD_MASTER_HOST = "clashd.master.host"
        const val CLASHD_MASTER_PORT = "clashd.master.port"
        const val CLASHD_COORDINATOR_HOST = "clashd.coordinator.host"
        const val CLASHD_COORDINATOR_PORT = "clashd.coordinator.port"

        const val CLASH_TICK_RATE = "clash.tick_rate"
        const val CLASH_TICK_SPOUT_NAME = "clash.tick_spout_name"

        // Default values

        const val DEFAULT_INFLUX_URL = "http://localhost:8086"
        const val DEFAULT_INFLUX_USERNAME = "clash"
        const val DEFAULT_INFLUX_PASSWORD = "clash"
        const val DEFAULT_INFLUX_DB_NAME = "clash"

        const val DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

        const val DEFAULT_SPOUT_PARALLELISM = 1L
        const val DEFAULT_DISPATCHER_PARALLELISM = 1L
        const val DEFAULT_SINK_PARALLELISM = 1L

        const val DEFAULT_DISPATCHER_NAME = "DISPATCHER"

        const val DEFAULT_OPTIMIZATION_STRATEGY = "FlatTreeStrategy"

        const val DEFAULT_WORKER_PORT = 13371
        const val DEFAULT_MASTER_HOST = "clashd.master.host"
        const val DEFAULT_MASTER_PORT = 13372
        const val DEFAULT_COORDINATOR_HOST = "clashd.coordinator.host"
        const val DEFAULT_COORDINATOR_PORT = 13373

        const val DEFAULT_TICK_RATE = 1000
        const val DEFAULT_TICK_SPOUT_NAME = "tickspout"
    }

    val kafkaBootstrapServers: String get() = getOrDefaultString(CLASH_KAFKA_BOOTSTRAP_SERVERS, DEFAULT_KAFKA_BOOTSTRAP_SERVERS)

    /**
     * @return The degree or parallelism for spouts
     */
    val spoutParallelism get() = getOrDefaultLong(CLASH_SPOUT_PARALLELISM, DEFAULT_SPOUT_PARALLELISM)

    /**
     * @return The degree or parallelism for dispatcher bolts
     */
    val dispatcherParallelism get() = getOrDefaultLong(CLASH_DISPATCHER_PARALLELISM, DEFAULT_DISPATCHER_PARALLELISM)

    /**
     * @return The degree or parallelism for sink bolts
     */
    val sinkParallelism get() = getOrDefaultLong(CLASH_SINK_PARALLELISM, DEFAULT_SINK_PARALLELISM)

    /**
     * @return The name for the dispatcher bolt
     */
    val dispatcherName get() = getOrDefaultString(CLASH_DISPATCHER_NAME, DEFAULT_DISPATCHER_NAME)

    /**
     * @return The name for the controller bolt
     */
    val controllerEnabled get() = getOrDefaultBoolean(CLASH_CONTROLLER_ENABLED, DEFAULT_CONTROLLER_ENABLED)

    /**
     * @return The name for the controller bolt
     */
    val controllerName get() = getOrDefaultString(CLASH_CONTROLLER_NAME, DEFAULT_CONTROLLER_NAME)

    /**
     * @return The URL of the InfluxDB instance
     */
    val influxUrl get() = getOrDefaultString(CLASH_INFLUX_URL, DEFAULT_INFLUX_URL)

    /**
     * @return The username for connecting to the InfluxDB instance
     */
    val influxUsername get() = getOrDefaultString(CLASH_INFLUX_USERNAME, DEFAULT_INFLUX_USERNAME)

    /**
     * @return The password for connecting to the InfluxDB instance
     */
    val influxPassword get() = getOrDefaultString(CLASH_INFLUX_PASSWORD, DEFAULT_INFLUX_PASSWORD)

    /**
     * @return The InfluxDB database name used for reporting
     */
    val influxDbName get() = getOrDefaultString(CLASH_INFLUX_DB_NAME, DEFAULT_INFLUX_DB_NAME)

    /**
     * @return The name for the controller bolt
     */
    val controllerTopic get() = getOrDefaultString(CLASH_CONTROLLER_TOPIC, DEFAULT_CONTROLLER_TOPIC)

    /**
     * @return whether to enable the probe log for handling delayed probe tuples
     */
    val probeLogEnabled get() = getOrDefaultBoolean(CLASH_PROBE_LOG_ENABLED, DEFAULT_PROBE_LOG_ENABLED)

    /**
     * @return how many entries may be saved inside the store log for handling delayed probe tuples
     */
    val probeLogMaxKeys get() = getOrDefaultInt(CLASH_PROBE_LOG_MAX_KEYS, DEFAULT_PROBE_LOG_MAX_KEYS)

    val workerPort get() = getOrDefaultInt(CLASHD_WORKER_PORT, DEFAULT_WORKER_PORT)
    val masterHost get() = getOrDefaultString(CLASHD_MASTER_HOST, DEFAULT_MASTER_HOST)
    val masterPort get() = getOrDefaultInt(CLASHD_MASTER_PORT, DEFAULT_MASTER_PORT)
    val coordinatorHost get() = getOrDefaultString(CLASHD_COORDINATOR_HOST, DEFAULT_COORDINATOR_HOST)
    val coordinatorPort get() = getOrDefaultInt(CLASHD_COORDINATOR_PORT, DEFAULT_COORDINATOR_PORT)

    /**
     * @return The tick rate of the tick spout in ms, or 0 if ticking is disabled.
     */
    val tickRate get() = getOrDefaultInt(CLASH_TICK_RATE, DEFAULT_TICK_RATE)

    /**
     * @return The name of the tick spout
     */
    val tickSpoutName get() = getOrDefaultString(CLASH_TICK_SPOUT_NAME, DEFAULT_TICK_SPOUT_NAME)

    fun getOrDefaultInt(key: Any, defaultValue: Int): Int {
        return if (this.containsKey(key)) {
            val value = this[key]
            try {
                Integer.parseInt(value.toString())
            } catch (e: NumberFormatException) {
                System.err.println("CANNOT PARSE NUMBER FORMAT $key")
                System.err.println("RETURNING DEFAULT VALUE INSTEAD")
                e.printStackTrace()
                defaultValue
            }
        } else {
            defaultValue
        }
    }

    fun getOrDefaultLong(key: Any, defaultValue: Long): Long {
        return if (this.containsKey(key)) {
            val value = this[key]
            try {
                value.toString().toLong()
//                Long.parseLong(value.toString())
            } catch (e: NumberFormatException) {
                System.err.println("CANNOT PARSE NUMBER FORMAT $key")
                System.err.println("RETURNING DEFAULT VALUE INSTEAD")
                e.printStackTrace()
                defaultValue
            }
        } else {
            defaultValue
        }
    }

    fun getOrDefaultString(key: Any, defaultValue: String): String {
        return if (this.containsKey(key)) {
            val value = this[key]
            value.toString()
        } else {
            defaultValue
        }
    }

    fun getOrDefaultBoolean(key: Any, defaultValue: Boolean): Boolean {
        return if (this.containsKey(key)) {
            val value = this[key]
            value as Boolean
        } else {
            defaultValue
        }
    }

    fun getOptimizationStrategy(): String {
        return getOrDefaultString(CLASH_OPTIMIZATION_STRATEGY, DEFAULT_OPTIMIZATION_STRATEGY)
    }
}

fun readConfig(configFilePath: String): ClashConfig {
    var inputStream: InputStream? = null
    var configMap: Map<String, Any>? = null

    try {
        inputStream = getConfigFileInputStream(configFilePath)
        if (null != inputStream) {
            val yaml = Yaml(SafeConstructor())
            configMap = yaml.load<Any>(InputStreamReader(inputStream)) as Map<String, Any>
        }
    } catch (e: IOException) {
        throw RuntimeException(e)
    } finally {
        if (inputStream != null) {
            try {
                inputStream.close()
            } catch (e: IOException) {
                throw RuntimeException(e)
            }
        }
    }

    if (configMap == null || configMap.isEmpty())
        return ClashConfig()

    val conf = ClashConfig()
    configMap.entries.forEach { conf[it.key] = it.value }
    return conf
}

fun getConfigFileInputStream(configFilePath: String): InputStream? {
    val resources = HashSet<URL>(findResources(configFilePath))
    if (resources.isEmpty()) {
        val configFile = File(configFilePath)
        if (configFile.exists()) {
            return FileInputStream(configFile)
        }
    } else if (resources.size > 1) {
        throw IOException(
                "Found multiple " + configFilePath +
                        " resources. You're probably bundling the Storm jars with your topology jar. " +
                        resources)
    } else {
        val resource = resources.iterator().next()
        return resource.openStream()
    }
    return null
}

fun findResources(name: String): List<URL> {
    try {
        val resources = Thread.currentThread().contextClassLoader.getResources(name)
        val ret = ArrayList<URL>()
        while (resources.hasMoreElements()) {
            ret.add(resources.nextElement())
        }
        return ret
    } catch (e: IOException) {
        throw RuntimeException(e)
    }
}
