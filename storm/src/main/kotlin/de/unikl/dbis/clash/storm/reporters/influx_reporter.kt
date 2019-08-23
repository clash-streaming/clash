package de.unikl.dbis.clash.storm.reporters

import com.codahale.metrics.MetricRegistry
import metrics_influxdb.HttpInfluxdbProtocol
import metrics_influxdb.InfluxdbProtocol
import metrics_influxdb.InfluxdbReporter
import metrics_influxdb.api.measurements.MetricMeasurementTransformer
import org.apache.storm.metrics2.reporters.ScheduledStormReporter
import org.apache.storm.utils.ObjectReader
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit


class InfluxStormReporter : ScheduledStormReporter() {
    val INFLUX_SCHEME = "influx.scheme"
    val INFLUX_DEFAULT_SCHEME = "http"
    val INFLUX_HOST = "influx.host"
    val INFLUX_PORT = "influx.port"
    val INFLUX_DEFAULT_PORT = 8086
    val INFLUX_USER = "influx.user"
    val INFLUX_DEFAULT_USER = "influx"
    val INFLUX_PASSWORD = "influx.password"
    val INFLUX_DEFAULT_PASSWORD = ""
    val INFLUX_DB = "influx.db"
    val INFLUX_CONNECT_TIMEOUT = "influx.connect_timeout"
    val INFLUX_DEFAULT_CONNECT_TIMEOUT = 10
    val INFLUX_READ_TIMEOUT = "influx.read_timeout"
    val INFLUX_DEFAULT_READ_TIMEOUT = 5

    private fun getMetricsTargetHost(reporterConf: Map<*, *>): String? {
        return ObjectReader.getString(reporterConf[INFLUX_HOST])
    }

    private fun getMetricsTargetPort(reporterConf: Map<*, *>): Int? {
        return ObjectReader.getInt(reporterConf[INFLUX_PORT], null)
    }

    private fun getInfluxdbProtocol(reporterConf: MutableMap<String, Any>): InfluxdbProtocol {
        val scheme = reporterConf.getOrDefault(INFLUX_SCHEME, INFLUX_DEFAULT_SCHEME) as String
        val host = reporterConf[INFLUX_HOST]!! as String
        val port = reporterConf.getOrDefault(INFLUX_PORT, INFLUX_DEFAULT_PORT) as Int
        val user = reporterConf.getOrDefault(INFLUX_USER, INFLUX_DEFAULT_USER) as String
        val password = reporterConf.getOrDefault(INFLUX_PASSWORD, INFLUX_DEFAULT_PASSWORD) as String
        val db = reporterConf[INFLUX_DB]!! as String
        val connectTimeout = reporterConf.getOrDefault(INFLUX_CONNECT_TIMEOUT, INFLUX_DEFAULT_CONNECT_TIMEOUT) as Int
        val readTimeout = reporterConf.getOrDefault(INFLUX_READ_TIMEOUT, INFLUX_DEFAULT_READ_TIMEOUT) as Int

        return HttpInfluxdbProtocol(scheme, host, port, user, password, db)
    }

    override fun prepare(metricsRegistry: MetricRegistry, conf: MutableMap<String, Any>, reporterConf: MutableMap<String, Any>) {
        LOG.debug("Preparing...")
        val builder = InfluxdbReporter.forRegistry(metricsRegistry)


//        val durationUnit = MetricsUtils.getMetricsDurationUnit(reporterConf)
        val durationUnit = TimeUnit.MILLISECONDS
        if (durationUnit != null) {
            builder.convertDurationsTo(durationUnit)
        }

//        val rateUnit = MetricsUtils.getMetricsRateUnit(reporterConf)
        val rateUnit = TimeUnit.SECONDS
        if (rateUnit != null) {
            builder.convertRatesTo(rateUnit)
        }

        val filter = getMetricsFilter(reporterConf)
        if (filter != null) {
            builder.filter(filter)
        }

        //defaults to 10
        reportingPeriod = getReportPeriod(reporterConf)

        //defaults to seconds
        reportingPeriodUnit = getReportPeriodUnit(reporterConf)

        val influxdbProtocol = getInfluxdbProtocol(reporterConf)
        builder.protocol(influxdbProtocol)

        builder.transformer(InfluxStormTransformer())

        reporter = builder.build()
    }

    companion object {
        val LOG = LoggerFactory.getLogger(InfluxStormReporter::class.java)!!
    }
}


/**
 * Metrics are communicated in the form
 *
 *     storm.topology.{topology ID}.{hostname}.{component ID}.{task ID}.{worker port}-myCounter
 *
 * for example:
 *
 *     storm.worker.test-1-1558351397.dbis-exp4_informatik_uni-kl_de.l.__metrics.12.6704-emitted
 *                           |                |                      |
 *              topology id -+                |                      |
 *                          hostname ---------+                      |
 *                                     component id -----------------+ ????
 *
 */
class InfluxStormTransformer : MetricMeasurementTransformer {
    val SEPARATOR = "."
    val INNER_SEPARATOR = "-"

    val STORM = 0
    val TOPOLOGY = 1
    val TOPOLOGY_ID = 2
    val HOSTNAME = 3
    val COMPONENT_ID = 4
    val TASK_ID = 5

    override fun tags(metricName: String): MutableMap<String, String> {
        val splitted = metricName.split(SEPARATOR)
        return mutableMapOf(
                "topology_id" to splitted[TOPOLOGY_ID],
                "component" to splitted[COMPONENT_ID],
                "taskId" to splitted[TASK_ID]
        )
    }

    override fun measurementName(metricName: String): String {
        val splitted = metricName.split(SEPARATOR)
        val splittedLast = splitted.last()
        return splittedLast
    }

}