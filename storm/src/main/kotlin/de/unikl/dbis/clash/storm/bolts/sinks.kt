package de.unikl.dbis.clash.storm.bolts

import de.unikl.dbis.clash.ClashConfig
import de.unikl.dbis.clash.physical.Rule
import de.unikl.dbis.clash.storm.DocumentsMessage
import de.unikl.dbis.clash.storm.StormInRule
import de.unikl.dbis.clash.support.KafkaConfig
import de.unikl.dbis.clash.support.PostgresConfig
import de.unikl.dbis.clash.support.kafkaProducerProperties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichBolt
import org.slf4j.LoggerFactory
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.SQLException


interface CommonSinkI : IRichBolt {

    /**
     * During topology construction, a bolt gets rules assigned. This method takes care of assigning
     * them the in and out rules.
     *
     * @param rule the rule that is added
     */
    fun addRule(rule: Rule)
}


class KafkaSinkBolt(name: String,
                    private val topic: String,
                    val config: KafkaConfig) : AbstractBolt(name), CommonSinkI {
    @Transient
    private var producer: KafkaProducer<String, String>? = null

    override fun prepare(conf: MutableMap<String, Any>?, topologyContext: TopologyContext?, outputCollector: OutputCollector?) {
        super.prepare(conf, topologyContext, outputCollector)
        connectToKafka()
    }

    private fun connectToKafka() {
        val props = kafkaProducerProperties(config)
        this.producer = KafkaProducer(props)
    }

    override fun executeDocuments(
            message: DocumentsMessage,
            stormInRule: StormInRule) {
        LOG.debug("Sink received tuple with ${message.documents.size} documents")
        for (document in message.documents) {
            this.producer!!.send(ProducerRecord(this.topic, null, document.toString()))
        }
    }

    override fun toString(): String {
        return "KafkaSinkBolt"
    }


    companion object {
        private val LOG = LoggerFactory.getLogger(KafkaSinkBolt::class.java)!!
    }
}


class LoggingSinkBolt(name: String) : AbstractBolt(name), CommonSinkI {

    override fun executeDocuments(message: DocumentsMessage,
                                  stormInRule: StormInRule) {

        for (document in message.documents) {
            LOG.info("$document")
        }
    }

    override fun toString(): String {
        return "LoggingSinkBolt"
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(LoggingSinkBolt::class.java)!!
    }
}


class NullSinkBolt : AbstractBolt("NullSink"), CommonSinkI {
    override fun executeDocuments(message: DocumentsMessage,
                                  stormInRule: StormInRule) {
    }
}


/**
 * This bolt accepts everything and stores it into a postgres table.
 */
class PgSinkBolt(name: String,
                 private val updateQuery: String,
                 private val postgresConfig: PostgresConfig
) : AbstractBolt(name), CommonSinkI {

    private lateinit var conn: Connection
    private lateinit var st: PreparedStatement

    override fun prepare(conf: MutableMap<String, Any>?, topologyContext: TopologyContext?, outputCollector: OutputCollector?) {
        super.prepare(conf, topologyContext, outputCollector)
        connectToDatabase()
    }

    override fun executeDocuments(message: DocumentsMessage,
                                  stormInRule: StormInRule) {
        try {
            for (document in message.documents) {
                this.st.setString(1, topologyContext.stormId)
                this.st.setString(2, document.toString())
                this.st.execute()
            }
        } catch (e: SQLException) {
            System.err.println("ERROR IN EXECUTING UPDATE")
            System.err.println(e.sqlState)
            e.printStackTrace()
        }
    }

    private fun connectToDatabase() {
        try {
            this.conn = DriverManager.getConnection(postgresConfig.jdbcConnectionString, postgresConfig.jdbcProperties)
            this.st = this.conn.prepareStatement(this.updateQuery)
            LOG.info("Successfully connected to Database.")

        } catch (e: SQLException) {
            System.err.println("ERROR IN CREATING DATABASE CONNECTION")
            e.printStackTrace()
            throw RuntimeException()
        }

    }

    override fun cleanup() {
        try {
            this.conn.close()
        } catch (e: SQLException) {
            System.err.println("ERROR IN CLOSING DATABASE CONNECTION")
            e.printStackTrace()
        }
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(PgSinkBolt::class.java)!!
    }
}


/**
 * This bolt accepts everything and writes it to the according file.
 * If that file did already exists, it is deleted beforehand.
 */
class FileSinkBolt(name: String,
                   val fileName: String
) : AbstractBolt(name), CommonSinkI {
    lateinit var file: File

    override fun prepare(conf: MutableMap<String, Any>?, topologyContext: TopologyContext?, outputCollector: OutputCollector?) {
        super.prepare(conf, topologyContext, outputCollector)
        LOG.debug("Creating file $fileName")
        file = File(fileName)
        file.delete()
        file.createNewFile()
    }

    override fun executeDocuments(message: DocumentsMessage,
                                  stormInRule: StormInRule) {
        for (document in message.documents) {
            file.appendText(document.toJson().toString() + "\n")
        }
    }


    companion object {
        private val LOG = LoggerFactory.getLogger(FileSinkBolt::class.java)!!
    }
}