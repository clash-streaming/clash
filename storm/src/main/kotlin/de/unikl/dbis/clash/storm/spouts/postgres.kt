package de.unikl.dbis.clash.storm.spouts

import de.unikl.dbis.clash.documents.Document
import de.unikl.dbis.clash.query.InputName
import de.unikl.dbis.clash.query.RelationAlias
import de.unikl.dbis.clash.storm.FinishedMessage
import de.unikl.dbis.clash.support.PostgresConfig
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.utils.Utils
import org.slf4j.LoggerFactory
import java.sql.*
import java.util.*

/**
 * The PgTableSpout emits the tuples from a postgres table with configurable delay.
 * See https://dbis-ci.cs.uni-kl.de/clash-doc/pages/outside_interfaces.html#postgres for more documentation.
 */
class PgTableSpout : CommonSpout, CommonSpoutI {

    private val query: String
    private val inputName: InputName
    private val relationAlias: RelationAlias

    private val jdbcUrl: String
    private val jdbcProperties: Properties
    private val columnNames = HashMap<Int, String>()
    private var conn: Connection? = null
    private var st: Statement? = null
    private var rs: ResultSet? = null
    private var columnCount: Int = 0
    private var alreadyFinished = false
    private var alreadyFinishedSent = false

    /**
     * As the rate is about tuples per second, we convert it into millis delay by converting rate n to
     * 1000/n for delay.
     *
     * @param query How the data is retrieved from the database
     * @param millisDelay the amount of delay in milliseconds between two tuples
     * @param config ClashConfig where Postgres connection information is stored
     */
    constructor(inputName: InputName,
                baseRelation: RelationAlias,
                query: String,
                millisDelay: Int,
                config: PostgresConfig) : super(millisDelay) {
        this.inputName = inputName
        this.relationAlias = baseRelation
        this.query = query
        this.jdbcUrl = config.jdbcConnectionString
        this.jdbcProperties = config.jdbcProperties
    }

    override fun open(conf: MutableMap<String, Any>?, topologyContext: TopologyContext?, spoutOutputCollector: SpoutOutputCollector?) {
        super.open(conf, topologyContext, spoutOutputCollector)

        connectToDatabase()
        createResultSet()
        loadMetadata()
    }


    override fun nextTuple() {
        if (this.alreadyFinishedSent) {
            Utils.sleep(5000)
            return
        }
        if (this.alreadyFinished) {
            super.emit(FinishedMessage(0, 0))
            this.alreadyFinishedSent = true
            Utils.sleep(5000)
            return
        }
        try {
            if (this.rs!!.next()) {
                val document = Document()

                for (i in 1..this.columnCount) {
                    val attrName = this.columnNames[i]!!
                    val value = this.rs!!.getString(i)
                    document[relationAlias, attrName] = value
                }
                super.emit(document)
            } else {
                this.alreadyFinished = true
                LOG.info("Spout has no more tuples to emit.")
            }
        } catch (e: SQLException) {
            e.printStackTrace()
        }

        super.delayNext()
    }

    override fun close() {
        try {
            this.rs!!.close()
            this.st!!.close()
            this.conn!!.close()
            LOG.info("Successfully closed database connection.")
        } catch (e: SQLException) {
            System.err.println("Error in closing database connection")
            e.printStackTrace()
            throw RuntimeException()
        }

        super.close()
    }

    private fun connectToDatabase() {
        try {
            this.conn = DriverManager.getConnection(this.jdbcUrl, this.jdbcProperties)
            LOG.info("Successfully connected to the database " + this.jdbcUrl)
        } catch (e: SQLException) {
            LOG.error("Error in connecting to the database " + this.jdbcUrl)
            e.printStackTrace()
            throw RuntimeException()
        }

    }

    private fun createResultSet() {
        try {
            this.st = this.conn!!.createStatement()
            this.rs = this.st!!.executeQuery(this.query)
            LOG.info("Successfully created resultSet")
        } catch (e: SQLException) {
            LOG.error("Error in getting resultSet")
            e.printStackTrace()
            throw RuntimeException()
        }

    }

    private fun loadMetadata() {
        try {
            val metadata = this.rs!!.metaData
            this.columnCount = metadata.columnCount
            for (i in 1..this.columnCount) {
                this.columnNames[i] = metadata.getColumnName(i)
            }
            LOG.info("Successfully got metadata from result set")
        } catch (e: SQLException) {
            LOG.error("Error in getting metadata from result set")
            e.printStackTrace()
            throw RuntimeException()
        }

    }

    override fun toString(): String {
        return "PgTableSpout"
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(PgTableSpout::class.java)
    }
}