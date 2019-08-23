package de.unikl.dbis.clash.storm.spouts

import de.unikl.dbis.clash.documents.Document
import de.unikl.dbis.clash.query.InputName
import de.unikl.dbis.clash.query.RelationAlias
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.utils.Utils
import org.json.JSONObject
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths

class JsonFileSpout constructor(
        val inputName: InputName,
        val relationAlias: RelationAlias,
        private val filePath: String,
        millisDelay: Int = 0) : CommonSpout(millisDelay) {
    var iterator: Iterator<String>? = null


    override fun open(conf: MutableMap<String, Any>?, topologyContext: TopologyContext?, spoutOutputCollector: SpoutOutputCollector?) {
        super.open(conf, topologyContext, spoutOutputCollector)
        LOG.debug("Opened spout ${inputName.inner}")
        try {
            val stream = Files.lines(Paths.get(this.filePath))
            this.iterator = stream.iterator()
        } catch (e: IOException) {
            e.printStackTrace()
        }

    }


    override fun nextTuple() {
        if (iterator != null && iterator!!.hasNext()) {
            val rawTuple = iterator!!.next()
            val tupleObject: JSONObject
            try {
                tupleObject = JSONObject(rawTuple)
            } catch (e: org.json.JSONException) {
                LOG.warn(
                        "Could not parse raw tuple \"" + rawTuple + "\"! (filename: " + this.filePath + ")", e)
                return
            }

            val document = Document()
            for (key in tupleObject.keySet()) {
                document[relationAlias, key] = tupleObject.get(key).toString()
            }

            super.emit(document)
        }
        val sleepTime = this.millisDelay.toLong()
        if (sleepTime > 0) {
            Utils.sleep(sleepTime)
        }
    }

    override fun fail(msgId: Any?) {
        LOG.warn("Failed to process " + msgId!!.toString())
    }

    override fun toString(): String {
        return "JsonFileSpout"
    }

    companion object {

        private val LOG = LoggerFactory.getLogger(JsonFileSpout::class.java)
    }
}



class LinewiseFileSpout constructor(
        val inputName: InputName,
        val relationAlias: RelationAlias,
        val filePath: String,
        millisDelay: Int = 0,
        val lineTransformer: (RelationAlias, String) -> Document) : CommonSpout(millisDelay) {

    var iterator: Iterator<String>? = null


    override fun open(conf: MutableMap<String, Any>?, topologyContext: TopologyContext?, spoutOutputCollector: SpoutOutputCollector?) {
        super.open(conf, topologyContext, spoutOutputCollector)
        LOG.debug("Opened spout ${inputName.inner}")
        try {
            val stream = Files.lines(Paths.get(this.filePath))
            this.iterator = stream.iterator()
        } catch (e: IOException) {
            e.printStackTrace()
        }
    }


    override fun nextTuple() {
        if (iterator != null && iterator!!.hasNext()) {
            val line = iterator!!.next()
            val document = lineTransformer(relationAlias, line)
            super.emit(document)
        }
        val sleepTime = this.millisDelay.toLong()
        if (sleepTime > 0) {
            Utils.sleep(sleepTime)
        }
    }

    override fun fail(msgId: Any?) {
        LOG.warn("Failed to process " + msgId!!.toString())
    }

    override fun toString(): String {
        return "LinewiseFileSpout"
    }

    companion object {

        private val LOG = LoggerFactory.getLogger(LinewiseFileSpout::class.java)
    }
}