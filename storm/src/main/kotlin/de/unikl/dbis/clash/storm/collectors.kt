package de.unikl.dbis.clash.storm

import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.OutputCollector

class EdgyOutputCollector(private val inner: OutputCollector) {
    fun emit(edgeLabel: StormEdgeLabel, message: AbstractMessage) {
        val values = message.asValues(edgeLabel.groupingAttribute)
        this.inner.emit(edgeLabel.streamName, values)
    }
}

class EdgySpoutOutputCollector(private val inner: SpoutOutputCollector) {
    fun emit(edgeLabel: StormEdgeLabel, message: AbstractMessage) {
        val values = message.asValues(edgeLabel.groupingAttribute)
        this.inner.emit(edgeLabel.streamName, values)
    }
}
