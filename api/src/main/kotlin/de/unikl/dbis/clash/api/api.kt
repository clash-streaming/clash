package de.unikl.dbis.clash.api

import de.unikl.dbis.clash.ClashConfig
import de.unikl.dbis.clash.datacharacteristics.AllCross
import de.unikl.dbis.clash.datacharacteristics.DataCharacteristics
import de.unikl.dbis.clash.optimizer.OptimizationParameters
import de.unikl.dbis.clash.optimizer.OptimizationResult
import de.unikl.dbis.clash.physical.PhysicalGraph
import de.unikl.dbis.clash.query.InputName
import de.unikl.dbis.clash.query.Query
import de.unikl.dbis.clash.query.QueryBuilder
import de.unikl.dbis.clash.query.Relation
import de.unikl.dbis.clash.storm.bolts.CommonSinkI
import de.unikl.dbis.clash.storm.builder.StormTopologyBuilder
import de.unikl.dbis.clash.storm.spouts.CommonSpoutI
import org.apache.storm.generated.StormTopology


class Clash @JvmOverloads constructor(
        private val config: ClashConfig = ClashConfig(),
        private val dataCharacteristics: DataCharacteristics = AllCross(),
        private val optimizationParameters: OptimizationParameters = OptimizationParameters(),
        private var query: Query? = null) {

    private val sources: MutableMap<InputName, CommonSpoutI> = mutableMapOf()
    private val sinks: MutableMap<Relation, CommonSinkI> = mutableMapOf()
    private val queryBuilder: QueryBuilder = QueryBuilder()
    private var optimizationResult: OptimizationResult? = null
    private var physicalGraph: PhysicalGraph? = null
    var storageCost: Double = -1.0
    var probeCost: Double = -1.0

    fun registerSource(string: String, source: CommonSpoutI) {
        this.sources[InputName(string)] = source
    }

    fun registerSource(relation: Relation, source: CommonSpoutI) {
        TODO()
//        this.sources[inputForRelation(relation)] = source
    }

    fun registerSink(sink: CommonSinkI) {
        // this.sinks[relation] = sink
    }

    fun registerSink(relation: Relation, sink: CommonSinkI) {
        this.sinks[relation] = sink
    }

    fun query(): QueryBuilder {
        return this.queryBuilder
    }

    fun optimize(): OptimizationResult {
        if(optimizationResult == null) optimizationResult = optimizationParameters.globalStrategy.optimize(this.query!!, this.dataCharacteristics, this.optimizationParameters)
        return optimizationResult!!
    }

    @Throws(InvalidClashTopologyException::class)
    fun buildTopology() {
        // Validate mandatory input
        if (this.sources.isEmpty()) throw InvalidClashTopologyException("No source definition provided.")
        if (this.sinks.isEmpty()) throw InvalidClashTopologyException("No sink definition provided.")

        if (query == null) query = queryBuilder.build()
        optimize()

        physicalGraph = optimizationResult!!.physicalGraph
        probeCost = optimizationResult!!.costEstimation.probeCost
        storageCost = optimizationResult!!.costEstimation.storageCost
    }


    @Throws(InvalidClashTopologyException::class)
    fun buildStormTopology(): StormTopology {
        if (this.physicalGraph == null) {
            this.buildTopology()
        }

        // currently only one sink is supported
        val sink = sinks.values.first()

        val builder = StormTopologyBuilder(
                this.physicalGraph!!,
                this.sources,
                sink=sink, // TODO maybe there is an input mapping missing
                config=this.config)
        return builder.build()
    }
}

class InvalidClashTopologyException internal constructor(error: String) : Exception(error)
