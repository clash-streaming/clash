package de.unikl.dbis.clash.storm.bolts

import de.unikl.dbis.clash.storm.StormEdgeLabel
import de.unikl.dbis.clash.workers.stores.ActualStore

class AggregateStore(
    name: String
) : AbstractBolt(name), IStoreStats by StoreStats()
