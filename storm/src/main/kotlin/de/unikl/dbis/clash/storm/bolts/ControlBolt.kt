package de.unikl.dbis.clash.storm.bolts

class ControlBolt(name: String) : AbstractBolt(name) {
    override fun toString(): String {
        return "ControlBolt: $name"
    }
}
