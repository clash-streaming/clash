package de.unikl.dbis.clash.flexstorm.sampling

class SampleBoltBuilder {
    val relations = mutableListOf<String>()

    fun addRelation(relationName: String): SampleBoltBuilder {
        relations.add(relationName)
        return this
    }

    fun build(): SampleBolt {
        TODO()
    }
}

class SampleBolt(relations: List<String>) {

}
