package de.unikl.dbis.clash.optimizer.ilp

open class Configuration(
    open val parallelism: Map<String, Int>
) {
    open fun getParallelism(s: String): Int {
        return parallelism[s]!!
    }
}

class ConstantConfiguration(val degree: Int) : Configuration(mapOf()) {
    override fun getParallelism(s: String): Int {
        return degree
    }
}
