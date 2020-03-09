package de.unikl.dbis.clash.manager

import com.google.gson.JsonArray
import de.unikl.dbis.clash.manager.api.Command
import java.util.concurrent.ConcurrentLinkedQueue

class CommandQueue {
    val inner = ConcurrentLinkedQueue<Command>()

    /**
     * Adds another element to the command queue
     */
    @Synchronized
    fun add(command: Command) {
        inner.add(command)
    }

    /**
     * Returns the list of Commands in order of insertion.
     * For example:
     *
     * cq.add(c1); cq.add(c2); cq.add(c3); cq.add(c4)
     * cq.removeAll()
     * -> [c1, c2, c3, c4]
     */
    @Synchronized
    fun removeAll(): List<Command> {
        val res = mutableListOf<Command>()
        while (inner.isNotEmpty())
            res.add(inner.remove())
        return res
    }

    fun toJson(): JsonArray {
        val commands = removeAll()
        val res = JsonArray(commands.size)
        commands.forEach { res.add(it.toJson()) }
        return res
    }
}
