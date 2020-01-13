package de.unikl.dbis.clash.manager

enum class Operation {
    READ,
    WRITE,
    COMMIT,
    ABORT
}

data class LogEntry(val operation: Operation, val tid: Int)

class Log {
    val entries = mutableListOf<LogEntry>()
    val tids = mutableSetOf<Int>()

    fun add(logEntry: LogEntry) {
        entries.add(logEntry)
        tids.add(logEntry.tid)
    }
}

fun main() {
    val log = Log()
    log.add(LogEntry(Operation.READ, 1))
    log.add(LogEntry(Operation.READ, 2))
    log.add(LogEntry(Operation.WRITE, 2))
    log.add(LogEntry(Operation.WRITE, 3))
    log.add(LogEntry(Operation.COMMIT, 1))
    log.add(LogEntry(Operation.COMMIT, 3))
    log.add(LogEntry(Operation.COMMIT, 2))
    log.add(LogEntry(Operation.READ, 4))

    // println(log.tids.map { tid -> log.entries.last { it.tid == tid && it.operation == Operation.COMMIT} })
    println(log.tids.filter { tid -> log.entries.last { it.tid == tid }.operation == Operation.COMMIT })
}
