package de.unikl.dbis.clash.optimizer.ilp

var counter = 0
fun newVariable() = "x${counter++}"

enum class IlpCompare {
    EQUAL,
    GREATER_THAN;

    override fun toString(): String {
        return when(this) {
            EQUAL -> "="
            GREATER_THAN -> ">"
        }
    }
}

data class IlpEntry(val multiplier: Number, val variable: String) {
    override fun toString(): String {
        return "$multiplier $variable"
    }
}

data class IlpRow(val entries: List<IlpEntry>, val compare: IlpCompare, val value: Int, val comment: String = "") {
    override fun toString(): String {
        return entries.joinToString(" + ") + " " + compare + " " + value + "\t\t// " + comment
    }
}

data class Ilp(val rows: List<IlpRow>, val goal: List<IlpEntry>, val list: List<String>) {
    override fun toString(): String {
        return "min ${goal.joinToString(" ")} s.t.\n" +  rows.joinToString("\n")
    }
}

