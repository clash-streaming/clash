package de.unikl.dbis.clash.optimizer.ilp

import kotlin.math.absoluteValue

fun Ilp.toCplex(sb: StringBuilder) {
    sb.append("Minimize\n")
    sb.append("goal: ")
    this.goal.forEach { it.toCplex(sb); sb.append(" ") }
    sb.append("\n\n")
    sb.append("Subject To\n")
    for (row in rows) {
        row.toCplex(sb)
        sb.append("\n")
    }
    sb.append("\nBINARY\n")
    for (variable in list) {
//        sb.append("0 <= $variable <= 1\n")
        sb.append("$variable\n")
    }
}

fun IlpRow.toCplex(sb: StringBuilder) {
    for (entry in entries) {
        sb.append("  ")
        entry.toCplex(sb)
        sb.append(" ")
    }
    compare.toCplex(sb)
    sb.append(" ")
    sb.append(value)
}

fun IlpEntry.toCplex(sb: StringBuilder) {
    if (multiplier == 0) return
    if (multiplier.toDouble() < 0.0) {
        sb.append("- ")
    }
    if (multiplier.toDouble() > 0.0) {
        sb.append("+ ")
    }
    sb.append(multiplier.toInt().absoluteValue)
    sb.append(" ")
    sb.append(variable)
}

fun IlpCompare.toCplex(sb: StringBuilder) {
    when (this) {
        IlpCompare.EQUAL -> sb.append("=")
        IlpCompare.GREATER_THAN -> sb.append(">")
    }
}

fun computeIlpSolution(ilp: Ilp, useGlpsol: Boolean = false): List<String> {
    val sb = StringBuilder()
    ilp.toCplex(sb)
    val problemFile = createTempFile("clash_ilp", ".lp")
    problemFile.writeText(sb.toString())

    val solutionFile = createTempFile("clash_ilp", ".sol")

    val command = if (useGlpsol) {
        arrayOf("glpsol", "--lp", "--binarize", "--interior", problemFile.absolutePath.toString(), "--output", solutionFile.absoluteFile.toString())
    } else {
        arrayOf("gurobi_cl", "ResultFile=${solutionFile.absolutePath}", problemFile.absolutePath.toString())
    }

    val pb = ProcessBuilder(*command)
    pb.inheritIO()
    val process = pb.start()
    process.waitFor()
    val text = solutionFile.readText()

    problemFile.delete()
    solutionFile.delete()

    // parse text
    val lines = text.split("\n")
    val satisfiedVariables = mutableListOf<String>()
    if (useGlpsol) {
        var found = false
        for (line in lines) {
            if (!found) {
                found = line.contains("Column name")
                continue
            }
            val c = line.split("\\s+".toRegex())
            if (c.size == 1) break
            if (c[4] == "1") satisfiedVariables += c[2]
        }
    } else {
        for (line in lines) {
            if (line.startsWith("#") || line.isEmpty()) continue
            val c = line.split(" ")
            if (c.size != 2) {
                continue
            }
            if (c[1] == "1") satisfiedVariables += c[0]
        }
    }

    return satisfiedVariables
}
