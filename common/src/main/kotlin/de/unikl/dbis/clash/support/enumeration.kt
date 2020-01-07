package de.unikl.dbis.clash.support

/**
 * All permutations of a given list.
 *
 * For example:
 *
 * listOf("a", "b", "c").permutations()
 * => [c, b, a]
 *    [b, c, a]
 *    [c, a, b]
 *    [a, c, b]
 *    [b, a, c]
 *    [a, b, c]
 */
fun <T> List<T>.permutations(): List<List<T>> {
    val result = mutableListOf<List<T>>()

    if (this.size == 1) {
        return listOf(this)
    }

    for (el in this) {
        val rest = this.minusElement(el)
        for (p in rest.permutations()) {
            result += p + el
        }
    }
    return result
}

/**
 * All subsets of the given list of size k. TODO rewrite
 *
 * For example:
 *
 * listOf("a", "b", "c", "d").subsetsOfSize(3)
 * => {"a", "b", "c"}
 *    {"a", "b", "d"}
 *    {"a", "c", "d"}
 *    {"b", "c", "d"}
 */
fun <T> List<T>.subsetsOfSize(m: Int): List<Set<T>> {
    fun getSubsets(superSet: List<T>, k: Int, idx: Int, current: MutableSet<T>, solution: MutableList<Set<T>>) {
        // successful stop clause
        if (current.size == k) {
            solution.add(current.toSet())
            return
        }
        // unsuccessful stop clause
        if (idx == superSet.size) {
            return
        }

        val x = superSet[idx]
        current.add(x)

        getSubsets(superSet, k, idx + 1, current, solution)
        current.remove(x)

        getSubsets(superSet, k, idx + 1, current, solution)
    }

    val res = mutableListOf<Set<T>>()
    getSubsets(this, m, 0, mutableSetOf(), res)
    return res
}

/**
 * Returns all partitions of the list into k part lists.
 *
 * E.g.
 *
 *   ["a", "b", "c"].partitions(2)
 *   => {"a", "b", "c"}, {}
 *      {"a", "b"}, {"c"}
 *      {"a", "c"}, {"b"}
 *      {"c", "b"}, {"a"}
 *
 * Source: http://www.informatik.uni-ulm.de/ni/Lehre/WS03/DMM/Software/partitions.pdf
 */
@Suppress("ComplexMethod")
fun <T> List<T>.partitions(k: Int): List<List<List<T>>> {
    TODO()
    fun pInitializeFirst(): Pair<IntArray, IntArray> {
        val kappa = IntArray(size)
        val m = IntArray(size)
        for (i in 0..size - k) {
            kappa[i] = 0
            m[i] = 0
        }
        for (i in size - k + 1 until size) {
            kappa[i] = i - (size - k)
            m[i] = i - (size - k)
        }

        return Pair(kappa, m)
    }

    fun pNextPartition(kappa: IntArray, m: IntArray) {
        for (i in (size - 1).downTo(1)) {
            if (kappa[i] < k - 1 && kappa[i] <= m[i - 1]) {
                kappa[i] = kappa[i] + 1
                m[i] = kotlin.math.max(m[i], kappa[i])
                for (j in (i + 1)..(size - (k - m[i]))) {
                    kappa[j] = 0
                    m[j] = m[i]
                }
                for (j in (size - (k - m[i]) + 1)..(size - 1)) {
                    kappa[j] = k - (size - j)
                    m[j] = k - (size - j)
                }
            }
        }
    }

    fun getPartitions(kappa: IntArray): List<List<T>> {
        val result = mutableListOf<MutableList<T>>()
        for (i in 0 until k) {
            result.add(mutableListOf())
        }
        for ((index, listId) in kappa.withIndex()) {
            result[listId].add(this[index])
        }
        return result
    }

    if (this.isEmpty()) return listOf()

    val (kappa, m) = pInitializeFirst()
    var notFailed = true
    val result = mutableListOf<List<List<T>>>()
    var lastKappa: IntArray? = null
    while (notFailed) {
        println(kappa.joinToString(", "))
        result.add(getPartitions(kappa))

        pNextPartition(kappa, m)
        if (lastKappa != null && kappa.contentEquals(lastKappa)) {
            break
        }
        lastKappa = kappa.clone()
    }

    return result
}

/**
 * Cross product of two iterables.
 *
 * If at least one of the iterables is empty, the result is empty.
 * If both contain elements, all nested-loops style combinations are output.
 */
operator fun <T : Any> Iterable<T>.times(other: Iterable<T>): Sequence<Pair<T, T>> {
    val first = iterator()
    var second = other.iterator()
    var a: T? = null

    fun nextPair(): Pair<T, T>? {
        if (a == null) { // initial state
            if (!first.hasNext() || !second.hasNext()) { // early return if one of iterators is empty
                return null
            } else { // set initial value of a
                a = first.next()
            }
        }

        return if (second.hasNext()) { // go through second iterator
            Pair(a!!, second.next())
        } else {
            if (first.hasNext()) { // wrap around
                a = first.next()
                second = other.iterator()
                Pair(a!!, second.next())
            } else { // first iterator exhausted, we are done
                null
            }
        }
    }

    return generateSequence { nextPair() }
}
