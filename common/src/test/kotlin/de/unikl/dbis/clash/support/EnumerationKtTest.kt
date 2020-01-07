package de.unikl.dbis.clash.support

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test

internal class EnumerationKtTest {
    @Test
    fun `permutations works for empty list`() {
        val list: List<Int> = listOf()

        Assertions.assertThat(list.permutations()).containsExactlyInAnyOrder()
    }

    @Test
    fun `permutations works for list with elements`() {
        val list = listOf(1, 2, 3)

        Assertions.assertThat(list.permutations()).containsExactlyInAnyOrder(
                listOf(1, 2, 3),
                listOf(1, 3, 2),
                listOf(2, 1, 3),
                listOf(2, 3, 1),
                listOf(3, 1, 2),
                listOf(3, 2, 1)
        )
    }

    @Test
    fun `subsetsOfSize works for empty list`() {
        val list: List<Int> = listOf()

        Assertions.assertThat(list.subsetsOfSize(1)).containsExactlyInAnyOrder()
    }

    @Test
    fun `subsetsOfSize works for three element list and subsets of size 1`() {
        val list = listOf("a", "b", "c")

        Assertions.assertThat(list.subsetsOfSize(1)).containsExactlyInAnyOrder(
                setOf("a"),
                setOf("b"),
                setOf("c")
        )
    }

    @Test
    fun `subsetsOfSize works for three element list and subsets of size 2`() {
        val list = listOf("a", "b", "c")

        Assertions.assertThat(list.subsetsOfSize(2)).containsExactlyInAnyOrder(
                setOf("a", "b"),
                setOf("a", "c"),
                setOf("b", "c")
        )
    }

//    @Test
//    fun `partitions works with empty list`() {
//        val list: List<Int> = listOf()
//
//        Assertions.assertThat(list.partitions(1)).containsExactlyInAnyOrder()
//    }
//
//    @Test
//    fun `partitions works with single element list and one partition`() {
//        val list = listOf("x")
//
//        Assertions.assertThat(list.partitions(1)).containsExactlyInAnyOrder(
//                listOf(listOf("x"), listOf())
//        )
//    }
//
//    @Test
//    fun `partitions works with three element list and two partitions`() {
//        val list = listOf("a", "b", "c")
//
//        Assertions.assertThat(list.partitions(1)).containsExactlyInAnyOrder(
//                listOf(listOf("a", "b", "c"), listOf()),
//                listOf(listOf("a", "b"), listOf("c")),
//                listOf(listOf("a", "c"), listOf("b")),
//                listOf(listOf("c", "b"), listOf("a"))
//        )
//    }

    @Test
    fun `times of empty sets works`() {
        val listA: List<Int> = listOf()
        val listB: List<Int> = listOf()

        Assertions.assertThat(listA.times(listB).toList()).containsExactlyInAnyOrder()
    }

    @Test
    fun `times of empty set with non-empty set is empty`() {
        val listA = listOf(5, 9)
        val listB: List<Int> = listOf()

        Assertions.assertThat(listA.times(listB).toList()).containsExactlyInAnyOrder()
        Assertions.assertThat(listB.times(listA).toList()).containsExactlyInAnyOrder()
    }

    @Test
    fun `times of two element sets works`() {
        val listA = listOf("x", "y")
        val listB = listOf("u", "v")

        Assertions.assertThat(listA.times(listB).toList()).containsExactlyInAnyOrder(
                Pair("x", "u"), Pair("x", "v"),
                Pair("y", "u"), Pair("y", "v")
        )

        Assertions.assertThat(listB.times(listA).toList()).containsExactlyInAnyOrder(
                Pair("u", "x"), Pair("u", "y"),
                Pair("v", "x"), Pair("v", "y")
        )
    }
}
