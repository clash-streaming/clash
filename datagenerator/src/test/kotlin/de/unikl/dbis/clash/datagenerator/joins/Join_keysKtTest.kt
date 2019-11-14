package de.unikl.dbis.clash.datagenerator.joins

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.params.ParameterizedTest;

import org.assertj.core.api.Assertions.*
import org.assertj.core.data.Percentage


import org.junit.jupiter.api.Test
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

internal class Join_keysKtTest {

    data class TestCase(val numStores: Long, val numProbes: Long, val selectivity: Double, val expectedResult: Long)

    @ParameterizedTest
    @MethodSource("testCases")
    fun generateTwoRelationKeys(testCase: TestCase) {
        val (rel1, rel2) = generateTwoRelationKeys(testCase.numStores, testCase.numProbes, testCase.selectivity)
        val joinResults = joinSize(rel1, rel2)
        assertThat(joinResults).isCloseTo(testCase.expectedResult, Percentage.withPercentage(1.0))
    }

    fun joinSize(rel1: List<Long>, rel2: List<Long>): Long {
        var joinResults = 0L
        for(store in rel1) {
            for(probe in rel2) {
                if(store == probe) {
                    joinResults++
                }
            }
        }
        return joinResults
    }

    companion object {
        @JvmStatic
        private fun testCases() = Stream.of(
                // First relation is bigger
                Join_keysKtTest.TestCase(10_000, 1_000, 0.01, 100_000),
                Join_keysKtTest.TestCase(10_000, 5_000, 0.01, 500_000),
                Join_keysKtTest.TestCase(10_000, 10_000, 0.01, 1_000_000),
                Join_keysKtTest.TestCase(10_000, 1_000, 0.001, 10_000),
                Join_keysKtTest.TestCase(10_000, 5_000, 0.001, 50_000),
                Join_keysKtTest.TestCase(10_000, 10_000, 0.001, 100_000),
                Join_keysKtTest.TestCase(10_000, 1_000, 0.0001, 1_000),
                Join_keysKtTest.TestCase(10_000, 5_000, 0.0001, 5_000),
                Join_keysKtTest.TestCase(10_000, 10_000, 0.0001, 10_000),

                // Second relation is bigger
                Join_keysKtTest.TestCase(1_000, 10_000, 0.01, 100_000),
                Join_keysKtTest.TestCase(5_000, 10_000, 0.01, 500_000),
                Join_keysKtTest.TestCase(10_000, 10_000, 0.01, 1_000_000),
                Join_keysKtTest.TestCase(1_000, 10_000, 0.001, 10_000),
                Join_keysKtTest.TestCase(5_000, 10_000, 0.001, 50_000),
                Join_keysKtTest.TestCase(10_000, 10_000, 0.001, 100_000),
                Join_keysKtTest.TestCase(1_000, 10_000, 0.0001, 1_000),
                Join_keysKtTest.TestCase(5_000, 10_000, 0.0001, 5_000),
                Join_keysKtTest.TestCase(10_000, 10_000, 0.0001, 10_000)
        )
    }
}
