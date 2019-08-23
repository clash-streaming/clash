package de.unikl.dbis.clash.support

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.*

import org.junit.jupiter.api.Test

internal class TimeKtTest {

    @Test
    fun minutesToMillis() {
        val min1 = 1L
        val min2 = 2L
        val min5 = 5L

        assertThat(minutesToMillis(min1)).isEqualTo(60_000)
        assertThat(minutesToMillis(min2)).isEqualTo(120_000)
        assertThat(minutesToMillis(min5)).isEqualTo(300_000)
    }
}