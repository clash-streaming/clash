package de.unikl.dbis.clash.support

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.*

import org.junit.jupiter.api.Test

internal class MathKtTest {

    @Test
    fun `ceil works for positive numbers`() {
        val d1 = 0.7
        assertThat(d1.ceil()).isEqualTo(1)

        val d2 = 51.1
        assertThat(d2.ceil()).isEqualTo(52)
    }

    @Test
    fun `ceil works for negative numbers`() {
        val d1 = -0.7
        assertThat(d1.ceil()).isEqualTo(0)

        val d2 = -51.1
        assertThat(d2.ceil()).isEqualTo(-51)
    }
}