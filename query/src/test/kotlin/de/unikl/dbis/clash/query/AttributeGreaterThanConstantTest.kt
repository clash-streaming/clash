package de.unikl.dbis.clash.query

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test

internal class AttributeGreaterThanConstantTest {
    @Test
    fun `predicate works for numbers`() {
        val attributeAccess = AttributeAccess("x", "a")
        val predicate = AttributeGreaterThanConstant(attributeAccess, 55)

        val d1 = TestTuple(attributeAccess, "8")
        Assertions.assertThat(predicate.evaluate(d1)).isFalse()

        val d2 = TestTuple(attributeAccess, "41")
        Assertions.assertThat(predicate.evaluate(d2)).isFalse()

        val d3 = TestTuple(attributeAccess, "89")
        Assertions.assertThat(predicate.evaluate(d3)).isTrue()

        val d4 = TestTuple(attributeAccess, "120")
        Assertions.assertThat(predicate.evaluate(d4)).isTrue()
    }

    @Test
    fun `predicate works for strings`() {
        val attributeAccess = AttributeAccess("x", "a")
        val predicate = AttributeGreaterThanConstant(attributeAccess, "55")

        val d1 = TestTuple(attributeAccess, "8")
        Assertions.assertThat(predicate.evaluate(d1)).isTrue()

        val d2 = TestTuple(attributeAccess, "41")
        Assertions.assertThat(predicate.evaluate(d2)).isFalse()

        val d3 = TestTuple(attributeAccess, "89")
        Assertions.assertThat(predicate.evaluate(d3)).isTrue()

        val d4 = TestTuple(attributeAccess, "120")
        Assertions.assertThat(predicate.evaluate(d4)).isFalse()
    }
}

