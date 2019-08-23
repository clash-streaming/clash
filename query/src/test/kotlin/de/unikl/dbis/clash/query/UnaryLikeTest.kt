package de.unikl.dbis.clash.query

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class UnaryLikeTest {

    @Test
    fun `search term in the middle works`() {
        val attributeAccess = AttributeAccess("x", "a")
        val like = UnaryLike(attributeAccess, "\$foo\$")

        val d1 = TestTuple(attributeAccess, "asdfoojkl")
        assertThat(like.evaluate(d1)).isTrue()

        val d2 = TestTuple(attributeAccess, "asdfoo")
        assertThat(like.evaluate(d2)).isTrue()

        val d3 = TestTuple(attributeAccess, "fooerkd")
        assertThat(like.evaluate(d3)).isTrue()

        val d4 = TestTuple(attributeAccess, "foo")
        assertThat(like.evaluate(d4)).isTrue()

        val d5 = TestTuple(attributeAccess, "asdfogj")
        assertThat(like.evaluate(d5)).isFalse()

        val d6 = TestTuple(attributeAccess, "")
        assertThat(like.evaluate(d6)).isFalse()
    }

    @Test
    fun `search term at the beginning works`() {
        val attributeAccess = AttributeAccess("x", "a")
        val like = UnaryLike(attributeAccess, "foo\$")

        val d1 = TestTuple(attributeAccess, "asdfoojkl")
        assertThat(like.evaluate(d1)).isFalse()

        val d2 = TestTuple(attributeAccess, "asdfoo")
        assertThat(like.evaluate(d2)).isFalse()

        val d3 = TestTuple(attributeAccess, "fooerkd")
        assertThat(like.evaluate(d3)).isTrue()

        val d4 = TestTuple(attributeAccess, "foo")
        assertThat(like.evaluate(d4)).isTrue()

        val d5 = TestTuple(attributeAccess, "asdfogj")
        assertThat(like.evaluate(d5)).isFalse()

        val d6 = TestTuple(attributeAccess, "")
        assertThat(like.evaluate(d6)).isFalse()

        val d7 = TestTuple(attributeAccess, "foasd")
        assertThat(like.evaluate(d7)).isFalse()

        val d8 = TestTuple(attributeAccess, "ooasd")
        assertThat(like.evaluate(d8)).isFalse()
    }

    @Test
    fun `search term at the end works`() {
        val attributeAccess = AttributeAccess("x", "a")
        val like = UnaryLike(attributeAccess, "\$foo")

        val d1 = TestTuple(attributeAccess, "asdfoojkl")
        assertThat(like.evaluate(d1)).isFalse()

        val d2 = TestTuple(attributeAccess, "asdfoo")
        assertThat(like.evaluate(d2)).isTrue()

        val d3 = TestTuple(attributeAccess, "fooerkd")
        assertThat(like.evaluate(d3)).isFalse()

        val d4 = TestTuple(attributeAccess, "foo")
        assertThat(like.evaluate(d4)).isTrue()

        val d5 = TestTuple(attributeAccess, "asdfogj")
        assertThat(like.evaluate(d5)).isFalse()

        val d6 = TestTuple(attributeAccess, "")
        assertThat(like.evaluate(d6)).isFalse()

        val d7 = TestTuple(attributeAccess, "asdfo")
        assertThat(like.evaluate(d7)).isFalse()

        val d8 = TestTuple(attributeAccess, "asdoo")
        assertThat(like.evaluate(d8)).isFalse()
    }
}