package de.unikl.dbis.clash.query.parser

import de.unikl.dbis.clash.query.AttributeAccess
import de.unikl.dbis.clash.query.AttributeGreaterThanConstant
import de.unikl.dbis.clash.query.AttributeGreaterThanOrEqualConstant
import de.unikl.dbis.clash.query.AttributeLessThanConstant
import de.unikl.dbis.clash.query.AttributeLessThanOrEqualConstant
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class UnaryPredicatesKtTest {
    @Test
    fun `attribute less-than constant parsed correctly`() {
        val qlongs = "SELECT * FROM r, s WHERE r.x < 5"
        val qdoubles = "SELECT * FROM r, s WHERE r.x < 1.582"
        val qlong = parseQuery(qlongs)
        val qdouble = parseQuery(qdoubles)

        assertThat(qlong.result.unaryPredicates).containsExactly(
                AttributeLessThanConstant(AttributeAccess("r", "x"), 5L)
        )
        assertThat(qdouble.result.unaryPredicates).containsExactly(
                AttributeLessThanConstant(AttributeAccess("r", "x"), 1.582)
        )
    }

    @Test
    fun `constant less-than attribute parsed correctly`() {
        val qlongs = "SELECT * FROM r, s WHERE 3 < s.x"
        val qdoubles = "SELECT * FROM r, s WHERE 3.141 < s.x"
        val qlong = parseQuery(qlongs)
        val qdouble = parseQuery(qdoubles)

        assertThat(qlong.result.unaryPredicates).containsExactly(
                AttributeLessThanConstant(AttributeAccess("s", "x"), 3L)
        )
        assertThat(qdouble.result.unaryPredicates).containsExactly(
                AttributeLessThanConstant(AttributeAccess("s", "x"), 3.141)
        )
    }

    @Test
    fun `attribute less-than-or-equal constant parsed correctly`() {
        val qlongs = "SELECT * FROM r, s WHERE r.x <= 5"
        val qdoubles = "SELECT * FROM r, s WHERE r.x <= 1.582"
        val qlong = parseQuery(qlongs)
        val qdouble = parseQuery(qdoubles)

        assertThat(qlong.result.unaryPredicates).containsExactly(
                AttributeLessThanOrEqualConstant(AttributeAccess("r", "x"), 5L)
        )
        assertThat(qdouble.result.unaryPredicates).containsExactly(
                AttributeLessThanOrEqualConstant(AttributeAccess("r", "x"), 1.582)
        )
    }

    @Test
    fun `constant less-than-or-equal attribute parsed correctly`() {
        val qlongs = "SELECT * FROM r, s WHERE 3 <= s.x"
        val qdoubles = "SELECT * FROM r, s WHERE 3.141 <= s.x"
        val qlong = parseQuery(qlongs)
        val qdouble = parseQuery(qdoubles)

        assertThat(qlong.result.unaryPredicates).containsExactly(
                AttributeLessThanOrEqualConstant(AttributeAccess("s", "x"), 3L)
        )
        assertThat(qdouble.result.unaryPredicates).containsExactly(
                AttributeLessThanOrEqualConstant(AttributeAccess("s", "x"), 3.141)
        )
    }

    @Test
    fun `attribute greater-than constant parsed correctly`() {
        val qlongs = "SELECT * FROM r, s WHERE r.x > 5"
        val qdoubles = "SELECT * FROM r, s WHERE r.x > 1.582"
        val qlong = parseQuery(qlongs)
        val qdouble = parseQuery(qdoubles)

        assertThat(qlong.result.unaryPredicates).containsExactly(
                AttributeGreaterThanConstant(AttributeAccess("r", "x"), 5L)
        )
        assertThat(qdouble.result.unaryPredicates).containsExactly(
                AttributeGreaterThanConstant(AttributeAccess("r", "x"), 1.582)
        )
    }

    @Test
    fun `constant greater-than attribute parsed correctly`() {
        val qlongs = "SELECT * FROM r, s WHERE 3 > s.x"
        val qdoubles = "SELECT * FROM r, s WHERE 3.141 > s.x"
        val qlong = parseQuery(qlongs)
        val qdouble = parseQuery(qdoubles)

        assertThat(qlong.result.unaryPredicates).containsExactly(
                AttributeGreaterThanConstant(AttributeAccess("s", "x"), 3L)
        )
        assertThat(qdouble.result.unaryPredicates).containsExactly(
                AttributeGreaterThanConstant(AttributeAccess("s", "x"), 3.141)
        )
    }

    @Test
    fun `attribute greater-than-or-equal constant parsed correctly`() {
        val qlongs = "SELECT * FROM r, s WHERE r.x >= 5"
        val qdoubles = "SELECT * FROM r, s WHERE r.x >= 1.582"
        val qlong = parseQuery(qlongs)
        val qdouble = parseQuery(qdoubles)

        assertThat(qlong.result.unaryPredicates).containsExactly(
                AttributeGreaterThanOrEqualConstant(AttributeAccess("r", "x"), 5L)
        )
        assertThat(qdouble.result.unaryPredicates).containsExactly(
                AttributeGreaterThanOrEqualConstant(AttributeAccess("r", "x"), 1.582)
        )
    }

    @Test
    fun `constant greater-than-or-equal attribute parsed correctly`() {
        val qlongs = "SELECT * FROM r, s WHERE 3 >= s.x"
        val qdoubles = "SELECT * FROM r, s WHERE 3.141 >= s.x"
        val qlong = parseQuery(qlongs)
        val qdouble = parseQuery(qdoubles)

        assertThat(qlong.result.unaryPredicates).containsExactly(
                AttributeGreaterThanOrEqualConstant(AttributeAccess("s", "x"), 3L)
        )
        assertThat(qdouble.result.unaryPredicates).containsExactly(
                AttributeGreaterThanOrEqualConstant(AttributeAccess("s", "x"), 3.141)
        )
    }
}
