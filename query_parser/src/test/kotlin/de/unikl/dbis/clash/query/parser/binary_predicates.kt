package de.unikl.dbis.clash.query.parser

import de.unikl.dbis.clash.query.AttributeAccess
import de.unikl.dbis.clash.query.BinaryLessThan
import de.unikl.dbis.clash.query.BinaryLessThanOrEqual
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class BinaryPredicatesKtTest {
    @Test
    fun `less-than parsed correctly`() {
        val qs = "SELECT * FROM r, s WHERE r.x < s.y"
        val q = parseQuery(qs)
        assertThat(q.result.binaryPredicates).containsExactly(
            BinaryLessThan(AttributeAccess("r", "x"), AttributeAccess("s", "y"))
        )
    }

    @Test
    fun `less-than-or-equal parsed correctly`() {
        val qs = "SELECT * FROM r, s WHERE r.x <= s.y"
        val q = parseQuery(qs)
        assertThat(q.result.binaryPredicates).containsExactly(
                BinaryLessThanOrEqual(AttributeAccess("r", "x"), AttributeAccess("s", "y"))
        )
    }

    @Test
    fun `greater-than parsed correctly`() {
        val qs = "SELECT * FROM r, s WHERE r.x > s.y"
        val q = parseQuery(qs)
        assertThat(q.result.binaryPredicates).containsExactly(
                BinaryLessThan(AttributeAccess("s", "y"), AttributeAccess("r", "x"))
        )
    }

    @Test
    fun `greater-than-or-equal parsed correctly`() {
        val qs = "SELECT * FROM r, s WHERE r.x >= s.y"
        val q = parseQuery(qs)
        assertThat(q.result.binaryPredicates).containsExactly(
                BinaryLessThanOrEqual(AttributeAccess("s", "y"), AttributeAccess("r", "x"))
        )
    }
}