package de.unikl.dbis.clash.query.parser

import de.unikl.dbis.clash.query.RelationAlias
import de.unikl.dbis.clash.query.Similarity
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test



internal class CustomPredicateTests {
    @Test
    fun `findsOutCustomRowPredicate`() {
        val qs1 = """
            SELECT *
            FROM x, y
            WHERE similar(x, y)
        """.trimIndent()
        val q1 = parseQuery(qs1)
        Assertions.assertThat(q1.result.binaryPredicates).containsExactly(
                Similarity(RelationAlias("x"), RelationAlias("y"))
        )
    }
}