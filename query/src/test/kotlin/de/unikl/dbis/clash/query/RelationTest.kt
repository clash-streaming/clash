package de.unikl.dbis.clash.query

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class RelationTest {
    @Test
    fun `subRelation works with single relation`() {
        val superRelation = Relation(
                mapOf(
                        RelationAlias("x") to WindowDefinition.hours(10),
                        RelationAlias("y") to WindowDefinition.hours(5)
                ),
                listOf(
                        BinaryPredicate.fromString("x.a = y.a")
                ),
                listOf() // TODO
        )
        // TODO: this test can only continue if attribute accesses are meaningfully defined
//        assertThat(superRelation.subRelation(RelationAlias("x"))).isEqualTo(
//                Relation(
//                        mapOf(
//                                RelationAlias("x") to WindowDefinition.hours(10)
//                        ),
//                        listOf(),
//                        listOf() // TODO
//                )
//        )
    }

    @Test
    fun `subRelation works with two relations`() {
        val superRelation = Relation(
                mapOf(
                        RelationAlias("x") to WindowDefinition.infinite(),
                        RelationAlias("y") to WindowDefinition.minutes(6),
                        RelationAlias("z") to WindowDefinition.hours(10)
                ),
                listOf(
                        BinaryPredicate.fromString("x.a = y.a"),
                        BinaryPredicate.fromString("z.b = y.b")
                ),
                listOf() // TODO
        )

        // TODO: this test can only continue if attribute accesses are meaningfully defined
//        assertThat(superRelation.subRelation(RelationAlias("x"), RelationAlias("y"))).isEqualTo(
//                Relation(
//                        mapOf(
//                                RelationAlias("x") to WindowDefinition.infinite(),
//                                RelationAlias("y") to WindowDefinition.minutes(6)
//                        ),
//                        listOf(
//                                BinaryPredicate.fromString("x.a = y.a")
//                        ),
//                        listOf() // TODO
//                )
//        )
    }

}