package de.unikl.dbis.clash.query.parser

import de.unikl.dbis.clash.query.AttributeAccess
import de.unikl.dbis.clash.query.BinaryEquality
import de.unikl.dbis.clash.query.ConstantEquality
import de.unikl.dbis.clash.query.OrList
import de.unikl.dbis.clash.query.RelationAlias
import de.unikl.dbis.clash.query.UnaryLike
import de.unikl.dbis.clash.query.UnaryNotLike
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Regression tests for join order benchmark
 */
internal class JOB_Fragment_parserKtTest {

    @Test
    fun parseUnaryEquality() {
        val q1p1 = "ct.kind = 'production companies'"
        val q1p1res = parsePredicate(q1p1)
        assertThat(q1p1res).isInstanceOfSatisfying(ConstantEquality::class.java) {
            assertThat(it.attributeAccess).isEqualTo(AttributeAccess("ct", "kind"))
            assertThat(it.constant).isEqualTo("production companies")
        }

        val q1p2 = "it.info = 'top 250 rank'"
        val q1p2res = parsePredicate(q1p2)
        assertThat(q1p2res).isInstanceOfSatisfying(ConstantEquality::class.java) {
            assertThat(it.attributeAccess).isEqualTo(AttributeAccess("it", "info"))
            assertThat(it.constant).isEqualTo("top 250 rank")
        }
    }

    @Test
    fun parseUnaryLike() {
        val q1p1 = "mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'"
        val q1p1res = parsePredicate(q1p1)
        assertThat(q1p1res).isInstanceOfSatisfying(UnaryNotLike::class.java) {
            assertThat(it.attributeAccess).isEqualTo(AttributeAccess("mc", "note"))
            assertThat(it.likeExpr).isEqualTo("%(as Metro-Goldwyn-Mayer Pictures)%")
        }

        val q1p2 = "mc.note LIKE '%(co-production)%'"
        val q1p2res = parsePredicate(q1p2)
        assertThat(q1p2res).isInstanceOfSatisfying(UnaryLike::class.java) {
            assertThat(it.attributeAccess).isEqualTo(AttributeAccess("mc", "note"))
            assertThat(it.likeExpr).isEqualTo("%(co-production)%")
        }

        val q1p3 = "mc.note LIKE '%(presents)%'"
        val q1p3res = parsePredicate(q1p3)
        assertThat(q1p3res).isInstanceOfSatisfying(UnaryLike::class.java) {
            assertThat(it.attributeAccess).isEqualTo(AttributeAccess("mc", "note"))
            assertThat(it.likeExpr).isEqualTo("%(presents)%")
        }
    }

    @Test
    fun parseOrList() {
        val q1p = "(mc.note LIKE '%(co-production)%' OR mc.note LIKE '%(presents)%')"
        val q1pres = parsePredicate(q1p)
        assertThat(q1pres).isInstanceOfSatisfying(OrList::class.java) {
            assertThat(it.inner).containsExactlyInAnyOrder(
                UnaryLike(AttributeAccess("mc.note"), "%(co-production)%"),
                UnaryLike(AttributeAccess("mc.note"), "%(presents)%")
            )
        }
    }

    @Test
    fun parseBinaryEquality() {
        val q1p1 = "ct.id = mc.company_type_id"
        val q1p1res = parsePredicate(q1p1)
        assertThat(q1p1res).isInstanceOfSatisfying(BinaryEquality::class.java) {
            assertThat(it.leftRelationAlias).isEqualTo(RelationAlias("ct"))
            assertThat(it.leftAttributeAccess).isEqualTo(AttributeAccess("ct", "id"))
            assertThat(it.rightRelationAlias).isEqualTo(RelationAlias("mc"))
            assertThat(it.rightAttributeAccess).isEqualTo(AttributeAccess("mc", "company_type_id"))
        }

        val q1p2 = "t.id = mc.movie_id"
        val q1p2res = parsePredicate(q1p2)
        assertThat(q1p2res).isInstanceOfSatisfying(BinaryEquality::class.java) {
            assertThat(it.leftRelationAlias).isEqualTo(RelationAlias("t"))
            assertThat(it.leftAttributeAccess).isEqualTo(AttributeAccess("t", "id"))
            assertThat(it.rightRelationAlias).isEqualTo(RelationAlias("mc"))
            assertThat(it.rightAttributeAccess).isEqualTo(AttributeAccess("mc", "movie_id"))
        }

        val q1p3 = "t.id = mi_idx.movie_id"
        val q1p3res = parsePredicate(q1p3)
        assertThat(q1p3res).isInstanceOfSatisfying(BinaryEquality::class.java) {
            assertThat(it.leftRelationAlias).isEqualTo(RelationAlias("t"))
            assertThat(it.leftAttributeAccess).isEqualTo(AttributeAccess("t", "id"))
            assertThat(it.rightRelationAlias).isEqualTo(RelationAlias("mi_idx"))
            assertThat(it.rightAttributeAccess).isEqualTo(AttributeAccess("mi_idx", "movie_id"))
        }

        val q1p4 = "mc.movie_id = mi_idx.movie_id"
        val q1p4res = parsePredicate(q1p4)
        assertThat(q1p4res).isInstanceOfSatisfying(BinaryEquality::class.java) {
            assertThat(it.leftRelationAlias).isEqualTo(RelationAlias("mc"))
            assertThat(it.leftAttributeAccess).isEqualTo(AttributeAccess("mc", "movie_id"))
            assertThat(it.rightRelationAlias).isEqualTo(RelationAlias("mi_idx"))
            assertThat(it.rightAttributeAccess).isEqualTo(AttributeAccess("mi_idx", "movie_id"))
        }

        val q1p5 = "it.id = mi_idx.info_type_id"
        val q1p5res = parsePredicate(q1p5)
        assertThat(q1p5res).isInstanceOfSatisfying(BinaryEquality::class.java) {
            assertThat(it.leftRelationAlias).isEqualTo(RelationAlias("it"))
            assertThat(it.leftAttributeAccess).isEqualTo(AttributeAccess("it", "id"))
            assertThat(it.rightRelationAlias).isEqualTo(RelationAlias("mi_idx"))
            assertThat(it.rightAttributeAccess).isEqualTo(AttributeAccess("mi_idx", "info_type_id"))
        }
    }

}
