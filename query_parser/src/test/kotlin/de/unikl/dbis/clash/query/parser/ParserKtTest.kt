package de.unikl.dbis.clash.query.parser

import de.unikl.dbis.clash.query.AttributeAccess
import de.unikl.dbis.clash.query.BinaryEquality
import de.unikl.dbis.clash.query.ConstantEquality
import de.unikl.dbis.clash.query.RelationAlias
import de.unikl.dbis.clash.query.WindowDefinition
import io.mockk.mockk
import net.sf.jsqlparser.expression.DateValue
import net.sf.jsqlparser.expression.DoubleValue
import net.sf.jsqlparser.expression.HexValue
import net.sf.jsqlparser.expression.LongValue
import net.sf.jsqlparser.expression.NullValue
import net.sf.jsqlparser.expression.SignedExpression
import net.sf.jsqlparser.expression.StringValue
import net.sf.jsqlparser.expression.TimeValue
import net.sf.jsqlparser.expression.TimestampValue
import net.sf.jsqlparser.expression.operators.relational.EqualsTo
import net.sf.jsqlparser.schema.Column
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.entry
import org.junit.jupiter.api.Test

internal class ParserKtTest {

    @Test
    fun extractPredicates() {
    }

    @Test
    fun `from without window and without alias is found`() {
        val qs1 = """
            SELECT *
            FROM x
        """.trimIndent()
        val q1 = parseQuery(qs1)
        assertThat(q1.result.inputs.size).isEqualTo(1)
        assertThat(q1.result.inputs).containsExactly(
                entry(RelationAlias("x"), WindowDefinition.infinite())
        )
    }

    @Test
    fun `from without window and with alias is found`() {
        val qs1 = """
            SELECT *
            FROM x y
        """.trimIndent()
        val q1 = parseQuery(qs1)
        assertThat(q1.result.inputs.size).isEqualTo(1)
        assertThat(q1.result.inputs).containsExactly(
                entry(RelationAlias("y"), WindowDefinition.infinite())
        )
    }

    @Test
    fun `from with window and without alias is found`() {
        val qs1 = """
            SELECT *
            FROM x('sliding', '1', 'minute')
        """.trimIndent()
        val q1 = parseQuery(qs1)
        assertThat(q1.result.inputs.size).isEqualTo(1)
        assertThat(q1.result.inputs).containsExactly(
                entry(RelationAlias("x"), WindowDefinition.minutes(1))
        )
    }

    @Test
    fun `from with window and with alias is found`() {
        val qs1 = """
            SELECT *
            FROM x('sliding', '1', 'minute') y
        """.trimIndent()
        val q1 = parseQuery(qs1)
        assertThat(q1.result.inputs.size).isEqualTo(1)
        assertThat(q1.result.inputs).containsExactly(
                entry(RelationAlias("y"), WindowDefinition.minutes(1))
        )
    }

    @Test
    fun `from without window and without alias is found for multiple relations`() {
        val qs1 = """
            SELECT *
            FROM x1, x2
        """.trimIndent()
        val q1 = parseQuery(qs1)
        assertThat(q1.result.inputs.size).isEqualTo(2)
        assertThat(q1.result.inputs).containsExactly(
                entry(RelationAlias("x1"), WindowDefinition.infinite()),
                entry(RelationAlias("x2"), WindowDefinition.infinite())
        )
    }

    @Test
    fun `from without window and with alias is found for multiple relations`() {
        val qs1 = """
            SELECT *
            FROM x1 y1, x2 y2
        """.trimIndent()
        val q1 = parseQuery(qs1)
        assertThat(q1.result.inputs.size).isEqualTo(2)
        assertThat(q1.result.inputs).containsExactly(
                entry(RelationAlias("y1"), WindowDefinition.infinite()),
                entry(RelationAlias("y2"), WindowDefinition.infinite())
        )
    }

    @Test
    fun `from with window and without alias is found for multiple relations`() {
        val qs1 = """
            SELECT *
            FROM x1('sliding', '1', 'minute'),
                 x2('sliding', '5', 'seconds')
        """.trimIndent()
        val q1 = parseQuery(qs1)
        assertThat(q1.result.inputs.size).isEqualTo(2)
        assertThat(q1.result.inputs).containsExactly(
                entry(RelationAlias("x1"), WindowDefinition.minutes(1)),
                entry(RelationAlias("x2"), WindowDefinition.seconds(5))
        )
    }

    @Test
    fun `from with window and with alias is found for multiple relations`() {
        val qs1 = """
            SELECT *
            FROM x1('sliding', '2', 'hours') y1,
                 x2('sliding', '1', 'minute') y2
        """.trimIndent()
        val q1 = parseQuery(qs1)
        assertThat(q1.result.inputs.size).isEqualTo(2)
        assertThat(q1.result.inputs).containsExactly(
                entry(RelationAlias("y1"), WindowDefinition.hours(2)),
                entry(RelationAlias("y2"), WindowDefinition.minutes(1))

        )
    }

    @Test
    fun `finds unary equality with constant`() {
        val qs1 = """
            SELECT *
            FROM x
            WHERE x.a = 'foo'
        """.trimIndent()
        val q1 = parseQuery(qs1)
        assertThat(q1.result.filters).containsExactly(
                ConstantEquality(AttributeAccess("x", "a"), "foo")
        )
    }

    @Test
    fun `unary equality without relation qualifier throws exception`() {
        val qs1 = """
            SELECT *
            FROM x
            WHERE a = 'foo'
        """.trimIndent()
        assertThatThrownBy { parseQuery(qs1) }.isInstanceOf(QueryParseException::class.java)
    }

    @Test
    fun `finds binary equality`() {
        val qs1 = """
            SELECT *
            FROM x, y
            WHERE x.a = y.b
        """.trimIndent()
        val q1 = parseQuery(qs1)
        assertThat(q1.result.joinPredicates).containsExactly(
                BinaryEquality(AttributeAccess("x", "a"), AttributeAccess("y", "b"))
        )
    }

    @Test
    fun `binary equality without left relation qualifier throws exception`() {
        val qs1 = """
            SELECT *
            FROM x, y
            WHERE a = y.b
        """.trimIndent()
        assertThatThrownBy { parseQuery(qs1) }.isInstanceOf(QueryParseException::class.java)
    }

    @Test
    fun `binary equality without right relation qualifier throws exception`() {
        val qs1 = """
            SELECT *
            FROM x, y
            WHERE x.a = b
        """.trimIndent()
        assertThatThrownBy { parseQuery(qs1) }.isInstanceOf(QueryParseException::class.java)
    }

    @Test
    fun `isUnary returns true for constants and schema`() {
        val e1 = EqualsTo()
        e1.leftExpression = Column("x")
        e1.rightExpression = DoubleValue("1.02")
        assertThat(isUnary(e1)).isTrue()

        val e2 = EqualsTo()
        e2.leftExpression = DoubleValue("1.02")
        e2.rightExpression = Column("x")
        assertThat(isUnary(e2)).isTrue()
    }

    @Test
    fun `isUnary returns false for binary joins`() {
        val e1 = EqualsTo()
        e1.leftExpression = Column("x")
        e1.rightExpression = Column("y")
        assertThat(isUnary(e1)).isFalse()
    }

    @Test
    fun `positive examples for isConstant run through`() {
        assertThat(isConstant(mockk<DateValue>())).isTrue()
        assertThat(isConstant(mockk<DoubleValue>())).isTrue()
        assertThat(isConstant(mockk<HexValue>())).isTrue()
        assertThat(isConstant(mockk<LongValue>())).isTrue()
        assertThat(isConstant(mockk<NullValue>())).isTrue()
        assertThat(isConstant(mockk<SignedExpression>())).isTrue()
        assertThat(isConstant(mockk<StringValue>())).isTrue()
        assertThat(isConstant(mockk<TimestampValue>())).isTrue()
        assertThat(isConstant(mockk<TimeValue>())).isTrue()
    }

    @Test
    fun `positive examples for isColumn run through`() {
        assertThat(isColumn(Column())).isTrue()
    }

    @Test
    fun `regression test - date function is treated as constant`() {
        val q = """SELECT * FROM x WHERE o.orderdate < date '1995-03-01'"""
    }

    @Test
    fun `query that references in where clause non introduced alias is rejected`() {
        val q1 = """SELECT * FROM x xa, y ya WHERE xa.foo = za.bar"""
        assertThatThrownBy { parseQuery(q1) }.isInstanceOf(QueryParseException::class.java)

        val q2 = """SELECT * FROM x xa, y ya WHERE za.foo = 19"""
        assertThatThrownBy { parseQuery(q2) }.isInstanceOf(QueryParseException::class.java)
    }

    @Test
    fun `query that references in select clause non introduced alias is rejected`() {
        val q = """SELECT xa.foo, za.baz FROM x xa, y ya WHERE xa.foo = ya.bar"""
        assertThatThrownBy { parseQuery(q) }.isInstanceOf(QueryParseException::class.java)
    }

    @Test
    fun `selection list is correctly extracted`() {
        val qs = """SELECT x.foo, y.bar, x.baz bazz FROM x, y"""
        val q = parseQuery(qs)

        // TODO TEST THAT PROJECTION LIST WORKS
    }

    /*
    @Test
    fun `regression test - tpch2 with infinite windows`() {
        val qs1 = """
            SELECT s.acctbal, s.name, n.name, p.partkey, p.mfgr, s.address, s.phone, s.comment
            FROM part p, supplier s, partsupp ps, nation n, region r
            WHERE p.partkey = ps.partkey AND s.suppkey = ps.suppkey AND s.nationkey = n.nationkey AND n.regionkey = r.regionkey
            AND p.size = '32' AND p.type like '%BRASS' AND r.name = 'AFRICA'
        """.trimIndent()

        val q1 = parseQuery(qs1)

        assertThat(q1.inputs).containsExactlyInAnyOrder(
                QueryInput(relationOf("p"), WindowDefinition.infinite()),
                QueryInput(relationOf("s"), WindowDefinition.infinite()),
                QueryInput(relationOf("ps"), WindowDefinition.infinite()),
                QueryInput(relationOf("n"), WindowDefinition.infinite()),
                QueryInput(relationOf("r"), WindowDefinition.infinite())
        )

        assertThat(q1.joinPredicates).containsExactlyInAnyOrder(
                BinaryPredicate.fromString("p.partkey = ps.partkey"),
                BinaryPredicate.fromString("s.suppkey = ps.suppkey"),
                BinaryPredicate.fromString("s.nationkey = n.nationkey"),
                BinaryPredicate.fromString("n.regionkey = r.regionkey")
        )

        assertThat(q1.inputPredicates).containsExactlyInAnyOrder(
                ConstantEquality(AttributeAccess("p", "size"), "32"),
                UnaryLike(AttributeAccess("p", "type"), "%BRASS"),
                ConstantEquality(AttributeAccess("r", "name"), "AFRICA")
        )
    }

    @Test
    fun `regression test - tpch2 with time-based windows`() {
        val qs1 = """
            SELECT s.acctbal, s.name, n.name, p.partkey, p.mfgr, s.address, s.phone, s.comment
            FROM part('sliding', '1', 'minute') p, supplier('sliding', '5', 'minutes') s, partsupp('sliding', '40', 'seconds') ps, nation('sliding', '1', 'hour') n, region r
            WHERE p.partkey = ps.partkey AND s.suppkey = ps.suppkey AND s.nationkey = n.nationkey AND n.regionkey = r.regionkey
        """.trimIndent()

        val q1 = parseQuery(qs1)

        assertThat(q1.inputs).containsExactlyInAnyOrder(
                QueryInput(relationOf("p"), WindowDefinition.minutes(1)),
                QueryInput(relationOf("s"), WindowDefinition.minutes(5)),
                QueryInput(relationOf("ps"), WindowDefinition.seconds(40)),
                QueryInput(relationOf("n"), WindowDefinition.hours(1)),
                QueryInput(relationOf("r"), WindowDefinition.infinite())
        )

        assertThat(q1.joinPredicates).containsExactlyInAnyOrder(
                BinaryPredicate.fromString("p.partkey = ps.partkey"),
                BinaryPredicate.fromString("s.suppkey = ps.suppkey"),
                BinaryPredicate.fromString("s.nationkey = n.nationkey"),
                BinaryPredicate.fromString("n.regionkey = r.regionkey")
        )
    }*/
}
