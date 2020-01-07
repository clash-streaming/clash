package de.unikl.dbis.clash.datacharacteristics

import de.unikl.dbis.clash.query.RelationAlias
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class ManualCharacteristicsTest {
    @Test
    fun `SetRate works`() {
        val dc = ManualCharacteristics()
        dc.setRate(RelationAlias("x"), 10.0)
        dc.setRate(RelationAlias("x"), 20.0)

        assertThat(dc.getRate(RelationAlias("x"))).isEqualTo(20.0)
    }

    @Test
    fun `GetRate works`() {
        val dc = ManualCharacteristics()
        dc.setRate(RelationAlias("x"), 10.0)
        dc.setRate(RelationAlias("y"), 20.0)

        assertThat(dc.getRate(RelationAlias("x"))).isEqualTo(10.0)
        assertThat(dc.getRate(RelationAlias("y"))).isEqualTo(20.0)
    }

    @Test
    fun `GetSelectivity works`() {
        val dc = ManualCharacteristics()
        dc.setSelectivity(RelationAlias("x"), RelationAlias("y"), 0.1)
        dc.setSelectivity(RelationAlias("y"), RelationAlias("z"), 0.02)

        assertThat(dc.getSelectivity(RelationAlias("x"), RelationAlias("y"))).isEqualTo(0.1)
        assertThat(dc.getSelectivity(RelationAlias("y"), RelationAlias("z"))).isEqualTo(0.02)
    }

    @Test
    fun `SetSelectivity works`() {
        val dc = ManualCharacteristics()
        dc.setSelectivity(RelationAlias("x"), RelationAlias("y"), 0.1)
        dc.setSelectivity(RelationAlias("x"), RelationAlias("y"), 0.02)

        assertThat(dc.getSelectivity(RelationAlias("x"), RelationAlias("y"))).isEqualTo(0.02)
    }

    @Test
    fun `computeJoinSize works for two relations`() {
        val dc = ManualCharacteristics()
        dc.setRate(RelationAlias("a"), 100.0)
        dc.setRate(RelationAlias("b"), 60.0)
        dc.setSelectivity(RelationAlias("a"), RelationAlias("b"), 0.1)

        assertThat(dc.computeJoinSize(RelationAlias("a"), RelationAlias("b"))).isEqualTo(600.0)
    }

    @Test
    fun `computeJoinSize works for chain`() {
        val dc = ManualCharacteristics()
        dc.setRate(RelationAlias("a"), 100.0)
        dc.setRate(RelationAlias("b"), 60.0)
        dc.setRate(RelationAlias("c"), 10.0)
        dc.setRate(RelationAlias("d"), 200.0)
        dc.setSelectivity(RelationAlias("a"), RelationAlias("b"), 0.01)
        dc.setSelectivity(RelationAlias("b"), RelationAlias("c"), 0.2)
        dc.setSelectivity(RelationAlias("c"), RelationAlias("d"), 0.05)

        assertThat(dc.computeJoinSize(RelationAlias("a"), RelationAlias("b"), RelationAlias("c"))).isEqualTo(120.0)
        assertThat(dc.computeJoinSize(RelationAlias("b"), RelationAlias("c"), RelationAlias("d"))).isEqualTo(1200.0)
        assertThat(dc.computeJoinSize(RelationAlias("a"), RelationAlias("b"), RelationAlias("c"), RelationAlias("d"))).isEqualTo(1200.0)
    }

    @Test
    fun `computeJoinSize works for circles`() {
        val dc = ManualCharacteristics()
        dc.setRate(RelationAlias("a"), 100.0)
        dc.setRate(RelationAlias("b"), 50.0)
        dc.setRate(RelationAlias("c"), 150.0)
        dc.setRate(RelationAlias("d"), 100.0)
        dc.setSelectivity(RelationAlias("a"), RelationAlias("b"), 0.01)
        dc.setSelectivity(RelationAlias("a"), RelationAlias("c"), 0.02)
        dc.setSelectivity(RelationAlias("b"), RelationAlias("c"), 0.05)
        dc.setSelectivity(RelationAlias("b"), RelationAlias("d"), 0.02)
        dc.setSelectivity(RelationAlias("c"), RelationAlias("d"), 0.01)

        assertThat(dc.computeJoinSize(RelationAlias("a"), RelationAlias("b"), RelationAlias("c"))).isEqualTo(7.5)
        assertThat(dc.computeJoinSize(RelationAlias("b"), RelationAlias("c"), RelationAlias("d"))).isEqualTo(7.5)
        assertThat(dc.computeJoinSize(RelationAlias("a"), RelationAlias("b"), RelationAlias("c"), RelationAlias("d"))).isEqualTo(0.15)
    }
}
