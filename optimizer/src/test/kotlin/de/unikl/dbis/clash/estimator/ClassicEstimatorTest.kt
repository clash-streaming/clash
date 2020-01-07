package de.unikl.dbis.clash.estimator

import de.unikl.dbis.clash.datacharacteristics.ManualCharacteristics
import de.unikl.dbis.clash.query.RelationAlias
import de.unikl.dbis.clash.query.parser.parseQuery
import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class ClassicEstimatorTest {
    @Test
    fun `size estimation for three way full-history is correct`() {
        val q = parseQuery("Select * from r, s, t where r.a = s.a and t.b = s.b")
        val r = RelationAlias("r")
        val s = RelationAlias("s")
        val t = RelationAlias("t")
        val rRate = 100.0
        val sRate = 120.0
        val tRate = 90.0
        val rsSelectivity = 0.005
        val stSelectivity = 0.012
        val dc = ManualCharacteristics()
        dc.setRate(r, rRate)
        dc.setRate(s, sRate)
        dc.setRate(t, tRate)
        dc.setSelectivity(r, s, rsSelectivity)
        dc.setSelectivity(s, t, stSelectivity)

        val subject1 = estimateSize(q.result, dc)
        assertThat(subject1).isCloseTo(rRate * sRate * tRate * rsSelectivity * stSelectivity, Assertions.offset(0.001))

        val subject2 = estimateSize(listOf(r, s, t), dc)
        assertThat(subject2).isCloseTo(rRate * sRate * tRate * rsSelectivity * stSelectivity, Assertions.offset(0.001))
    }
}
