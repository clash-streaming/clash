package de.unikl.dbis.clash.optimizer

import de.unikl.dbis.clash.datacharacteristics.ManualCharacteristics
import de.unikl.dbis.clash.optimizer.materializationtree.MatSource
import de.unikl.dbis.clash.optimizer.probeorder.ProbeOrder
import de.unikl.dbis.clash.query.AttributeAccess
import de.unikl.dbis.clash.query.BinaryEquality
import de.unikl.dbis.clash.query.RelationAlias
import de.unikl.dbis.clash.query.relationOf
import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test


internal class CostKtTest {
    val rRate = 100.0
    val sRate = 100.0
    val tRate = 100.0
    val rsSel = 0.01
    val stSel = 0.01

    val dc = {
        val dc = ManualCharacteristics()
        dc.setRate(RelationAlias("R"), rRate)
        dc.setRate(RelationAlias("S"), sRate)
        dc.setRate(RelationAlias("T"), tRate)
        dc.setSelectivity(RelationAlias("R"), RelationAlias("S"), rsSel)
        dc.setSelectivity(RelationAlias("S"), RelationAlias("T"), stSel)
        dc
    }()

    val rParallelism = 1L
    val rNode = MatSource(relationOf("R"), rParallelism, listOf(), rRate)
    val sParallelism = 1L
    val sNode = MatSource(relationOf("S"), sParallelism, listOf(), sRate)
    val tParallelism = 1L
    val tNode = MatSource(relationOf("T"), tParallelism, listOf(), tRate)

    val rsPred = BinaryEquality(AttributeAccess("R", "x"), AttributeAccess("S", "x"))
    val stPred = BinaryEquality(AttributeAccess("S", "y"), AttributeAccess("T", "y"))


    @Test
    fun `probeTuplesSentForProbeOrder works correctly for R,S,T`() {
        val probeOrderForR = ProbeOrder(listOf(
                Pair(rNode, setOf()),
                Pair(sNode, setOf(rsPred)),
                Pair(tNode, setOf(stPred))
        ))
        val result = probeTuplesSentForProbeOrder(dc, probeOrderForR)
        assertThat(result).containsExactly(
                Assertions.entry(rNode, rRate * sParallelism),
                Assertions.entry(sNode, rRate * sRate * rsSel * tParallelism * 1.0 / 2.0)
        )
    }


    @Test
    fun `relationSizeForProbeOrder works correctly for R,S,T`() {
        val probeOrderForR = ProbeOrder(listOf(
                Pair(rNode, setOf()),
                Pair(sNode, setOf(rsPred)),
                Pair(tNode, setOf(stPred))
        ))
        val res = relationSizeForProbeOrder(dc, probeOrderForR)

        assertThat(res.steps.map { Pair(it.first, it.second) }).containsExactlyInAnyOrder(
                Pair(rNode, setOf()),
                Pair(sNode, setOf(rsPred)),
                Pair(tNode, setOf(stPred))
        )
        assertThat(res.steps.first { it.first == rNode }.third).isEqualTo(rRate, Assertions.offset(0.001))
        assertThat(res.steps.first { it.first == sNode }.third).isEqualTo(rRate * sRate * rsSel * 1.0/2.0, Assertions.offset(0.001))
        assertThat(res.steps.first { it.first == tNode }.third).isEqualTo(rRate * sRate * rsSel * tRate * stSel * 1.0/3.0, Assertions.offset(0.001))
    }

    @Test
    fun `relationSizeForProbeOrder works correctly for S,R,T`() {
        val probeOrderForR = ProbeOrder(listOf(
                Pair(sNode, setOf()),
                Pair(rNode, setOf(rsPred)),
                Pair(tNode, setOf(stPred))
        ))
        val res = relationSizeForProbeOrder(dc, probeOrderForR)

        assertThat(res.steps.map { Pair(it.first, it.second) }).containsExactlyInAnyOrder(
                Pair(sNode, setOf()),
                Pair(rNode, setOf(rsPred)),
                Pair(tNode, setOf(stPred))
        )
        assertThat(res.steps.first { it.first == sNode }.third).isEqualTo(sRate, Assertions.offset(0.001))
        assertThat(res.steps.first { it.first == rNode }.third).isEqualTo(rRate * sRate * rsSel * 1.0/2.0, Assertions.offset(0.001))
        assertThat(res.steps.first { it.first == tNode }.third).isEqualTo(rRate * sRate * rsSel * tRate * stSel * 1.0/3.0, Assertions.offset(0.001))
    }
}