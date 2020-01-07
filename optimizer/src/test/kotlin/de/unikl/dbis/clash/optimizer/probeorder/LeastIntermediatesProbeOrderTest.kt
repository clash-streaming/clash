package de.unikl.dbis.clash.optimizer.probeorder

import de.unikl.dbis.clash.datacharacteristics.TpchOnlyJoinsCharacteristics
import de.unikl.dbis.clash.optimizer.materializationtree.MatSource
import de.unikl.dbis.clash.query.RelationAlias
import de.unikl.dbis.clash.query.TpcHConstants
import de.unikl.dbis.clash.query.TpcHOnlyJoins
import de.unikl.dbis.clash.query.relationOf
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class LeastIntermediatesProbeOrderTest {

    @Test
    fun `Q2 regression`() {
        val dc = TpchOnlyJoinsCharacteristics.q2()

        val partNode = MatSource(relationOf(TpcHConstants.part), 1, listOf(), dc.getRate(RelationAlias(TpcHConstants.part)))
        val partSuppNode = MatSource(relationOf(TpcHConstants.partsupp), 1, listOf(), dc.getRate(RelationAlias(TpcHConstants.partsupp)))
        val suppNode = MatSource(relationOf(TpcHConstants.supplier), 1, listOf(), dc.getRate(RelationAlias(TpcHConstants.supplier)))
        val nationNode = MatSource(relationOf(TpcHConstants.nation), 1, listOf(), dc.getRate(RelationAlias(TpcHConstants.nation)))
        val regionNode = MatSource(relationOf(TpcHConstants.region), 1, listOf(), dc.getRate(RelationAlias(TpcHConstants.region)))

        val predicates = TpcHOnlyJoins.q2().result.binaryPredicates
        val nodes = listOf(partNode, partSuppNode, suppNode, nationNode, regionNode)

        val result = LeastIntermediatesProbeOrder().optimize(dc, predicates, nodes)

        assertThat(result.first.inner.size).isEqualTo(5)

        val partOrder = result.first.inner.getValue(partNode)
        val partSuppOrder = result.first.inner.getValue(partSuppNode)
        val suppOrder = result.first.inner.getValue(suppNode)
        val nationOrder = result.first.inner.getValue(nationNode)
        val regionOrder = result.first.inner.getValue(regionNode)

        assertThat(partOrder.first.steps.map { it.first }).isEqualTo(
                listOf(partNode, partSuppNode, suppNode, nationNode, regionNode)
        )

        assertThat(suppOrder.first.steps.map { it.first }).isEqualTo(
                listOf(suppNode, nationNode, regionNode, partSuppNode, partNode)
        )
    }
}
