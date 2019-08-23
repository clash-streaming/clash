package de.unikl.dbis.clash.optimizer.materializationtree

import de.unikl.dbis.clash.datacharacteristics.ManualCharacteristics
import de.unikl.dbis.clash.optimizer.OptimizationParameters
import de.unikl.dbis.clash.optimizer.materializationtree.strategies.FlatTheta
import de.unikl.dbis.clash.query.RelationAlias
import de.unikl.dbis.clash.query.parser.parseQuery
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test


internal class TreeStrategyTest {
//    @Test TODO
//    fun `query that needs too many tasks is rejected`() {
//        val query = parseQuery("SELECT * FROM r, s, t")
//        val dataCharacteristics = ManualCharacteristics()
//        dataCharacteristics.setRate(RelationAlias("r"), 100.0)
//        dataCharacteristics.setRate(RelationAlias("s"), 100.0)
//        dataCharacteristics.setRate(RelationAlias("t"), 100.0)
//        val treeStrategy = LeftDeepGreedy()
//
//        val params = OptimizationParameters(taskCapacity = 10000, availableTasks = 2, crossProductsAllowed = true)
//        Assertions.assertThatThrownBy { treeStrategy.optimize(query, dataCharacteristics, params) }.isInstanceOf(OptimizationError::class.java)
//    }

    @Test
    fun `query that has enough resources is accepted`() {
        val query = parseQuery("SELECT * FROM r, s, t")
        val dataCharacteristics = ManualCharacteristics()
        dataCharacteristics.setRate(RelationAlias("r"), 100.0)
        dataCharacteristics.setRate(RelationAlias("s"), 100.0)
        dataCharacteristics.setRate(RelationAlias("t"), 100.0)
        val treeStrategy = FlatTheta()

        val params1 = OptimizationParameters(taskCapacity = 100, availableTasks = 3, crossProductsAllowed = true)
//        assertThat { treeStrategy.optimize(query, dataCharacteristics, params1) }.doesNotThrowAnyException()
    }
}

