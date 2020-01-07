package de.unikl.dbis.clash.datacharacteristics

import de.unikl.dbis.clash.query.RelationAlias
import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.json.JSONObject
import org.junit.jupiter.api.Test

internal class SymmetricJSONCharacteristicsTest {
    @Test
    fun `parse json object`() {
        val jsonStr = """
           {
             "rates": {
                "x": 1400,
                "y": 402.8,
                "z": 5991
             },
             "selectivities": {
               "x": {
                 "y": 0.004,
                 "z": 0.2
               },
               "y": {
                 "z": 0.001
               }
             }
           }
        """
        val jsonObj = JSONObject(jsonStr)
        val c = SymmetricJSONCharacteristics(jsonObj)

        assertThat(c.rates).containsExactly(
                Assertions.entry(RelationAlias("x"), 1400.0),
                Assertions.entry(RelationAlias("y"), 402.8),
                Assertions.entry(RelationAlias("z"), 5991.0)
        )
        assertThat(c.selectivities).containsExactly(
                Assertions.entry(Pair(RelationAlias("x"), RelationAlias("y")), 0.004),
                Assertions.entry(Pair(RelationAlias("x"), RelationAlias("z")), 0.2),
                Assertions.entry(Pair(RelationAlias("y"), RelationAlias("z")), 0.001)
        )
    }
}
