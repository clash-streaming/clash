package de.unikl.dbis.clash.optimizer

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test

internal class RegistryTest {

    @Test
    fun `DEFAULT of GlobalStrategyRegistry is contained in SUPPORTED`() {
        Assertions.assertThat(GlobalStrategyRegistry.SUPPORTED)
                .contains(GlobalStrategyRegistry.DEFAULT)
    }

    @Test
    fun `DEFAULT of ProbeOrderStrategyRegistry is contained in SUPPORTED`() {
        Assertions.assertThat(ProbeOrderStrategyRegistry.SUPPORTED)
                .contains(ProbeOrderStrategyRegistry.DEFAULT)
    }
}
