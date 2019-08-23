package de.unikl.dbis.clash

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test


class ClashConfigTest {

    @Test
    fun `reporter name`() {
        val config = ClashConfig()

        config[ClashConfig.CLASH_REPORTER_NAME] = "myReporter"
        assertThat(config.reporterName).isEqualTo("myReporter")

        config[ClashConfig.CLASH_REPORTER_NAME] = "myReporter"
        assertThat(config.reporterName).isEqualTo("myReporter")
    }

    @Test
    fun `reporter parallelism`() {
        val config = ClashConfig()

        config[ClashConfig.CLASH_REPORTER_PARALLELISM] = 2
        assertThat(config.reporterParallelism).isEqualTo(2)

        config[ClashConfig.CLASH_REPORTER_PARALLELISM] = 5
        assertThat(config.reporterParallelism).isEqualTo(5)
    }

    @Test
    fun `controller name`() {
        val config = ClashConfig()

        config[ClashConfig.CLASH_CONTROLLER_NAME] = "myController"
        assertThat(config.controllerName).isEqualTo("myController")

        config[ClashConfig.CLASH_CONTROLLER_NAME] = "upperManagement"
        assertThat(config.controllerName).isEqualTo("upperManagement")
    }

    @Test
    fun `controller enabled`() {
        val config = ClashConfig()

        config[ClashConfig.CLASH_CONTROLLER_ENABLED] = true
        assertThat(config.controllerEnabled).isTrue()

        config[ClashConfig.CLASH_CONTROLLER_ENABLED] = false
        assertThat(config.controllerEnabled).isFalse()
    }

    @Test
    fun `dispatcher name`() {
        val config = ClashConfig()

        config[ClashConfig.CLASH_DISPATCHER_NAME] = "myDispatcher"
        assertThat(config.dispatcherName).isEqualTo("myDispatcher")

        config[ClashConfig.CLASH_DISPATCHER_NAME] = "theDispatcher"
        assertThat(config.dispatcherName).isEqualTo("theDispatcher")
    }

    @Test
    fun `dispatcher parallelism`() {
        val config = ClashConfig()

        config[ClashConfig.CLASH_DISPATCHER_PARALLELISM] = 2
        assertThat(config.dispatcherParallelism).isEqualTo(2)

        config[ClashConfig.CLASH_DISPATCHER_PARALLELISM] = 5
        assertThat(config.dispatcherParallelism).isEqualTo(5)
    }

    @Test
    fun `no interference`() {
        val config = ClashConfig()
        config[ClashConfig.CLASH_REPORTER_PARALLELISM] = 1
        config[ClashConfig.CLASH_REPORTER_NAME] = "reporter a"
        config[ClashConfig.CLASH_CONTROLLER_ENABLED] = false
        config[ClashConfig.CLASH_CONTROLLER_NAME] = "ctrl b"
        config[ClashConfig.CLASH_DISPATCHER_PARALLELISM] = 3
        config[ClashConfig.CLASH_DISPATCHER_NAME] = "dispatch c"

        assertThat(config.reporterParallelism).isEqualTo(1)
        assertThat(config.reporterName).isEqualTo("reporter a")
        assertThat(config.controllerEnabled).isFalse()
        assertThat(config.controllerName).isEqualTo("ctrl b")
        assertThat(config.dispatcherParallelism).isEqualTo(3)
        assertThat(config.dispatcherName).isEqualTo("dispatch c")
    }
}
