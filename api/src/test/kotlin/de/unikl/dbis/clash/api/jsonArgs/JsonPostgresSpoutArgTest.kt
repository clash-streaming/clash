package de.unikl.dbis.clash.api.jsonArgs

import com.beust.klaxon.Klaxon
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class JsonPostgresSpoutArgTest {

    @Test
    fun `parses json correctly`() {
        val query = "select * from foobar where x = y"
        val databaseName = "dbName"
        val databaseUser = "dbUser"
        val databasePassword = "dbPwd"
        val millisDelay = 5

        val result = Klaxon().parse<JsonSourceArg>("""
            {
                "type": "postgres",
                "query": "$query",
                "databaseName": "$databaseName",
                "databaseUser": "$databaseUser",
                "databasePassword": "$databasePassword",
                "millisDelay": $millisDelay
            }
        """.trimIndent())

        assertThat(result).isNotNull
        assertThat(result).isInstanceOf(JsonPostgresSpoutArg::class.java)

        val pgResult = result as JsonPostgresSpoutArg
        assertThat(pgResult.databaseName).isEqualTo(databaseName)
        assertThat(pgResult.databaseUser).isEqualTo(databaseUser)
        assertThat(pgResult.databasePassword).isEqualTo(databasePassword)
    }
}
