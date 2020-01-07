package de.unikl.dbis.clash.support

import java.io.Serializable
import java.util.Properties
import java.util.concurrent.ExecutionException
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.ListTopicsOptions
import org.apache.kafka.clients.admin.NewTopic

data class KafkaConfig(
    val kafkaBootstrapServers: String
) : Serializable

// TODO this does not work before execution of main program
// TODO it seems like handling futures more then only at listTopics command
fun checkKafka(config: KafkaConfig): Boolean {
    val adminClient = AdminClient.create(kafkaProducerProperties(config)) ?: return false
    try {
        val listTopics = adminClient.listTopics(ListTopicsOptions().timeoutMs(5000))
    } catch (e: ExecutionException) {
        println("Could not connect to Kafka")
        return false
    }
    return true
}

fun kafkaConsumerProperties(config: KafkaConfig): Properties {
    val props = Properties()
    props["bootstrap.servers"] = config.kafkaBootstrapServers
    props["group.id"] = "test"
    props["enable.auto.commit"] = "true"
    props["auto.commit.interval.ms"] = "1000"
    props["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
    props["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"

    return props
}

fun kafkaProducerProperties(config: KafkaConfig): Properties {
    val props = Properties()
    props["bootstrap.servers"] = config.kafkaBootstrapServers
    props["acks"] = "all"
    props["retries"] = 0
    props["batch.size"] = 16384
    props["linger.ms"] = 1
    props["buffer.memory"] = 33554432
    props["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
    props["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
    return props
}

fun createTopic(config: KafkaConfig, topic: String) {
    val adminClient = AdminClient.create(kafkaProducerProperties(config))
    val kTopic = NewTopic(topic, 1, 1)
    adminClient.createTopics(listOf(kTopic))
    adminClient.close()
}
//  TODO
// /**
// * Add Kafka sources and sinks for TPC-H Q2
// */
// fun wireKafkaQ2(clash: Clash, config: ClashConfig) {
//    clash.registerSource(relationOf("part"), KafkaSpout(InputName("p"), RelationAlias("p"),"part", config))
//    clash.registerSource(relationOf("partsupp"), KafkaSpout(InputName("ps"), RelationAlias("ps"), "partsupp", config))
//    clash.registerSource(relationOf("supplier"), KafkaSpout(InputName("s"), RelationAlias("s"), "supplier", config))
//    clash.registerSource(relationOf("nation"), KafkaSpout(InputName("n"), RelationAlias("n"), "nation", config))
//    clash.registerSource(relationOf("region"), KafkaSpout(InputName("r"), RelationAlias("r"), "region", config))
//
//    // TODO
//    //clash.registerSink(relationOf("part", "partsupp", "supplier", "nation", "region"), KafkaSinkBolt("joinResult", "joinResult", config))
// }
//
// /**
// * Add Kafka sources and sinks for TPC-H Q3
// */
// fun wireKafkaQ3(clash: Clash, config: ClashConfig) {
//    clash.registerSource(relationOf("customer"), KafkaSpout(InputName("c"), RelationAlias("c"),"customer", config))
//    clash.registerSource(relationOf("orders"), KafkaSpout(InputName("o"), RelationAlias("o"),"orders", config))
//    clash.registerSource(relationOf("lineitem"), KafkaSpout(InputName("l"), RelationAlias("l"), "lineitem", config))
//
//    // TODO
//    // clash.registerSink(relationOf("customer", "orders", "lineitem"), KafkaSinkBolt("joinResult", "joinResult", config))
// }
//
// /**
// * Add Kafka sources and sinks for TPC-H Q5
// */
// fun wireKafkaQ5(clash: Clash, config: ClashConfig) {
//    clash.registerSource(relationOf("customer"), KafkaSpout(InputName("c"), RelationAlias("c"),"customer", config))
//    clash.registerSource(relationOf("orders"), KafkaSpout(InputName("o"), RelationAlias("o"),"orders", config))
//    clash.registerSource(relationOf("lineitem"), KafkaSpout(InputName("l"), RelationAlias("l"),"lineitem", config))
//    clash.registerSource(relationOf("supplier"), KafkaSpout(InputName("s"), RelationAlias("s"),"supplier", config))
//    clash.registerSource(relationOf("nation"), KafkaSpout(InputName("n"), RelationAlias("n"), "nation", config))
//    clash.registerSource(relationOf("region"), KafkaSpout(InputName("r"), RelationAlias("r"), "region", config))
//
//    clash.registerSink(relationOf("result"), KafkaSinkBolt("joinResult", "joinResult", config))
// }
