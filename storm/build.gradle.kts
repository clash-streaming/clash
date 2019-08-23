// See ../build.gradle.kts for shared configuration

dependencies {
    implementation(project(":documents"))
    implementation(project(":kafka"))
    implementation(project(":physical_graph"))
    implementation(project(":postgres"))
    implementation(project(":query"))
    implementation(project(":workers"))

    implementation("org.json:json:20180813")
    implementation("org.apache.storm:storm-client:2.0.0")
    implementation("org.apache.storm:storm-server:2.0.0")
    implementation("org.apache.kafka:kafka-clients:2.1.0")
    implementation("org.influxdb:influxdb-java:2.14")
    implementation("com.github.davidb:metrics-influxdb:0.9.3")
}

val test by tasks.getting(Test::class) {
    useJUnitPlatform()
}
