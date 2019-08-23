// See ../build.gradle.kts for shared configuration

dependencies {
    implementation("org.apache.kafka:kafka-clients:2.1.0")
}

val test by tasks.getting(Test::class) {
    useJUnitPlatform()
}
