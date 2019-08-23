// See ../build.gradle.kts for shared configuration

dependencies {
    implementation("org.influxdb:influxdb-java:2.14")
}

val test by tasks.getting(Test::class) {
    useJUnitPlatform()
}
