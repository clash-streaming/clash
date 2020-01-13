// See ../build.gradle.kts for shared configuration

val ktorVersion = "1.2.6"

dependencies {
    implementation("org.json:json:20180813")
    implementation("org.apache.storm:storm-client:2.1.0")
    implementation("org.apache.storm:storm-server:2.1.0")

    implementation("io.ktor:ktor-client-json:$ktorVersion")
    implementation("io.ktor:ktor-client-gson:$ktorVersion")
    implementation("io.ktor:ktor-client-cio:$ktorVersion")
}

val test by tasks.getting(Test::class) {
    useJUnitPlatform()
}
