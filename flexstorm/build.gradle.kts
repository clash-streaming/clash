// See ../build.gradle.kts for shared configuration

dependencies {
    implementation("org.json:json:20180813")
    implementation("org.apache.storm:storm-client:2.1.0")
    implementation("org.apache.storm:storm-server:2.1.0")
}

val test by tasks.getting(Test::class) {
    useJUnitPlatform()
}
