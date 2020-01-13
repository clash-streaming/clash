// See ../build.gradle.kts for shared configuration

val ktorVersion = "1.2.6"

dependencies {
    implementation("io.ktor:ktor-server-core:$ktorVersion")
    implementation("io.ktor:ktor-server-netty:$ktorVersion")
}

val test by tasks.getting(Test::class) {
    useJUnitPlatform()
}
