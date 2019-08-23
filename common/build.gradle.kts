// See ../build.gradle.kts for shared configuration

dependencies {
    implementation("org.yaml:snakeyaml:1.25")
}

val test by tasks.getting(Test::class) {
    useJUnitPlatform()
}
