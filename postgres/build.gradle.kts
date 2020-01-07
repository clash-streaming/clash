// See ../build.gradle.kts for shared configuration

val test by tasks.getting(Test::class) {
    useJUnitPlatform()
}
