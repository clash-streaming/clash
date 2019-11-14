// See ../build.gradle.kts for shared configuration

dependencies {
}

val test by tasks.getting(Test::class) {
    useJUnitPlatform()
}
