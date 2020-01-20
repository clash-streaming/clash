// See ../build.gradle.kts for shared configuration

dependencies {
    api("com.google.code.gson:gson:2.8.5")
}

val test by tasks.getting(Test::class) {
    useJUnitPlatform()
}
