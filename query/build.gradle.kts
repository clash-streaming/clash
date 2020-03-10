// See ../build.gradle.kts for shared configuration

dependencies {
    api("com.github.jsqlparser:jsqlparser:1.3")
}

val test by tasks.getting(Test::class) {
    useJUnitPlatform()
}
