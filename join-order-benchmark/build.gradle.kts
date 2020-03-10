// See ../build.gradle.kts for shared configuration

dependencies {
    implementation(project(":query"))
}

val test by tasks.getting(Test::class) {
    useJUnitPlatform()
}
