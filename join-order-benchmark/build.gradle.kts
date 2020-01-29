// See ../build.gradle.kts for shared configuration

dependencies {
    implementation(project(":query"))
    implementation(project(":query_parser"))
}

val test by tasks.getting(Test::class) {
    useJUnitPlatform()
}
