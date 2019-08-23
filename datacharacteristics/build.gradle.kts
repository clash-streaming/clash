// See ../build.gradle.kts for shared configuration

dependencies {
    compile(project(":query"))

    implementation("org.json:json:20180813")
}

val test by tasks.getting(Test::class) {
    useJUnitPlatform()
}
