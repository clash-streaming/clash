// See ../build.gradle.kts for shared configuration

dependencies {
    implementation(project(":datacharacteristics"))
    implementation(project(":optimizer"))
    implementation(project(":query"))
    implementation(project(":physical_graph"))

    implementation("org.json:json:20180813")
}

val test by tasks.getting(Test::class) {
    useJUnitPlatform()
}
