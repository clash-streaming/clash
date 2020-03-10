// See ../build.gradle.kts for shared configuration

dependencies {
    implementation(project(":datacharacteristics"))
    implementation(project(":physical_graph"))
    implementation(project(":query"))

    testImplementation(project(":query"))
    testImplementation(project(":tpch"))
}

val test by tasks.getting(Test::class) {
    useJUnitPlatform()
}
