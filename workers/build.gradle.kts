// See ../build.gradle.kts for shared configuration

dependencies {
    implementation(project(":documents"))
    implementation(project(":physical_graph"))
    implementation(project(":query"))
    implementation(project(":similarityjoin"))
}

val test by tasks.getting(Test::class) {
    useJUnitPlatform()
}
