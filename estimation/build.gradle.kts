// See ../build.gradle.kts for shared configuration

dependencies {
    implementation(project(":query"))
    implementation(project(":join-order-benchmark"))
    implementation(project(":datacharacteristics"))
}

val test by tasks.getting(Test::class) {
    useJUnitPlatform()
}
