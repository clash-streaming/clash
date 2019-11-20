// See ../build.gradle.kts for shared configuration

dependencies {
//    implementation(project(":datacharacteristics"))
}

val test by tasks.getting(Test::class) {
    useJUnitPlatform()
}
