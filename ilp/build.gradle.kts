// See ../build.gradle.kts for shared configuration

dependencies {
//    compile(project(":query"))
    implementation("org.ojalgo:ojalgo:47.3.1")
}

val test by tasks.getting(Test::class) {
    useJUnitPlatform()
}
