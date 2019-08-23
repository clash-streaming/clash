// See ../build.gradle.kts for shared configuration

dependencies {
    implementation(project(":query"))

    implementation("com.github.jsqlparser:jsqlparser:1.3")
}

val test by tasks.getting(Test::class) {
    useJUnitPlatform()
}
