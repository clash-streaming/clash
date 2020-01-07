// See ../build.gradle.kts for shared configuration

dependencies {
    implementation(project(":datagenerator"))
    implementation(project(":documents"))
    implementation(project(":physical_graph"))
    implementation(project(":query"))
    implementation(project(":workers"))

    implementation("com.github.ajalt:clikt:1.7.0")
}

val test by tasks.getting(Test::class) {
    useJUnitPlatform()
}

/**
 * gradle jar
 *
 * Create a .jar for standalone work.
 */
tasks.jar {
    manifest {
        attributes["Main-Class"] = "de.unikl.dbis.clash.api.CliKt"
        attributes["Multi-Release"] = "true"
    }

    configurations["compileClasspath"].forEach { file: File ->
        from(zipTree(file.absoluteFile))
    }
}
