// See ../build.gradle.kts for shared configuration

dependencies {
    implementation(project(":datacharacteristics"))
    implementation(project(":documents"))
    implementation(project(":inspection"))
    implementation(project(":optimizer"))
    implementation(project(":physical_graph"))
    implementation(project(":postgres"))
    implementation(project(":query"))
    implementation(project(":storm"))
    implementation(project(":tpch"))

    implementation("org.json:json:20180813")
    implementation("com.beust:klaxon:5.0.12")

    implementation("com.github.ajalt:clikt:1.7.0")
    implementation("org.apache.storm:storm-client:2.0.0")
    implementation("org.apache.storm:storm-server:2.0.0")

    compile("org.apache.logging.log4j:log4j-core:2.11.1")
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

    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
}
