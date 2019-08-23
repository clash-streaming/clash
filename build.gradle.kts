import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.3.41"
}

repositories {
    mavenCentral()
}

group = "de.unikl.dbis"
version = "0.3.0"

subprojects {
    val projectName = this.name

    apply(plugin="kotlin")

    repositories {
        mavenCentral()
    }

    dependencies {
        implementation(kotlin("stdlib-jdk8"))

        if(projectName != "common") {
            implementation(project(":common"))
        }

        compile("org.apache.logging.log4j:log4j-slf4j-impl:2.11.1")

        testCompile ("org.junit.jupiter:junit-jupiter-api:5.3.2")
        testCompile ("io.mockk:mockk:1.9")
        testCompile ("org.assertj:assertj-core:3.11.1")

        testImplementation ( "org.junit.jupiter:junit-jupiter-api:5.3.2")
        testRuntimeOnly ( "org.junit.jupiter:junit-jupiter-engine:5.3.2")
    }

    tasks.withType<KotlinCompile> {
        kotlinOptions.jvmTarget = "1.8"
    }
}
