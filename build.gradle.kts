import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.3.41"
    id("org.jlleitschuh.gradle.ktlint") version "9.2.1"
    id("io.gitlab.arturbosch.detekt") version "1.6.0"
}

repositories {
    mavenCentral()
    maven("https://plugins.gradle.org/m2/")

    jcenter {
        content {
            // just allow to include kotlinx projects
            // detekt needs 'kotlinx-html' for the html report
            includeGroup("org.jetbrains.kotlinx")
        }
    }
}

allprojects {
    group = "de.unikl.dbis"
    version = "0.3.0"
}

subprojects {
    val projectName = this.name

    apply(plugin = "kotlin")
    apply(plugin = "io.gitlab.arturbosch.detekt")

    repositories {
        jcenter()
        mavenCentral()
    }

    dependencies {
        implementation(kotlin("stdlib-jdk8"))

        if (projectName != "common") {
            implementation(project(":common"))
        }

        if (projectName != "manager") {
            compile("org.apache.logging.log4j:log4j-slf4j-impl:2.12.1")
        }
        compile("com.fasterxml.jackson.core:jackson-databind:2.10.2")
        compile("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.10.2")

        testCompile("org.junit.jupiter:junit-jupiter-api:5.3.2")
        testCompile("io.mockk:mockk:1.9")
        testCompile("org.assertj:assertj-core:3.11.1")

        testImplementation("org.junit.jupiter:junit-jupiter-api:5.3.2")
        testImplementation("org.junit.jupiter:junit-jupiter-params:5.3.2")
        testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.3.2")
    }

    val projectJvmTarget = "1.8"

    tasks.withType<KotlinCompile> {
        kotlinOptions.jvmTarget = projectJvmTarget
    }

    tasks.withType<io.gitlab.arturbosch.detekt.Detekt>().configureEach {
        exclude("resources/")
        exclude("build/")
        jvmTarget = projectJvmTarget
    }

    apply(plugin = "org.jlleitschuh.gradle.ktlint") // Version should be inherited from parent

//    synchronizeSharedResources()

//    sourceSets {
//        main {
//            resources.srcDir(project(":common").sourceSets["main"].resources.srcDirs)
// //            resources.srcDirs("src/main/resources","../shared-resources")
// //            resources {
// //                srcDirs("src/main/resources","../shared-resources")
// //            }
//        }
//    }
}
//
// fun Project.synchronizeSharedResources() {
//    sourceSets {
//        main {
//            resources.srcDir(project(":common").sourceSets["main"].resources.srcDirs)
//        }
//        test {
//            resources.srcDir(project(":common").sourceSets["test"].resources.srcDirs)
//        }
//    }
// }
