import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.3.41"
    id("org.jlleitschuh.gradle.ktlint") version "9.1.1"
}

repositories {
    mavenCentral()
    maven("https://plugins.gradle.org/m2/")
}

allprojects {
    group = "de.unikl.dbis"
    version = "0.3.0"
}

subprojects {
    val projectName = this.name

    apply(plugin = "kotlin")

    repositories {
        jcenter()
        mavenCentral()
    }

    dependencies {
        implementation(kotlin("stdlib-jdk8"))

        if (projectName != "common") {
            implementation(project(":common"))
        }

        compile("org.apache.logging.log4j:log4j-slf4j-impl:2.12.1")

        testCompile("org.junit.jupiter:junit-jupiter-api:5.3.2")
        testCompile("io.mockk:mockk:1.9")
        testCompile("org.assertj:assertj-core:3.11.1")

        testImplementation("org.junit.jupiter:junit-jupiter-api:5.3.2")
        testImplementation("org.junit.jupiter:junit-jupiter-params:5.3.2")
        testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.3.2")
    }

    tasks.withType<KotlinCompile> {
        kotlinOptions.jvmTarget = "1.8"
    }

    apply(plugin = "org.jlleitschuh.gradle.ktlint") // Version should be inherited from parent

    // Optionally configure plugin
    ktlint {
        debug.set(true)
    }

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
