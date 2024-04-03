import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    id("java-library")
    alias(libs.plugins.jetbrainsKotlinJvm)
    alias(libs.plugins.serialization)
    id("maven-publish")
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

kotlin {
    compilerOptions {
        jvmTarget = JvmTarget.JVM_1_8
    }
}

dependencies {
    implementation(libs.kotlinx.coroutines.core)
    testImplementation(libs.junit)
    testImplementation(libs.burst.junit4)
    testImplementation(libs.okio)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.truth)
    testImplementation(libs.kotlinx.serialization.json)
    testImplementation(libs.lincheck)
}

tasks.withType<Test> {
    jvmArgs(
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-exports", "java.base/jdk.internal.util=ALL-UNNAMED",
        "--add-exports", "java.base/sun.security.action=ALL-UNNAMED"
    )
}

afterEvaluate {
    publishing {
        publications {
            create<MavenPublication>("tape-kt") {
                from(components["kotlin"])

                groupId = "com.msvhora.github"
                artifactId = "tape-kt"
                version = "1.0.2"
            }
        }
    }
}