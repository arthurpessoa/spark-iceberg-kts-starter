plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.ktlint)
    application
}

group = "com.example"
version = "0.0.1-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(11)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.kotlin.reflect)
    implementation(libs.spark.core)
    implementation(libs.spark.sql)
    implementation(libs.spark.avro)
    implementation(libs.spark.sql.kafka)
    implementation(libs.kafka.clients)
    implementation(libs.iceberg.spark.runtime)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.kotlin.test.junit5)
    testImplementation(libs.embedded.kafka)
    testImplementation(libs.kotlinx.coroutines.core)
    testImplementation(libs.awaitility.kotlin)
}

kotlin {
    compilerOptions {
        freeCompilerArgs.addAll("-Xjsr305=strict")
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}

application {
    mainClass.set("com.example.demo.MainKt")
}
