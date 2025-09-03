dependencies {
    implementation("software.amazon.awssdk:s3:2.20.0")
    implementation("software.amazon.awssdk:sqs:2.20.0")
    implementation("org.apache.commons:commons-math3:3.6.1")
}

tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
    manifest {
        attributes["Main-Class"] = "com.example.datasplitter.DataSplitterLambda"
    }
}