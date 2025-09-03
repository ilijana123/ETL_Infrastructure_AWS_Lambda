dependencies {
    implementation("software.amazon.awssdk:s3:2.20.0")
    implementation("software.amazon.awssdk:sqs:2.20.0")
    implementation("org.postgresql:postgresql:42.6.0")
    implementation("com.zaxxer:HikariCP:5.0.1")
    implementation("org.apache.commons:commons-lang3:3.12.0")
}

tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
    manifest {
        attributes["Main-Class"] = "com.example.resultscompiler.ResultsCompilerLambda"
    }
}