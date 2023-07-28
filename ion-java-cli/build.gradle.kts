plugins {
    java
    application
}

description = "A CLI that implements the standard interface defined by ion-test-driver."
version = 1.0
java.sourceCompatibility = JavaVersion.VERSION_1_8
java.targetCompatibility = JavaVersion.VERSION_1_8

repositories {
    mavenCentral()
}

dependencies {
    implementation("args4j:args4j:2.33")
    implementation(rootProject)
}

application {
    mainClass.set("com.amazon.tools.cli.IonJavaCli")
}
