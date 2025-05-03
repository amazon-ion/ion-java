plugins {
    java
    application
    // Apply GraalVM Native Image plugin
    id("org.graalvm.buildtools.native") version "0.10.3"
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

    implementation("info.picocli:picocli:4.7.6")
    annotationProcessor("info.picocli:picocli-codegen:4.7.6")
}

tasks.withType<JavaCompile> {
    options.compilerArgs.add("-Aproject=${project.group}/${project.name}")
}

application {
    mainClass.set("com.amazon.tools.cli.IonJavaCli")
}

// Defines an ion-java-cli:nativeCompile task which produces ion-java-cli/build/native/nativeCompile/jion
// You need to have GRAALVM_HOME pointed at a GraalVM installation
// You can get one of those via e.g. `sdk install java 17.0.9-graalce`
// See: https://sdkman.io/
graalvmNative {
    testSupport.set(false)
    binaries {
        named("main") {
            imageName.set("jion")
            mainClass.set("com.amazon.tools.cli.SimpleIonCli")
            buildArgs.add("-O4")
        }
    }
    binaries.all {
        buildArgs.add("--verbose")
    }
}
