
// Automatically resolve and download any missing JDK versions
plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version("0.8.0")
}

rootProject.name = "ion-java"
include("ion-java-cli")
