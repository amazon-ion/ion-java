import java.net.URI
import java.time.Instant
import java.util.Properties

plugins {
    java
    `maven-publish`
    jacoco
    signing
    // TODO: static analysis. E.g.:
    // id("com.diffplug.spotless") version "6.11.0"
    // id("com.github.spotbugs") version "4.8.0"
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("junit:junit:4.13.1")
    testImplementation("org.hamcrest:hamcrest:2.2")
    testImplementation("pl.pragmatists:JUnitParams:1.1.1")
}

group = "com.amazon.ion"
// The version is stored in a separate file so that we can easily create CI workflows that run when the version changes
// and so that any tool can access the version without having to do any special parsing.
version = File(project.rootDir.path + "/project.version").readLines().single()
description = "A Java implementation of the Amazon Ion data notation."
// Can't target 6 from 8
// https://stackoverflow.com/questions/48146930/is-it-possible-to-compile-project-on-java-8-for-target-java-6
java.sourceCompatibility = JavaVersion.VERSION_1_6

val isReleaseVersion: Boolean = !version.toString().endsWith("SNAPSHOT")
val generatedJarInfoDir = "${buildDir}/generated/jar-info"
lateinit var sourcesArtifact: PublishArtifact
lateinit var javadocArtifact: PublishArtifact

sourceSets {
    main {
        java.srcDir("src")
        resources.srcDir(generatedJarInfoDir)
    }
    test {
        java.srcDir("test")
    }
}

tasks {
    withType<JavaCompile> { options.encoding = "UTF-8" }

    javadoc {
        // Suppressing Javadoc warnings is clunky, but there doesn't seem to be any nicer way to do it.
        // https://stackoverflow.com/questions/52205209/configure-gradle-build-to-suppress-javadoc-console-warnings
        options {
            (this as CoreJavadocOptions).addStringOption("Xdoclint:none", "-quiet")
        }
    }

    create<Jar>("sourcesJar") sourcesJar@ {
        archiveClassifier.set("sources")
        from(sourceSets["main"].java.srcDirs)
        artifacts { sourcesArtifact = archives(this@sourcesJar) }
    }

    create<Jar>("javadocJar") javadocJar@ {
        archiveClassifier.set("javadoc")
        from(javadoc)
        artifacts { javadocArtifact = archives(this@javadocJar) }
    }

    /**
     * This task creates a properties file that will be included in the compiled jar. It contains information about
     * the build/version of the library used in [com.amazon.ion.util.JarInfo]. See https://github.com/amzn/ion-java/pull/433
     * for why this is done with a properties file rather than the Jar manifest.
     */
    val generateJarInfo by creating<Task> {
        doLast {
        val propertiesFile = File("$generatedJarInfoDir/${project.name}.properties")
            propertiesFile.parentFile.mkdirs()
            val properties = Properties()
            properties.setProperty("build.version", version.toString())
            properties.setProperty("build.time", Instant.now().toString())
            properties.store(propertiesFile.writer(), null)
        }
        outputs.dir(generatedJarInfoDir)
    }

    processResources { dependsOn(generateJarInfo) }

    jacocoTestReport {
        dependsOn(test)
        reports {
            xml.isEnabled = true
            html.isEnabled = true
        }
        doLast {
            logger.quiet("Coverage report written to file://${reports.html.destination}/index.html")
        }
    }

    test {
        // report is always generated after tests run
        finalizedBy(jacocoTestReport)
    }

    withType<Sign> {
        setOnlyIf { isReleaseVersion && gradle.taskGraph.hasTask("publish") }
    }
}

publishing {
    publications.create<MavenPublication>("IonJava") {
        from(components["java"])
        artifact(sourcesArtifact)
        artifact(javadocArtifact)

        pom {
            name.set("Ion Java")
            description.set(project.description)
            url.set("https://github.com/amzn/ion-java/")

            licenses {
                license {
                    name.set("The Apache License, Version 2.0")
                    url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                }
            }
            developers {
                developer {
                    name.set("Amazon Ion Team")
                    email.set("ion-team@amazon.com")
                    organization.set("Amazon Ion")
                    organizationUrl.set("https://github.com/amazon-ion")
                }
            }
            scm {
                connection.set("scm:git:git@github.com:amzn/ion-java.git")
                developerConnection.set("scm:git:git@github.com:amzn/ion-java.git")
                url.set("git@github.com:amzn/ion-java.git")
            }
        }
    }
    repositories.mavenCentral {
        credentials {
            username = properties["ossrhUsername"].toString()
            password = properties["ossrhPassword"].toString()
        }
        url = URI.create("https://aws.oss.sonatype.org/service/local/staging/deploy/maven2")
    }
}

signing {
    // Allow publishing to maven local even if we don't have the signing keys
    // This works because when not required, the signing task will be skipped
    // if signing.keyId, signing.password, signing.secretKeyRingFile, etc are
    // not present in gradle.properties.
    isRequired = isReleaseVersion
    sign(publishing.publications["IonJava"])
}
