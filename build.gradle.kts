import java.net.URI
import java.time.Instant
import java.util.Properties


plugins {
    java
    `maven-publish`
    jacoco
    signing
    id("org.cyclonedx.bom") version "1.7.2"
    id("com.github.spotbugs") version "5.0.13"
    // TODO: more static analysis. E.g.:
    // id("com.diffplug.spotless") version "6.11.0"
}

jacoco {
    toolVersion = "0.8.10+"
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
    testCompileOnly("junit:junit:4.13")
    testRuntimeOnly("org.junit.vintage:junit-vintage-engine")
    testImplementation("org.hamcrest:hamcrest:2.2")
    testImplementation("pl.pragmatists:JUnitParams:1.1.1")
    testImplementation("com.google.code.tempus-fugit:tempus-fugit:1.1")
}

group = "com.amazon.ion"
// The version is stored in a separate file so that we can easily create CI workflows that run when the version changes
// and so that any tool can access the version without having to do any special parsing.
version = File(project.rootDir.path + "/project.version").readLines().single()
description = "A Java implementation of the Amazon Ion data notation."

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
    withType<JavaCompile> {
        options.encoding = "UTF-8"
        // Because we set the `release` option, you can no longer build ion-java using JDK 8. However, we continue to
        // emit JDK 8 compatible classes due to widespread use of this library with JDK 8.
        options.release.set(8)
    }

    javadoc {
        // Suppressing Javadoc warnings is clunky, but there doesn't seem to be any nicer way to do it.
        // https://stackoverflow.com/questions/62894307/option-xdoclintnone-does-not-work-in-gradle-to-ignore-javadoc-warnings
        options {
            this as StandardJavadocDocletOptions
            addBooleanOption("Xdoclint:none", true)
            addStringOption("Xmaxwarns", "1") // best we can do is limit warnings to 1
            // Stops jquery from being included as a bundled dependency
            this.noIndex(true)
        }
    }

    // spotbugs-gradle-plugin creates a :spotbugsTest task by default, but we don't want it
    // see: https://github.com/spotbugs/spotbugs-gradle-plugin/issues/391
    project.gradle.startParameter.excludedTaskNames.add(":spotbugsTest")

    spotbugsMain {
        val spotbugsBaselineFile = "$rootDir/config/spotbugs/baseline.xml"

        // CI=true means we're in a CI workflow
        // https://docs.github.com/en/actions/learn-github-actions/variables#default-environment-variables
        val ciWorkflow = System.getenv()["CI"].toBoolean()
        val baselining = project.hasProperty("baseline") // e.g. `./gradlew :spotbugsMain -Pbaseline`

        if (!baselining) {
            baselineFile.set(file(spotbugsBaselineFile))
        }

        // The plugin only prints to console when no reports are configured
        // See: https://github.com/spotbugs/spotbugs-gradle-plugin/issues/172
        if (!ciWorkflow && !baselining) {
            reports.create("html").required.set(true)
        } else if (baselining) {
            // Note this path succeeds :spotbugsMain because *of course it does*
            ignoreFailures = true
            reports.create("xml") {
                // Why bother? Otherwise we have kilobytes of workspace-specific absolute paths, statistics, etc.
                // cluttering up the baseline XML and preserved in perpetuity. It would be far better if we could use
                // the SpotBugs relative path support, but the way SpotBugs reporters are presently architected they
                // drop the `destination` information long before Project.writeXML uses its presence/absence to
                // determine whether to generate relative instead of absolute paths. So, contribute patch to SpotBugs or
                // write own SpotBugs reporter in parallel or... do this.
                // Improvements are definitely possible, but left as an exercise to the reader.
                doLast {
                    // It would be super neat if we had some way to handle this xml processing directly inline, without
                    // generating a temp file or at least without shelling out.
                    // Use ant's xslt capabilities? Xalan? Saxon gradle plugin (eerohele)? javax.xml.transform?
                    // In the mean time... xsltproc!
                    exec {
                        commandLine(
                            "xsltproc",
                            "--output", spotbugsBaselineFile,
                            "$rootDir/config/spotbugs/baseline.xslt",
                            outputLocation.get().toString()
                        )
                    }
                }
            }
        }
    }

    create<Jar>("sourcesJar") sourcesJar@{
        archiveClassifier.set("sources")
        from(sourceSets["main"].java.srcDirs)
        artifacts { sourcesArtifact = archives(this@sourcesJar) }
    }

    create<Jar>("javadocJar") javadocJar@{
        archiveClassifier.set("javadoc")
        from(javadoc)
        artifacts { javadocArtifact = archives(this@javadocJar) }
    }

    /**
     * This task creates a properties file that will be included in the compiled jar. It contains information about
     * the build/version of the library used in [com.amazon.ion.util.JarInfo]. See https://github.com/amazon-ion/ion-java/pull/433
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
            xml.required.set(true)
            html.required.set(true)
        }
        doLast {
            logger.quiet("Coverage report written to file://${reports.html.outputLocation.get()}/index.html")
        }
    }

    test {
        maxHeapSize = "1g" // When this line was added Xmx 512m was the default, and we saw OOMs
        maxParallelForks = Math.max(1, Runtime.getRuntime().availableProcessors() / 2)
        useJUnitPlatform()
        // report is always generated after tests run
        finalizedBy(jacocoTestReport)
    }

    withType<Sign> {
        setOnlyIf { isReleaseVersion && gradle.taskGraph.hasTask(":publish") }
    }

    cyclonedxBom {
        dependsOn(jar)
        includeConfigs.set(listOf("runtimeClasspath"))
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
            url.set("https://github.com/amazon-ion/ion-java/")

            licenses {
                license {
                    name.set("The Apache License, Version 2.0")
                    url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
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
                connection.set("scm:git:git@github.com:amazon-ion/ion-java.git")
                developerConnection.set("scm:git:git@github.com:amazon-ion/ion-java.git")
                url.set("git@github.com:amazon-ion/ion-java.git")
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
