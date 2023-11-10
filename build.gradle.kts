import com.github.jk1.license.filter.LicenseBundleNormalizer
import com.github.jk1.license.render.InventoryMarkdownReportRenderer
import com.github.jk1.license.render.TextReportRenderer
import org.jetbrains.kotlin.gradle.dsl.KotlinCompile
import org.jetbrains.kotlin.gradle.dsl.KotlinJvmOptions
import proguard.gradle.ProGuardTask
import java.net.URI
import java.time.Instant
import java.util.Properties

buildscript {
    repositories {
        mavenCentral()
    }
    dependencies {
        classpath("com.guardsquare:proguard-gradle:7.1.0")
    }
}

plugins {
    kotlin("jvm") version "1.9.0"
    java
    `maven-publish`
    jacoco
    signing
    id("com.github.johnrengelman.shadow") version "8.1.1"

    id("org.cyclonedx.bom") version "1.7.2"
    id("com.github.spotbugs") version "5.0.13"
    id("org.jlleitschuh.gradle.ktlint") version "11.3.2"
    // TODO: more static analysis. E.g.:
    // id("com.diffplug.spotless") version "6.11.0"

    // Used for generating the third party attribution document
    id("com.github.jk1.dependency-license-report") version "2.5"
}

jacoco {
    toolVersion = "0.8.10+"
}

repositories {
    mavenCentral()
    google()
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-stdlib:1.9.0")

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
val generatedResourcesDir = "$buildDir/generated/main/resources"
lateinit var sourcesArtifact: PublishArtifact
lateinit var javadocArtifact: PublishArtifact

sourceSets {
    main {
        java.srcDir("src")
        resources.srcDir(generatedResourcesDir)
    }
    test {
        java.srcDir("test")
    }
}

licenseReport {
    // Because of the current gradle project structure, we must explicitly exclude ion-java-cli, even
    // though ion-java does not depend on ion-java-cli. By default, the license report generator includes
    // the current project (ion-java) and all its subprojects.
    projects = arrayOf(project)
    outputDir = "$buildDir/reports/licenses"
    renderers = arrayOf(InventoryMarkdownReportRenderer(), TextReportRenderer())
    // Dependencies use inconsistent titles for Apache-2.0, so we need to specify mappings
    filters = arrayOf(
        LicenseBundleNormalizer(
            mapOf(
                "The Apache License, Version 2.0" to "Apache-2.0",
                "The Apache Software License, Version 2.0" to "Apache-2.0",
            )
        )
    )
}

tasks {
    withType<JavaCompile> {
        options.encoding = "UTF-8"
        // Because we set the `release` option, you can no longer build ion-java using JDK 8. However, we continue to
        // emit JDK 8 compatible classes due to widespread use of this library with JDK 8.
        options.release.set(8)
    }
    withType<KotlinCompile<KotlinJvmOptions>> {
        kotlinOptions {
            // Kotlin jvmTarget must match the JavaCompile release version
            jvmTarget = "1.8"
        }
    }

    jar {
        archiveClassifier.set("original")
    }

    // Creates a super jar of ion-java and its dependencies where all dependencies are shaded (moved)
    // to com.amazon.ion_.shaded.are_you_sure_you_want_to_use_this
    shadowJar {
        val newLocation = "com.amazon.ion.shaded_.are_you_sure_you_want_to_use_this"
        archiveClassifier.set("shaded")
        dependsOn(generateLicenseReport)
        from(generateLicenseReport.get().outputFolder)
        relocate("kotlin", "$newLocation.kotlin")
        relocate("org.jetbrains", "$newLocation.org.jetbrains")
        relocate("org.intellij", "$newLocation.org.intellij")
        dependencies {
            // Remove all Kotlin metadata so that it looks like an ordinary Java Jar
            exclude("**/*.kotlin_metadata")
            exclude("**/*.kotlin_module")
            exclude("**/*.kotlin_builtins")
            // Eliminate dependencies' pom files
            exclude("**/pom.*")
        }
    }

    /**
     * The `minifyJar` task uses Proguard to create a JAR that is smaller than the combined size of ion-java
     * and its dependencies. This is the final JAR that is published to maven central.
     */
    val minifyJar by register<ProGuardTask>("minifyJar") {
        group = "build"
        val rulesPath = file("config/proguard/rules.pro")
        val inputJarPath = shadowJar.get().outputs.files.singleFile
        val outputJarPath = "build/libs/ion-java-$version.jar"

        inputs.file(rulesPath)
        inputs.file(inputJarPath)
        outputs.file(outputJarPath)
        dependsOn(shadowJar)
        dependsOn(configurations.runtimeClasspath)

        injars(inputJarPath)
        outjars(outputJarPath)
        configuration(rulesPath)

        val javaHome = System.getProperty("java.home")
        // Automatically handle the Java version of this build, but we don't support lower than JDK 11
        // See https://github.com/Guardsquare/proguard/blob/e76e47953f6f295350a3bb7eeb801b33aac34eae/examples/gradle-kotlin-dsl/build.gradle.kts#L48-L60
        libraryjars(
            mapOf("jarfilter" to "!**.jar", "filter" to "!module-info.class"),
            "$javaHome/jmods/java.base.jmod"
        )
    }

    build {
        dependsOn(minifyJar)
    }

    generateLicenseReport {
        doLast {
            // We don't want the time in the generated markdown report because that makes it unstable for
            // our verification of the THIRD_PARTY_LICENSES file.
            val markdownReport = outputs.files.single().walk().single { it.path.endsWith(".md") }
            val dateRegex = Regex("^_\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2} \\w+_$")
            // Reads the content of the markdown report, replacing the date line with an empty line, and
            // trimming extra whitespace from the end of all other lines.
            val newMarkdownContent = markdownReport.readLines()
                .joinToString("\n") { if (it.matches(dateRegex)) "" else it.trimEnd() }
            markdownReport.writeText(newMarkdownContent)
        }
    }

    // Task to check whether the THIRD_PARTY_LICENSES file is still up-to-date.
    val checkThirdPartyLicensesFile by register("checkThirdPartyLicensesFile") {
        val thirdPartyLicensesFileName = "THIRD_PARTY_LICENSES.md"
        val thirdPartyLicensesPath = "$rootDir/$thirdPartyLicensesFileName"
        dependsOn(generateLicenseReport)
        inputs.file(thirdPartyLicensesPath)
        group = "verification"
        description = "Verifies that $thirdPartyLicensesFileName is up-to-date."
        doLast {
            val generatedMarkdownReport = generateLicenseReport.get().outputs.files.single()
                .walk().single { it.path.endsWith(".md") }
            val generatedMarkdownReportContent = generatedMarkdownReport.readLines()
                .filter { it.isNotBlank() }
                .joinToString("\n")

            val sourceControlledMarkdownReport = File(thirdPartyLicensesPath)
            val sourceControlledMarkdownReportContent = sourceControlledMarkdownReport.readLines()
                .filter { it.isNotBlank() }
                .joinToString("\n")

            if (sourceControlledMarkdownReportContent != generatedMarkdownReportContent) {
                throw IllegalStateException(
                    "$thirdPartyLicensesPath is out of date.\n" +
                        "Please replace the file content with the content of $generatedMarkdownReport."
                )
            }
        }
    }

    check {
        dependsOn(checkThirdPartyLicensesFile)
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

    ktlint {
        version.set("0.40.0")
        outputToConsole.set(true)
    }

    // spotbugs-gradle-plugin creates a :spotbugsTest task by default, but we don't want it
    // see: https://github.com/spotbugs/spotbugs-gradle-plugin/issues/391
    project.gradle.startParameter.excludedTaskNames.add(":spotbugsTest")

    spotbugsMain {
        val spotbugsBaselineFile = "$rootDir/config/spotbugs/baseline.xml"

        val baselining = project.hasProperty("baseline") // e.g. `./gradlew :spotbugsMain -Pbaseline`

        if (!baselining) {
            baselineFile.set(file(spotbugsBaselineFile))
        }

        // The plugin only prints to console when no reports are configured
        // See: https://github.com/spotbugs/spotbugs-gradle-plugin/issues/172
        if (!baselining) {
            reports.create("html").required.set(true)
        } else {
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
                            "${outputLocation.get()}/spotBugs"
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
        val propertiesFile = File("$generatedResourcesDir/${project.name}.properties")
        doLast {
            propertiesFile.parentFile.mkdirs()
            val properties = Properties()
            properties.setProperty("build.version", version.toString())
            properties.setProperty("build.time", Instant.now().toString())
            properties.store(propertiesFile.writer(), null)
        }
        outputs.file(propertiesFile)
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

    fun Test.applyCommonTestConfig() {
        group = "verification"
        maxHeapSize = "1g" // When this line was added Xmx 512m was the default, and we saw OOMs
        maxParallelForks = Math.max(1, Runtime.getRuntime().availableProcessors() / 2)
        useJUnitPlatform()
        // report is always generated after tests run
        finalizedBy(jacocoTestReport)
    }

    test {
        applyCommonTestConfig()
    }

    /** Runs the JUnit test on the minified jar. */
    register<Test>("minifyTest") {
        applyCommonTestConfig()
        classpath = project.configurations.testRuntimeClasspath.get() + project.sourceSets.test.get().output + minifyJar.outputs.files
        dependsOn(minifyJar)
    }

    /** Runs the JUnit test on the shadow jar. */
    register<Test>("shadowTest") {
        applyCommonTestConfig()
        classpath = project.configurations.testRuntimeClasspath.get() + project.sourceSets.test.get().output + shadowJar.get().outputs.files
        dependsOn(minifyJar)
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
