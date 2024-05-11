import com.diffplug.gradle.spotless.SpotlessTask
import com.github.jk1.license.filter.LicenseBundleNormalizer
import com.github.jk1.license.render.InventoryMarkdownReportRenderer
import com.github.jk1.license.render.TextReportRenderer
import java.time.Instant
import java.util.Properties
import org.gradle.kotlin.dsl.support.unzipTo
import org.gradle.kotlin.dsl.support.zipTo
import org.jetbrains.kotlin.gradle.dsl.KotlinCompile
import org.jetbrains.kotlin.gradle.dsl.KotlinJvmOptions
import proguard.gradle.ProGuardTask

buildscript {
    repositories {
        mavenCentral()
    }
    dependencies {
        classpath("com.guardsquare:proguard-gradle:7.4.0")
    }
}

plugins {
    kotlin("jvm") version "1.9.0"
    java
    `maven-publish`

    // There are newer versions available, but they are not guaranteed to support Java 8.
    id("io.github.gradle-nexus.publish-plugin") version "1.3.0"

    jacoco
    signing
    id("com.github.johnrengelman.shadow") version "8.1.1"

    id("org.cyclonedx.bom") version "1.7.2"
    id("com.github.spotbugs") version "5.0.13"
    id("org.jlleitschuh.gradle.ktlint") version "11.3.2"

    id("com.diffplug.spotless") version "6.11.0"

    // Used for generating the third party attribution document
    id("com.github.jk1.dependency-license-report") version "2.5"

    // Used for generating OSGi bundle information
    // We cannot use the latest version since it targets JDK 17, and we don't want to force building with JDK 17
    // Without `apply false`, the plugin is automatically applied to the main "jar" task, which somehow interferes with
    // the "spotbugsMain" task, causing it to fail. Instead, we will create a separate task to generate the bundle info.
    id("biz.aQute.bnd.builder") version "6.4.0" apply false

    id("me.champeau.jmh") version "0.7.2"
}

jacoco {
    toolVersion = "0.8.10+"
}

repositories {
    mavenCentral()
    google()
}

// This list should be kept up to date to include all LTS versions of Corretto.
// These are the versions that we guarantee are supported by `ion-java`, though it can probably run on other versions too.
val SUPPORTED_JRE_VERSIONS = listOf(8, 11, 17, 21)

java {
    toolchain {
        // Always build with the minimum supported Java version so that builds are reproducible,
        // and so it automatically targets the min supported version.
        languageVersion.set(JavaLanguageVersion.of(SUPPORTED_JRE_VERSIONS.min()))
        vendor.set(JvmVendorSpec.AMAZON)
    }
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-stdlib:1.9.0")
    implementation("org.jetbrains.kotlinx:kotlinx-collections-immutable:0.3.6")

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

val isCI: Boolean = System.getenv("CI") == "true"
val githubRepositoryUrl = "https://github.com/amazon-ion/ion-java/"
val isReleaseVersion: Boolean = !version.toString().endsWith("SNAPSHOT")
// Workflows triggered by a new release always have a tag ref.
val isReleaseWorkflow: Boolean = (System.getenv("GITHUB_REF") ?: "").startsWith("refs/tags/")
val generatedResourcesDir = "${layout.buildDirectory}/generated/main/resources"

sourceSets {
    main {
        resources.srcDir(generatedResourcesDir)
    }
}

licenseReport {
    // Because of the current gradle project structure, we must explicitly exclude ion-java-cli, even
    // though ion-java does not depend on ion-java-cli. By default, the license report generator includes
    // the current project (ion-java) and all its subprojects.
    projects = arrayOf(project)
    outputDir = "${layout.buildDirectory}/reports/licenses"
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

// Spotless eagerly checks for the `rachetFrom` git ref even if there are no spotless tasks in the task
// graph, so we're going to use a git tag to create our own lazy evaluation and setting of `rachetFrom`.
// See https://github.com/diffplug/spotless/issues/1902
val SPOTLESS_TAG = "spotless-check-${Instant.now().epochSecond}-DELETE-ME"

/**
 * This is the commit where the current branch most recently forked from master. We use this as
 * our "rachetFrom" base so that changes in master don't cause unexpected formatting failures in
 * feature branches.
 */
val sourceRepoRachetFromCommit: String by lazy {
    val git = System.getenv("GIT_CLI") ?: "git"

    fun String.isSourceRepo(): Boolean {
        val url = "$git remote get-url ${this@isSourceRepo}".trim().runCommand()
        return "amazon-ion/ion-java" in url || "amzn/ion-java" in url
    }

    var remoteName = "$git remote".runCommand().trim().lines().firstOrNull { it.isSourceRepo() }

    if (isCI) {
        // When running on a CI environment e.g. GitHub Actions, we might need to automatically add the remote
        if (remoteName == null) {
            remoteName = "ci_source_repository"
            "$git remote add $remoteName $githubRepositoryUrl".runCommand(log = logger::quiet)
            logger.quiet("Added remote repository ")
        }
        // ...and make sure that we have indeed fetched that remote
        "$git fetch --unshallow --no-tags --no-recurse-submodules $remoteName master".runCommand()
    }

    remoteName ?: throw Exception(
        """
            |No git remote found for amazon-ion/ion-java. Try again after running:
            |
            |    git remote add -f <name> $githubRepositoryUrl
        """.trimMargin()
    )

    // TODO: We might need to use the PR base ref when this is running as part of a CI check for a PR.
    logger.quiet("Finding spotless ratchetFrom base...")
    "$git merge-base $remoteName/master HEAD".runCommand(log = logger::quiet).trim()
}

fun String.runCommand(workingDir: File = rootProject.projectDir, log: (String) -> Unit = logger::info): String {
    log("$ $this")
    val parts = this.split("\\s".toRegex())
    val proc = ProcessBuilder(*parts.toTypedArray())
        .directory(workingDir)
        .redirectOutput(ProcessBuilder.Redirect.PIPE)
        .redirectError(ProcessBuilder.Redirect.PIPE)
        .start()
    proc.waitFor(30, TimeUnit.SECONDS)
    val stdOut = proc.inputStream.bufferedReader().readText()
    val stdErr = proc.errorStream.bufferedReader().readText()
    if (stdOut.isNotBlank()) log(stdOut)
    if (stdErr.isNotBlank()) logger.warn(stdErr)
    if (proc.exitValue() != 0) {
        throw Exception("Failed to run command: $this")
    }
    return stdOut
}

spotless {
    // If this is an automated release workflow, don't configure any style checks.
    // This is important because we're ratcheting from `master` and when we create
    // a release that is not directly from `master`, the spotless checks can cause
    // the release workflow to fail if `master` has any commits that are not in the
    // release branch.
    if (isReleaseWorkflow) return@spotless

    "git tag -f $SPOTLESS_TAG".runCommand()
    ratchetFrom(SPOTLESS_TAG)
    // Make sure this always gets cleaned up. We can't do it inline here, so we'll do it once the task graph is created.
    gradle.taskGraph.addTaskExecutionGraphListener { "git tag -d $SPOTLESS_TAG".runCommand() }

    val shortFormLicenseHeader = """
        // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
        // SPDX-License-Identifier: Apache-2.0
    """.trimIndent()

    java {
        // Note that the order of these is important. Each of these is an individual formatter
        // that is applied sequentially.
        licenseHeader(shortFormLicenseHeader)
        removeUnusedImports()
    }
    kotlin {
        licenseHeader(shortFormLicenseHeader)
    }
}

// Tasks that must be visible outside the tasks block
lateinit var sourcesJar: AbstractArchiveTask
lateinit var javadocJar: AbstractArchiveTask
lateinit var minifyJar: ProGuardTask

tasks {
    withType<JavaCompile> {
        options.encoding = "UTF-8"
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

    val generateManifest = create<aQute.bnd.gradle.Bundle>("generateManifest") {
        // Create the manifest using the same sources as the regular jar.
        from(jar.get().source)
        archiveClassifier.set("manifest")

        manifest {
            attributes(
                "Automatic-Module-Name" to "com.amazon.ion",
                "Main-Class" to "com.amazon.ion.impl._Private_CommandLine",
                "Build-Time" to "${Instant.now()}",
                "Build-Version" to "$version",
            )
        }
        // Sets OSGi bundle attributes
        // See https://en.wikipedia.org/wiki/OSGi#Bundles for a minimal introduction to the bundle manifest
        // See https://enroute.osgi.org/FAQ/520-bnd.html for a high level of what is the "bnd" tool
        // If we ever expose any shaded classes, then the bundle info will need to be added after the shadow step.
        // For now, though, generating the bundle info here results
        bundle {
            bnd(
                "Bundle-License: https://www.apache.org/licenses/LICENSE-2.0.txt",
                // These must be specified manually to keep the values the same as the pre-v1.9.5 values.
                // What will happen if they change? I don't know, but possibly some sort of compatibility problem.
                "Bundle-Name: com.amazon.ion:ion-java",
                "Bundle-SymbolicName: com.amazon.ion.java",
                // Exclusions must come first when specifying exports.
                // We exclude the `apps`, `impl`, and `shaded_` packages and expose everything else.
                "Export-Package: !com.amazon.ion.apps.*,!com.amazon.ion.impl.*,!com.amazon.ion.shaded_.*,com.amazon.ion.*",
                // Limit imports to only this package so that we don't add in any kotlin imports.
                // This line is not necessary if we create bundle info after shading (but that is more complex).
                "Import-Package: com.amazon.ion.*",
                // Removing the 'Private-Package' header because it is optional and was not present prior to v1.9.5
                "-removeheaders: Private-Package",
            )
        }
        // This is not strictly necessary, but it is nice to make the output of this task into a manifest-only jar.
        doLast {
            val temp = temporaryDir
            unzipTo(temp, outputs.files.singleFile)
            delete(fileTree(temp).matching { excludes.add("META-INF/MANIFEST.MF") })
            zipTo(outputs.files.singleFile, temp)
        }
    }

    // Creates a super jar of ion-java and its dependencies where all dependencies are shaded (moved)
    // to com.amazon.ion_.shaded.are_you_sure_you_want_to_use_this
    shadowJar {
        val newLocation = "com.amazon.ion.shaded_.do_not_use"
        archiveClassifier.set("shaded")
        dependsOn(generateManifest, generateLicenseReport)
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
        manifest.inheritFrom(generateManifest.manifest)
    }

    /**
     * The `minifyJar` task uses Proguard to create a JAR that is smaller than the combined size of ion-java
     * and its dependencies. This is the final JAR that is published to maven central.
     */
    minifyJar = create<ProGuardTask>("minifyJar") proguardTask@{
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
        if (JavaVersion.current() == JavaVersion.VERSION_1_8) {
            libraryjars("$javaHome/lib/jce.jar")
            libraryjars("$javaHome/lib/rt.jar")
        } else {
            libraryjars(
                mapOf("jarfilter" to "!**.jar", "filter" to "!module-info.class"),
                "$javaHome/jmods/java.base.jmod"
            )
        }
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
        version.set("0.45.2")
        outputToConsole.set(true)
    }

    // spotbugs-gradle-plugin creates a :spotbugsTest task by default, but we don't want it
    // see: https://github.com/spotbugs/spotbugs-gradle-plugin/issues/391
    project.gradle.startParameter.excludedTaskNames.add(":spotbugsTest")

    spotbugsMain {
        launcher.set(
            project.javaToolchains.launcherFor {
                languageVersion.set(JavaLanguageVersion.of(SUPPORTED_JRE_VERSIONS.min()))
                vendor.set(JvmVendorSpec.AMAZON)
            }
        )

        val spotbugsConfigDir = "$rootDir/config/spotbugs"
        excludeFilter.set(file("$spotbugsConfigDir/exclude.xml"))
        val spotbugsBaselineFile = "$spotbugsConfigDir/baseline.xml"

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
                            "$spotbugsConfigDir/baseline.xslt",
                            "${outputLocation.get()}"
                        )
                    }
                }
            }
        }
    }

    sourcesJar = create<Jar>("sourcesJar") sourcesJar@{
        archiveClassifier.set("sources")
        from(sourceSets["main"].java.srcDirs)
    }

    javadocJar = create<Jar>("javadocJar") javadocJar@{
        archiveClassifier.set("javadoc")
        from(javadoc)
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
    }

    test {
        applyCommonTestConfig()
        // report is always generated after tests run
        finalizedBy(jacocoTestReport)
    }

    /**
     * Runs the JUnit test on the shadow jar.
     * Potentially useful for debugging issues that are not reproducible in the standard `test` task.
     */
    register<Test>("shadowTest") {
        applyCommonTestConfig()
        classpath = project.configurations.testRuntimeClasspath.get() + project.sourceSets.test.get().output + shadowJar.get().outputs.files
        dependsOn(minifyJar)
    }

    val jvmSpecificMinifyTests = SUPPORTED_JRE_VERSIONS.map {
        // Run the JUnit tests on the minified jar using the given java version for setting up the JRE
        register<Test>("minifyTest$it") {
            javaLauncher.set(
                project.javaToolchains.launcherFor {
                    languageVersion.set(JavaLanguageVersion.of(it))
                    vendor.set(JvmVendorSpec.AMAZON)
                }
            )
            applyCommonTestConfig()
            classpath = project.configurations.testRuntimeClasspath.get() + project.sourceSets.test.get().output + minifyJar.outputs.files
            dependsOn(minifyJar)
        }
    }.toTypedArray()

    /**
     * Umbrella task for the JUnit tests on the minified jar for all supported JRE versions.
     *
     * This ensures that the built JAR will run properly on all the supported JREs. It is time-consuming
     * to run all tests for each JRE, so they are not included in the `build` task. However, they are
     * mandatory as a prerequisite for publishing any release.
     *
     * They should all be run in the CI workflow for every PR, but it is best if they run concurrently
     * in separate workflow steps.
     */
    val minifyTest = register<Task>("minifyTest") {
        group = "verification"
        dependsOn(jvmSpecificMinifyTests)
    }

    publish { dependsOn(minifyTest) }

    withType<Sign> {
        setOnlyIf { isReleaseVersion && gradle.taskGraph.hasTask(":publish") }
    }

    cyclonedxBom {
        dependsOn(jar)
        includeConfigs.set(listOf("runtimeClasspath"))
    }

    withType<SpotlessTask> {
        doFirst { "git tag -f $SPOTLESS_TAG $sourceRepoRachetFromCommit".runCommand() }
        doLast { "git tag -d $SPOTLESS_TAG".runCommand() }
    }
}

publishing {
    publications.create<MavenPublication>("IonJava") {
        artifact(sourcesJar)
        artifact(javadocJar)
        artifact(minifyJar.outJarFiles.single()) {
            builtBy(minifyJar)
        }

        pom {
            name.set("Ion Java")
            description.set(project.description)
            url.set(githubRepositoryUrl)

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
}

nexusPublishing {
    // Documentation for this plugin, see https://github.com/gradle-nexus/publish-plugin/blob/v1.3.0/README.md
    this.repositories {
        sonatype {
            nexusUrl.set(uri("https://aws.oss.sonatype.org/service/local/"))
            // For CI environments, the username and password should be stored in
            // ORG_GRADLE_PROJECT_sonatypeUsername and ORG_GRADLE_PROJECT_sonatypePassword respectively.
            if (!isCI) {
                username.set(properties["ossrhUsername"].toString())
                password.set(properties["ossrhPassword"].toString())
            }
        }
    }
}

signing {
    // Allow publishing to maven local even if we don't have the signing keys
    // This works because when not required, the signing task will be skipped
    // if signing.keyId, signing.password, signing.secretKeyRingFile, etc are
    // not present in gradle.properties.
    isRequired = isReleaseVersion

    if (isCI) {
        val signingKeyId: String? by project
        val signingKey: String? by project
        val signingPassword: String? by project
        useInMemoryPgpKeys(signingKeyId, signingKey, signingPassword)
    }

    sign(publishing.publications["IonJava"])
}
