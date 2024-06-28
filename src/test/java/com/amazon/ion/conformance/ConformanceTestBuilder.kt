// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.conformance

import com.amazon.ion.*
import com.amazon.ion.system.*
import com.amazon.ionelement.api.IonElement
import com.amazon.ionelement.api.SeqElement
import com.amazon.ionelement.api.location
import java.io.File
import org.junit.jupiter.api.DynamicContainer.dynamicContainer
import org.junit.jupiter.api.DynamicNode
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.opentest4j.AssertionFailedError
import org.opentest4j.TestAbortedException

data class ConformanceTestBuilder(
    val config: Config,
    /** File that the cases are bring created from. */
    val file: File,
    // Internal fields for building up state from which to create a test case
    private val nameParts: List<String> = listOf(),
    private val fragments: List<SeqElement> = listOf(),
) {

    /**
     * Helper class that provides runtime support to the test cases.
     */
    class TestCaseSupport(private val testBuilder: ConformanceTestBuilder, private val readerBuilder: IonReaderBuilder) {

        private val data: ByteArray by lazy { readFragments(testBuilder.fragments) }

        /** Creates a new reader for this test case */
        fun createFragmentReader(): IonReader = readerBuilder.build(data)

        /** Logs a lazily-evaluated message, if debug is enabled. */
        fun debug(message: () -> String) = testBuilder.debug(message)

        /** Throws an exception for a syntax error in the tests */
        fun reportSyntaxError(element: IonElement, details: String? = null): Nothing =
            testBuilder.reportSyntaxError(element, details)

        /** Creates a file URI for the given IonElement */
        fun locationOf(element: IonElement) = "file://${testBuilder.file.absolutePath}:${element.metas.location}"

        /** Creates a failure message that includes a file link to [element] */
        fun createFailureMessage(element: IonElement, details: String? = null): String =
            "${details ?: "Assertion failed"} at ${locationOf(element)}; $element"

        /** Throws an [AssertionFailedError] to fail a test case */
        fun fail(expectation: IonElement, details: String, t: Throwable? = null): Nothing =
            throw AssertionFailedError(createFailureMessage(expectation, details), t)
    }

    // Leaf nodes need a full name or else the HTML report is incomprehensible.
    private val fullName: String
        get() = nameParts.joinToString(" ")

    // TODO: this could be fullName or nameParts.last()
    //       Both have drawbacks, but it only affects the display of the interior nodes of the test tree
    val containerName: String
        get() = fullName // nameParts.last()

    /** Prints a debug message, if debug messages are enabled in the config. */
    fun debug(message: () -> String) {
        if (config.debugEnabled) println("[TEST: $fullName] ${message()}")
    }

    // Copy-on-write setters
    fun plusName(name: String): ConformanceTestBuilder = copy(nameParts = nameParts + name)
    fun plusFragment(fragment: SeqElement): ConformanceTestBuilder = copy(fragments = fragments + fragment)
    fun plusFragments(newFragments: List<SeqElement>): ConformanceTestBuilder = copy(fragments = fragments + newFragments)
    fun plus(name: String, fragment: SeqElement): ConformanceTestBuilder = copy(nameParts = nameParts + name, fragments = fragments + fragment)
    fun plus(name: String, newFragments: List<SeqElement>): ConformanceTestBuilder = copy(nameParts = nameParts + name, fragments = fragments + newFragments)

    fun build(executable: TestCaseSupport.() -> Unit): DynamicNode {
        return config.readerBuilders.map { (readerName, readerBuilder) ->
            val testName = "$fullName using $readerName"
            val testCaseSupport = TestCaseSupport(this, readerBuilder)
            dynamicTest(testName) {
                if (!config.testFilter(file, testName)) throw TestAbortedException(testName)
                debug { "Begin Test using $readerName" }
                try {
                    executable(testCaseSupport)
                } catch (e: NotImplementedError) {
                    if (config.failUnimplemented) throw e
                    debug { "Ignored because ${e.message}" }
                    throw TestAbortedException("$e")
                }
            }
        }.let {
            dynamicContainer(containerName, it)
        }
    }

    /** Builds a [DynamicNode] container with the correct name */
    fun buildContainer(children: Iterable<DynamicNode>): DynamicNode = dynamicContainer(containerName, children)

    /** Builds a [DynamicNode] container with the correct name */
    fun buildContainer(vararg children: DynamicNode): DynamicNode = dynamicContainer(containerName, children.toList())

    /** Signals to the test builder that there is a syntax error */
    fun reportSyntaxError(element: IonElement, details: String? = null): Nothing =
        throw ConformanceTestInvalidSyntaxException(file, element, details)
}
