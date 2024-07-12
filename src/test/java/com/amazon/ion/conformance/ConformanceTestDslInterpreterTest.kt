// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.conformance

import com.amazon.ion.system.*
import java.io.File
import kotlin.streams.toList
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.DynamicContainer
import org.junit.jupiter.api.DynamicNode
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

/** Some minimal tests for the DSL interpreter. */
object ConformanceTestDslInterpreterTest {

    private val CONFIG = Config(
        debugEnabled = true,
        failUnimplemented = false,
        readerBuilders = mapOf("only reader" to IonReaderBuilder.standard()),
    )

    @JvmStatic
    fun data(): Iterable<Pair<String, Int>> = listOf(
        """
        (document "a test using 'produces'"
                  (produces))
        """ to 1,
        """
        (ion_1_0 "a test using 'text'"
                 (text ''' {a:1, b:2} "two" ''')
                 (produces {b:2, a:1} "two"))
        """ to 1,
        """
        (ion_1_0 "a test using 'signals'"
                 (text ''' {a:1, b:2 "two" ''')
                 (signals "struct missing closing delimiter"))
        """ to 1,
        """
        (ion_1_1 "a test that uses binary"
                  (bytes "6F 6E 60")
                  (produces false true 0))
        """ to 1,
        """
        (ion_1_0 "a test that uses denotes"
                  (text "${'$'}4")
                  (denotes (Symbol "name")))
        """ to 1,
        """
        (ion_1_0 "a test using 'then'"
                 (text ''' 1 ''')
                 (then (text "2")
                       (produces 1 2)))
        """ to 1,
        """
        (ion_1_0 "a test using 'then' to create more than one test case"
                 (text ''' 1 ''')
                 (then "then 2"
                       (text "2")
                       (produces 1 2))
                 (then "then 3"
                       (text "3")
                       (produces 1 3)))
        """ to 2,
        """
        (ion_1_0 "a test using 'each' to create more than one test case"
                 (text " 1 ")
                 (each "unclosed container"
                       (text " { ")
                       (text " [ ")
                       (text " ( ")
                       "invalid timestamp"
                       (text "2022-99-99T")
                       (signals "something bad")))
        """ to 4,
        """
        (ion_1_x "a test using 'ion_1_x' to create more than one test case"
                 (text " 1 ")
                 (produces 1))
        """ to 2,
        // TODO: Tests to check the demangling behavior, use different types of fragments
    )

    @MethodSource("data")
    @ParameterizedTest
    fun interpreterTests(testInput: Pair<String, Int>) {
        val (dsl, expectedNumberOfTestCases) = testInput

        val testBuilder = CONFIG.newCaseBuilder(File("fake-file"))
        val testCases = testBuilder.readAllTests(ION.newReader(dsl)).flatten()

        // It should have the correct number of test cases
        assertEquals(expectedNumberOfTestCases, testCases.size)
        // All the test case executables should run without throwing any exceptions (i.e. pass)
        testCases.forEach { it.executable.execute() }
    }

    private fun DynamicNode.flatten(): List<DynamicTest> {
        return when (this@flatten) {
            is DynamicContainer -> children.toList().flatMap { it.flatten() }
            is DynamicTest -> listOf(this)
            else -> TODO("Unreachable")
        }
    }
}
