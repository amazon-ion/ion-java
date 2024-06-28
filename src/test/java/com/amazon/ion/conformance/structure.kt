// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.conformance

import com.amazon.ion.*
import com.amazon.ionelement.api.AnyElement
import com.amazon.ionelement.api.ElementType
import com.amazon.ionelement.api.SeqElement
import com.amazon.ionelement.api.StringElement
import com.amazon.ionelement.api.loadAllElements
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.DynamicContainer
import org.junit.jupiter.api.DynamicNode
import org.junit.jupiter.api.DynamicTest

// There are three distinct parts to this DSL
//  1. The structure clauses (document, then, each, etc.)
//  2. The input (fragment) clauses
//  3. The expectation clauses
//
// The structure is eagerly evaluated. The other clauses are lazily evaluated in the actual test cases.

/**
 * Tuple of a [ConformanceTestBuilder], the current s-expression, and the current position in the s-expression.
 *
 * This is immutable. Branches in the `read` functions require creating a new updated copy of [ParserState].
 */
private data class ParserState(val builder: ConformanceTestBuilder, val sexp: SeqElement, val pos: Int = 0)

private fun ParserState.updateState(pos: Int = this.pos, builderUpdate: ConformanceTestBuilder.() -> ConformanceTestBuilder = { this }): ParserState =
    copy(pos = pos, builder = builderUpdate(builder))

/**
 * Entry point to reading the test structure.
 */
fun ConformanceTestBuilder.readAllTests(reader: IonReader): DynamicNode {
    return loadAllElements(reader, ELEMENT_LOADER_OPTIONS)
        .mapIndexed { i, it ->
            try {
                readTest(it)
            } catch (e: ConformanceTestInvalidSyntaxException) {
                // If there's a syntax error in this test tree, we'll create a test case to represent it
                // and rethrow the error in there. This will allow other tests to run even if some malformed
                // tests exist.
                DynamicTest.dynamicTest("$file[$i]") { throw e }
            } catch (e: NotImplementedError) {
                // Hack to report something useful if we can't read the test case because we
                // haven't implemented something yet. This creates a test case that always skips.
                DynamicTest.dynamicTest("$file[$i] - ${e.message}") { assumeTrue(false) }
            }
        }
        .let { DynamicContainer.dynamicContainer(file.path, it) }
}

/** Reads a top-level test clause. */
fun ConformanceTestBuilder.readTest(element: AnyElement): DynamicNode {
    val sexp = element as? SeqElement ?: reportSyntaxError(element, "test-case")
    val parserState = ParserState(this, sexp, 1)

    return when (sexp.head) {
        "document" ->
            parserState.readDescription()
                .readFragments { updateState { plusFragments(it) } }
                .readContinuation()

        "ion_1_0" ->
            parserState.updateState { plusFragment(ivm(sexp, 1, 0)) }
                .readDescription()
                .readFragments { updateState { plusFragments(it) } }
                .readContinuation()

        "ion_1_1" ->
            parserState.updateState { plusFragment(ivm(sexp, 1, 1)) }
                .readDescription()
                .readFragments { updateState { plusFragments(it) } }
                .readContinuation()

        "ion_1_x" -> {
            parserState.readDescription()
                .let { p ->
                    val ion10Branch = p.updateState { plus("In Ion 1.0", ivm(sexp, 1, 0)) }
                    val ion11Branch = p.updateState { plus("In Ion 1.1", ivm(sexp, 1, 1)) }
                    p.builder.buildContainer(
                        ion10Branch
                            .readFragments { updateState { plusFragments(it) } }
                            .readContinuation(),
                        ion11Branch
                            .readFragments { updateState { plusFragments(it) } }
                            .readContinuation(),
                    )
                }
        }
        else -> reportSyntaxError(sexp)
    }
}

/**
 * Reads 0 or more fragments from an s-expression starting from the position
 * given in [ParserState]. Returns a [ParserState] with an updated position
 * and a list of any fragment expressions that were found.
 */
private fun <T> ParserState.readFragments(useFragments: ParserState.(List<SeqElement>) -> T): T {
    val fragments = sexp.tailFrom(pos)
        .takeWhile { it is SeqElement && it.head in FRAGMENT_KEYWORDS } as List<SeqElement>
    return this.updateState(pos = pos + fragments.size).useFragments(fragments)
}

/**
 * Reads an optional description, returning an updated [ParserState].
 * This function always adds _some_ description to the [ParserState].
 * If the clause contains no description, it uses the clause keyword as a description.
 */
private fun ParserState.readDescription(): ParserState {
    return sexp.values[pos].let {
        // If it's a string (even null), update position
        val newPos = pos + if (it.type == ElementType.STRING) 1 else 0
        // If there is no description, or the description is null, use the clause name instead.
        val text = (it as? StringElement)?.textValue ?: "«${sexp.head}»"
        updateState(newPos) { plusName(text) }
    }
}

/** Reads a `then` clause, starting _after_ the `then` keyword. */
private fun ParserState.readThen(): List<DynamicNode> {
    return readDescription()
        .readFragments { frags -> updateState { plusFragments(frags) } }
        .readContinuation()
        .let(::listOf)
}

/** Reads an `each` clause, starting _after_ the `each` keyword. */
private fun ParserState.readEach(): List<DynamicNode> {
    // TODO: Handle case where 0 fragments
    return readDescription()
        .readFragments {
            it.mapIndexed { i, frag ->
                updateState { plus(name = "[$i]", frag) }.readContinuation()
            }
        }
}

/** Reads an extension, returning a list of test case nodes constructed from those extensions. */
private fun ParserState.readExtension(): List<DynamicNode> {
    return when (sexp.head) {
        "each" -> updateState(pos = 1).readEach()
        "then" -> updateState(pos = 1).readThen()
        else -> builder.reportSyntaxError(sexp, "unknown extension")
    }
}

/** Reads a continuation—a single expectation or one-to-many extensions. */
private fun ParserState.readContinuation(): DynamicNode {
    val continuation = sexp.tailFrom(pos)

    val firstExpression = continuation.first()
    firstExpression as? SeqElement ?: builder.reportSyntaxError(firstExpression, "continuation")

    return continuation.flatMap {
        it as? SeqElement ?: builder.reportSyntaxError(it, "extension")
        with(ParserState(builder, it)) {
            readExpectation()?.let { expectation -> return expectation }
            readExtension()
        }
    }.let(builder::buildContainer)
}

/**
 * Reads an optional expectation clause. If the current clause is not an expectation,
 * returns null.
 */
private fun ParserState.readExpectation(): DynamicNode? {
    return when (sexp.head) {
        "and" -> TODO("'and' not implemented")
        "not" -> TODO("'not' not implemented")
        "produces" -> builder.build {
            val actual = loadAllElements(createFragmentReader()).toList()
            assertEquals(sexp.tail, actual, createFailureMessage(sexp))
        }
        "signals" -> builder.build { assertSignals(sexp, createFragmentReader()) }
        "denotes" -> builder.build { assertDenotes(sexp.tail, createFragmentReader()) }
        else -> null
    }
}
