// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.conformance

import com.amazon.ion.*
import com.amazon.ion.conformance.ConformanceTestBuilder.*
import com.amazon.ionelement.api.AnyElement
import com.amazon.ionelement.api.BoolElement
import com.amazon.ionelement.api.IntElement
import com.amazon.ionelement.api.IntElementSize
import com.amazon.ionelement.api.SeqElement
import com.amazon.ionelement.api.SexpElement
import com.amazon.ionelement.api.StringElement
import com.amazon.ionelement.api.TextElement
import kotlin.streams.toList
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue

/**
 * Asserts that fully traversing the reader will result in an [IonException].
 * It's expected to be for the reason given in [sexp], but we don't have a way
 * to check that right now because the `signals` message is non-normative.
 */
fun TestCaseSupport.assertSignals(sexp: SeqElement, r: IonReader) {
    val signalDescription = sexp.tail.single().textValue
    // The usual `assertThrows` doesn't give us the ability to add our own context to the failure message.
    val events = try {
        // Just walk the reader without materializing so that we can ensure that the error is raised
        // specifically by the reader.
        r.walk()
    } catch (e: IonException) {
        debug { "Expected an IonException because '$signalDescription'; found $e" }
        // Test case passes
        return
    } catch (t: Throwable) {
        fail(sexp, "Expected an IonException because '$signalDescription' but was ${t::class.simpleName}", t)
    }
    fail(
        sexp,
        "Expected an IonException because '$signalDescription'; " +
            "successfully read: ${events.joinToString("\n")}"
    )
}

/**
 * Walks all data available from an IonReader. Records all data as a stream of events so that
 * if an error is _not_ encountered, we have some useful information for debugging the test failure.
 */
private fun IonReader.walk(): List<String> {
    val events = mutableListOf("START")
    fun recordEvent(eventType: String = type.toString(), value: Any? = "") {
        events.add("[$eventType] $value")
    }

    while (true) {
        next()
        val currentType = type
            ?: try {
                stepOut()
                recordEvent("STEP-OUT")
                continue
            } catch (e: IllegalStateException) {
                recordEvent("END")
                return events
            }

        if (isInStruct) recordEvent("FIELD-NAME", fieldNameSymbol)
        typeAnnotationSymbols.forEach { recordEvent("ANNOTATION", it) }

        if (isNullValue) {
            recordEvent("NULL", currentType)
        } else when (currentType) {
            IonType.BOOL -> recordEvent(value = booleanValue())
            IonType.INT -> recordEvent(value = bigIntegerValue())
            IonType.FLOAT -> recordEvent(value = doubleValue())
            IonType.DECIMAL -> recordEvent(value = decimalValue())
            IonType.TIMESTAMP -> recordEvent(value = timestampValue())
            IonType.SYMBOL -> recordEvent(value = symbolValue())
            IonType.STRING -> recordEvent(value = stringValue())
            IonType.CLOB,
            IonType.BLOB -> recordEvent(value = newBytes())
            IonType.LIST,
            IonType.SEXP,
            IonType.STRUCT -> {
                recordEvent("STEP-IN", type)
                stepIn()
            }
            IonType.NULL,
            IonType.DATAGRAM -> TODO("Unreachable")
        }
    }
}

/**
 * Entry point into `denotes` evaluation. Asserts that each top-level value on the reader
 * matches its respective model-value, and that there are no extra, unexpected values.
 *
 * See https://github.com/amazon-ion/ion-tests/tree/master/conformance#modeling-outputs
 */
fun TestCaseSupport.assertDenotes(modelValues: List<AnyElement>, reader: IonReader) {
    modelValues.forEach {
        reader.next()
        denotesModelValue(it, reader)
    }
    // Assert no more elements in sequence
    assertNull(reader.next(), "unexpected extra element(s) at end of stream")
}

/**
 * Assert that the data at the reader's current position matches a particular Ion value.
 */
private fun TestCaseSupport.denotesModelValue(expectation: AnyElement, reader: IonReader) {
    if (reader.type == null) fail(expectation, "no more values; expected $expectation")
    if (expectation is SexpElement && expectation.head == "annot") {
        val actualAnnotations = reader.typeAnnotationSymbols
        expectation.tailFrom(2)
            .forEachIndexed { i, it -> denotesSymtok(it, actualAnnotations[i]) }
        denotesModelContent(expectation.tail.first(), reader)
    } else {
        assertEquals(SymbolToken.EMPTY_ARRAY, reader.typeAnnotationSymbols, createFailureMessage(expectation, "expected no annotations"))
        denotesModelContent(expectation, reader)
    }
}

private fun TestCaseSupport.denotesModelContent(modelContent: AnyElement, reader: IonReader) {
    when (modelContent) {
        is IntElement -> denotesInt(modelContent, reader)
        is BoolElement -> denotesBool(modelContent, reader)
        is StringElement -> {
            val failureContext = createFailureMessage(modelContent)
            assertEquals(IonType.STRING, reader.type, failureContext)
            assertEquals(modelContent.stringValue, reader.stringValue(), failureContext)
        }
        is SexpElement -> when (modelContent.head) {
            "Null" -> denotesNull(modelContent, reader)
            "Bool" -> denotesBool(modelContent, reader)
            "Int" -> denotesInt(modelContent, reader)
            "Float" -> TODO("denotes float")
            "Decimal" -> TODO("denotes decimal")
            "Timestamp" -> TODO("denotes timestamp")
            "Symbol" -> denotesSymtok(modelContent.tail.single(), reader.symbolValue())
            "String" -> denotesCodepoints(modelContent, reader.stringValue())
            "Blob" -> denotesLob(IonType.BLOB, modelContent, reader)
            "Clob" -> denotesLob(IonType.CLOB, modelContent, reader)
            "List" -> denotesSeq(IonType.LIST, modelContent, reader)
            "Sexp" -> denotesSeq(IonType.SEXP, modelContent, reader)
            "Struct" -> TODO("denotes struct")
            else -> reportSyntaxError(modelContent, "model-content")
        }
        else -> reportSyntaxError(modelContent, "model-content")
    }
}

private fun TestCaseSupport.denotesNull(expectation: SeqElement, reader: IonReader) {
    val expectedType = expectation.tail.single().textValue.uppercase().let(IonType::valueOf)
    val actualType = reader.next()
    assertTrue(reader.isNullValue, createFailureMessage(expectation))
    assertEquals(expectedType, actualType)
}

private fun TestCaseSupport.denotesBool(modelBoolean: AnyElement, reader: IonReader) {
    val expected = when (modelBoolean) {
        is BoolElement -> modelBoolean.booleanValue
        is SexpElement -> modelBoolean.tail.single().booleanValue
        else -> reportSyntaxError(modelBoolean, "model-boolean")
    }
    assertEquals(IonType.BOOL, reader.type, createFailureMessage(modelBoolean))
    assertEquals(expected, reader.booleanValue(), createFailureMessage(modelBoolean))
}

private fun TestCaseSupport.denotesInt(expectation: AnyElement, reader: IonReader) {
    val expectedValue = when (expectation) {
        is SexpElement -> expectation.tail.single().asInt()
        is IntElement -> expectation
        else -> reportSyntaxError(expectation, "model-integer")
    }
    assertEquals(IonType.INT, reader.type, createFailureMessage(expectation))
    assertFalse(reader.isNullValue, createFailureMessage(expectation))
    when (expectedValue.integerSize) {
        IntElementSize.LONG -> {
            assertNotEquals(IntegerSize.BIG_INTEGER, reader.integerSize, createFailureMessage(expectation))
            assertEquals(expectedValue.longValue, reader.longValue(), createFailureMessage(expectation))
        }
        IntElementSize.BIG_INTEGER -> {
            assertEquals(IntegerSize.BIG_INTEGER, reader.integerSize, createFailureMessage(expectation))
        }
    }
    assertEquals(expectedValue.bigIntegerValue, reader.bigIntegerValue(), createFailureMessage(expectation))
}

private fun TestCaseSupport.denotesSeq(type: IonType, expectation: SeqElement, reader: IonReader) {
    assertFalse(reader.isNullValue, createFailureMessage(expectation))
    assertEquals(type, reader.type, createFailureMessage(expectation))
    reader.stepIn()
    expectation.tail.forEach {
        reader.next()
        denotesModelValue(it, reader)
    }
    // Assert no more elements in sequence
    assertNull(reader.next(), "unexpected extra element(s) at end of $type")
    reader.stepOut()
}

private fun TestCaseSupport.denotesSymtok(expectation: AnyElement, actual: SymbolToken) {
    when (expectation) {
        is TextElement -> assertEquals(expectation.textValue, actual.text, createFailureMessage(expectation))
        is IntElement -> assertEquals(expectation.longValue, actual.sid, createFailureMessage(expectation))
        is SeqElement -> when (expectation.head) {
            "absent" -> {
                if (actual.text != null) fail(expectation, "Expected unknown text; was '${actual.text}'")
                // TODO: Calculate offset, Symtab name?
            }
            "text" ->
                actual.text
                    ?.let { denotesCodepoints(expectation, it) }
                    ?: fail(expectation, "Expected known text; none present in $actual")
            else -> reportSyntaxError(expectation, "model-symtok")
        }
    }
}

private fun TestCaseSupport.denotesCodepoints(expectation: SeqElement, actual: String) {
    val expectedCodePoints = expectation.tail.map { it.longValue }
    val actualCodePoints = actual.codePoints().toList()
    assertEquals(expectedCodePoints, actualCodePoints, createFailureMessage(expectation))
}

private fun TestCaseSupport.denotesLob(type: IonType, expectation: SeqElement, reader: IonReader) {
    val expectedBytes = readBytes(expectation)
    assertEquals(type, reader.type, createFailureMessage(expectation))
    assertEquals(expectedBytes.size, reader.byteSize(), createFailureMessage(expectation))

    // bufferSize is intentionally small but >1 so that we can test reading chunks of a lob.
    val bufferSize = 3
    val buffer = ByteArray(bufferSize)
    expectedBytes.toList().chunked(bufferSize).forEachIndexed { i, chunk ->
        val bytesRead = reader.getBytes(buffer, i * 3, 3)
        if (bytesRead == bufferSize) {
            assertArrayEquals(chunk.toByteArray(), buffer, createFailureMessage(expectation))
        } else {
            chunk.forEachIndexed { j, byte -> assertEquals(byte, buffer[j], createFailureMessage(expectation)) }
        }
    }
    assertArrayEquals(expectedBytes, reader.newBytes(), createFailureMessage(expectation))
}
