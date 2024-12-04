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
import java.lang.AssertionError
import kotlin.streams.toList
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
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
    val events = mutableListOf<String>()
    fun recordEvent(eventType: String = type.toString(), value: Any? = "") {
        events.add("[$eventType] $value")
    }
    recordEvent("START")

    while (true) {
        next()
        val currentType = type
        if (currentType == null) {
            if (depth > 0) {
                stepOut()
                recordEvent("STEP-OUT")
                continue
            } else {
                recordEvent("END")
                return events
            }
        }

        if (isInStruct) recordEvent("FIELD-NAME", fieldNameSymbol)
        typeAnnotationSymbols.forEach { recordEvent("ANNOTATION", it) }

        if (isNullValue) {
            recordEvent("NULL", currentType)
        } else when (currentType) {
            // TODO: See if we can exercise multiple APIs here.
            //       Since `walk()` is used for looking for errors, we might need to create
            //       multiple versions of walk that use different subsets of the APIs so we
            //       can ensure that all of them result in the expected error.
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
        is SeqElement -> when (modelContent.head) {
            "Null" -> denotesNull(modelContent, reader)
            "Bool" -> denotesBool(modelContent, reader)
            "Int" -> denotesInt(modelContent, reader)
            "Float" -> denotesFloat(modelContent, reader)
            "Decimal" -> denotesDecimal(modelContent, reader)
            "Timestamp" -> denotesTimestamp(modelContent, reader)
            "Symbol" -> denotesSymtok(modelContent.tail.single(), reader.symbolValue())
            "String" -> denotesCodepoints(modelContent, reader.stringValue())
            "Blob" -> denotesLob(IonType.BLOB, modelContent, reader)
            "Clob" -> denotesLob(IonType.CLOB, modelContent, reader)
            "List" -> denotesSeq(IonType.LIST, modelContent, reader)
            "Sexp" -> denotesSeq(IonType.SEXP, modelContent, reader)
            "Struct" -> denotesStruct(modelContent, reader)
            else -> reportSyntaxError(modelContent, "model-content")
        }
        else -> reportSyntaxError(modelContent, "model-content")
    }
}

private fun TestCaseSupport.denotesNull(expectation: SeqElement, reader: IonReader) {
    val expectedType = expectation.tail.singleOrNull()?.textValue?.uppercase()?.let(IonType::valueOf) ?: IonType.NULL
    val actualType = reader.type
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
            assertEquals(expectedValue.longValue, reader.longValue(), createFailureMessage(expectation))
        }
        IntElementSize.BIG_INTEGER -> {
            assertEquals(IntegerSize.BIG_INTEGER, reader.integerSize, createFailureMessage(expectation))
        }
    }
    assertEquals(expectedValue.bigIntegerValue, reader.bigIntegerValue(), createFailureMessage(expectation))
}

private fun TestCaseSupport.denotesFloat(expectation: SeqElement, reader: IonReader) {
    assertFalse(reader.isNullValue, createFailureMessage(expectation))
    assertEquals(IonType.FLOAT, reader.type, createFailureMessage(expectation))

    val actualValue = reader.doubleValue()

    when (val floatValueAsString = expectation.tail.single().asString().textValue) {
        "nan" -> assertTrue(actualValue.isNaN(), "expected 'nan'; was $actualValue")
        "+inf" -> assertEquals(Double.POSITIVE_INFINITY, actualValue)
        "-inf" -> assertEquals(Double.NEGATIVE_INFINITY, actualValue)
        else -> {
            val expected = floatValueAsString.toDouble()
            assertEquals(expected, actualValue, createFailureMessage(expectation))
        }
    }
}

private fun TestCaseSupport.denotesDecimal(expectation: SeqElement, reader: IonReader) {
    assertFalse(reader.isNullValue, createFailureMessage(expectation))
    assertEquals(IonType.DECIMAL, reader.type, createFailureMessage(expectation))
    val actualValue = reader.decimalValue()

    val exponent = expectation.values[2].bigIntegerValue
    assertEquals(exponent, actualValue.scale() * -1, createFailureMessage(expectation, "exponent not equal"))
    when (val coefficient = expectation.values[1]) {
        is IntElement -> assertEquals(
            coefficient.bigIntegerValue,
            actualValue.bigDecimalValue().unscaledValue(),
            createFailureMessage(expectation, "coefficient not equal")
        )
        is TextElement -> {
            if (coefficient.textValue != "negative_0") reportSyntaxError(coefficient, "model-decimal")
            assertTrue(actualValue.isNegativeZero, createFailureMessage(expectation, "coefficient expected to be negative 0"))
        }
    }
}

private fun TestCaseSupport.denotesTimestamp(expectation: SeqElement, reader: IonReader) {
    assertFalse(reader.isNullValue, createFailureMessage(expectation))
    assertEquals(IonType.TIMESTAMP, reader.type, createFailureMessage(expectation))
    val actualValue = reader.timestampValue()

    val modelTimestamp = expectation.tail
    val precision = modelTimestamp.first().textValue

    assertEquals(modelTimestamp[1].longValue, actualValue.year, createFailureMessage(expectation, "unexpected year"))
    if (precision == "year") {
        assertEquals(Timestamp.Precision.YEAR, actualValue.precision)
        return
    }

    assertEquals(modelTimestamp[2].longValue, actualValue.month, createFailureMessage(expectation, "unexpected month"))
    if (precision == "month") {
        assertEquals(Timestamp.Precision.MONTH, actualValue.precision)
        return
    }

    assertEquals(modelTimestamp[3].longValue, actualValue.day, createFailureMessage(expectation, "unexpected day"))
    if (precision == "day") {
        assertEquals(Timestamp.Precision.DAY, actualValue.precision)
        return
    }

    val expectedOffsetMinutes = modelTimestamp[4].seqValues[1].longValueOrNull
    assertEquals(expectedOffsetMinutes, actualValue.localOffset, createFailureMessage(expectation, "unexpected offset"))
    assertEquals(modelTimestamp[5].longValue, actualValue.hour, createFailureMessage(expectation, "unexpected hour"))
    assertEquals(modelTimestamp[6].longValue, actualValue.minute, createFailureMessage(expectation, "unexpected minute"))
    if (precision == "minute") {
        assertEquals(Timestamp.Precision.MINUTE, actualValue.precision, createFailureMessage(expectation))
        return
    }

    val expectedSecond = modelTimestamp[7].longValue
    assertEquals(expectedSecond, actualValue.second, createFailureMessage(expectation, "unexpected second"))
    if (precision == "second") {
        assertEquals(Timestamp.Precision.SECOND, actualValue.precision)
        return
    }

    // Timestamps cannot have -0 as the fractional second coefficient.
    val subsecondCoefficient = modelTimestamp[8].longValue
    val subsecondScale = modelTimestamp[9].longValue.toInt() * -1

    if (precision == "fraction") {
        val expectedDecimalSecond = Decimal.valueOf(subsecondCoefficient, subsecondScale).add(Decimal.valueOf(expectedSecond))
        assertEquals(expectedDecimalSecond, actualValue.decimalSecond, createFailureMessage(expectation, "unexpected seconds fraction"))
        return
    }

    reportSyntaxError(expectation, "model-timestamp with unknown precision: $precision")
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
    assertNull(reader.next(), createFailureMessage(expectation, "unexpected extra element(s) at end of sequence"))
    reader.stepOut()
}

private fun TestCaseSupport.denotesStruct(expectation: SeqElement, reader: IonReader) {
    assertFalse(reader.isNullValue, createFailureMessage(expectation))
    assertEquals(IonType.STRUCT, reader.type, createFailureMessage(expectation))
    reader.stepIn()

    val expectedFields = expectation.tail
    val hasSeenField = BooleanArray(expectedFields.size)

    // FIXME: For structs with repeated field names, this will break because we can't rewind and replay from the
    //        reader, so we can't test the same nested stream multiple times from the reader. This issue is not
    //        caused by using exceptions for control flow.
    while (reader.next() != null) {
        // This is a low-effort solution. If the performance of these tests becomes a problem, rewrite to not
        // use exceptions for control flow.

        // Find all field names that match
        val matchingFieldNameIndices = expectedFields.mapIndexedNotNull { i, modelField ->
            modelField as SeqElement
            val modelFieldName = modelField.values[0]
            try {
                denotesSymtok(modelFieldName, reader.fieldNameSymbol)
                i
            } catch (e: AssertionError) {
                null
            }
        }

        // Now check the field value, if needed.
        when (matchingFieldNameIndices.size) {
            0 -> fail(expectation, "Found unexpected field name: ${reader.fieldNameSymbol}")
            1 -> {
                val modelFieldIndex = matchingFieldNameIndices.single()
                if (hasSeenField[modelFieldIndex]) {
                    fail(expectedFields[modelFieldIndex], "Found multiple matching fields")
                }
                val modelFieldValue = expectedFields[modelFieldIndex].seqValues[1]
                denotesModelValue(modelFieldValue, reader)
                hasSeenField[modelFieldIndex] = true
            }
            else -> TODO("Test runner implementation does not support repeated field names yet.")
        }
    }

    val firstUnseenField = hasSeenField.indexOfFirst { !it }
    if (firstUnseenField != -1) {
        fail(expectation, "Missing at least one expected field, including ${expectedFields[firstUnseenField]}")
    }
    reader.stepOut()
}

private fun TestCaseSupport.denotesSymtok(expectation: AnyElement, actual: SymbolToken) {
    when (expectation) {
        is TextElement -> assertEquals(expectation.textValue, actual.text, createFailureMessage(expectation))
        is IntElement -> assertEquals(expectation.longValue.toInt(), actual.sid, createFailureMessage(expectation))
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
        else -> reportSyntaxError(expectation, "model-symtok")
    }
}

private fun TestCaseSupport.denotesCodepoints(expectation: SeqElement, actual: String) {
    val expectedCodePoints: List<Int> = expectation.tail.map { it.longValue.toInt() }
    val actualCodePoints: List<Int> = actual.codePoints().toList()
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
