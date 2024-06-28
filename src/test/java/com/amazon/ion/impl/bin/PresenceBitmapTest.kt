// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.IonException
import com.amazon.ion.TestUtils.*
import com.amazon.ion.impl.macro.Macro.*
import java.io.ByteArrayOutputStream
import org.junit.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

class PresenceBitmapTest {

    companion object {
        val taggedZeroToMany = Parameter("a", ParameterEncoding.Tagged, ParameterCardinality.Any)
        val taggedExactlyOne = Parameter("b", ParameterEncoding.Tagged, ParameterCardinality.One)
        val taggedZeroOrOne = Parameter("c", ParameterEncoding.Tagged, ParameterCardinality.AtMostOne)
        val taggedOneToMany = Parameter("d", ParameterEncoding.Tagged, ParameterCardinality.AtLeastOne)
        val taglessZeroToMany = Parameter("e", ParameterEncoding.uint8, ParameterCardinality.Any)
        val taglessExactlyOne = Parameter("f", ParameterEncoding.uint8, ParameterCardinality.One)
        val taglessZeroOrOne = Parameter("g", ParameterEncoding.uint8, ParameterCardinality.AtMostOne)
        val taglessOneToMany = Parameter("h", ParameterEncoding.uint8, ParameterCardinality.AtLeastOne)
    }

    @Test
    fun `initialize should ensure that values are cleared`() {
        val signature = listOf(
            taggedExactlyOne, taggedZeroToMany, taggedZeroOrOne, taggedOneToMany,
            taglessExactlyOne, taglessZeroToMany, taglessZeroOrOne, taglessOneToMany,
        )
        val pb = PresenceBitmap()
        pb.initialize(signature)
        for (i in 0..7) pb[i] = PresenceBitmap.EXPRESSION
        pb.initialize(signature)
        for (i in 0..7) assertEquals(PresenceBitmap.VOID, pb[i])
        assertThrows<IndexOutOfBoundsException> { pb[8] }
    }

    @Test
    fun `when initializing with a too-large signature, should throw exception`() {
        val pb = PresenceBitmap()
        val signature = List(PresenceBitmap.MAX_SUPPORTED_PARAMETERS + 1) { taggedZeroToMany }
        assertThrows<IonException> { pb.initialize(signature) }
    }

    @Test
    fun `when calling set with an invalid index, should throw exception`() {
        val pb = PresenceBitmap()
        val signature = listOf(taggedZeroOrOne, taggedOneToMany, taggedExactlyOne)
        pb.initialize(signature)
        assertThrows<IndexOutOfBoundsException> { pb.set(-1, PresenceBitmap.EXPRESSION) }
        assertThrows<IndexOutOfBoundsException> { pb.set(3, PresenceBitmap.EXPRESSION) }
    }

    @Test
    fun `when calling get with an invalid index, should throw exception`() {
        val pb = PresenceBitmap()
        val signature = listOf(taggedZeroOrOne, taggedOneToMany, taggedExactlyOne)
        pb.initialize(signature)
        assertThrows<IndexOutOfBoundsException> { pb.get(-1) }
        assertThrows<IndexOutOfBoundsException> { pb.get(3) }
    }

    @Test
    fun `when calling set, the presence bits value for that parameter is _not_ validated`() {
        val signature = listOf(taggedZeroOrOne, taggedOneToMany, taggedExactlyOne)
        with(PresenceBitmap()) {
            initialize(signature)
            // PresenceBits is an internal only class, so we rely on callers to do the correct thing.
            // There should not be an exception thrown for any of these.
            set(0, value = PresenceBitmap.GROUP)
            set(1, value = PresenceBitmap.VOID)
            set(2, value = PresenceBitmap.GROUP)
            set(2, value = PresenceBitmap.VOID)
        }
    }

    @ParameterizedTest
    @CsvSource(
        // For some reason `Long.decode()` doesn't support binary, so
        // we're just using decimal for the presence values here.
        "One, 0, false",
        "One, 1, true",
        "One, 2, false",
        "One, 3, false",
        "Any, 0, true",
        "Any, 1, true",
        "Any, 2, true",
        "Any, 3, false",
        "AtMostOne, 0, true",
        "AtMostOne, 1, true",
        "AtMostOne, 2, false",
        "AtMostOne, 3, false",
        "AtLeastOne, 0, false",
        "AtLeastOne, 1, true",
        "AtLeastOne, 2, true",
        "AtLeastOne, 3, false",
    )
    fun `validate() correctly throws exception when presence bits are invalid for signature`(cardinality: ParameterCardinality, presenceValue: Long, isValid: Boolean) {
        val signature = listOf(Parameter("a", ParameterEncoding.uint8, cardinality))
        with(PresenceBitmap()) {
            initialize(signature)
            set(0, presenceValue)
            if (isValid) {
                validate()
            } else {
                assertThrows<IonException> { validate() }
            }
        }
    }

    @Test
    fun `when all parameters are tagged and exactly-one, no presence bits are needed or written`() {
        (0..128).forEach { n -> assertExpectedPresenceBitSizes(expectedByteSize = 0, signature = List(n) { taggedExactlyOne }) }
    }

    @Test
    fun `when all parameters are tagless and exactly-one, no presence bits are needed or written`() {
        (0..128).forEach { n -> assertExpectedPresenceBitSizes(expectedByteSize = 0, signature = List(n) { taglessExactlyOne }) }
    }

    @Test
    fun `when all parameters are tagged and not exactly-one, should write expected number of presence bits`() {
        // Index of an element in this list is the number of parameters in the signature
        listOf(0, 0, 0, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3)
            .forEachIndexed { numParameters, expectedByteSize ->
                assertExpectedPresenceBitSizes(expectedByteSize, signature = List(numParameters) { taggedZeroToMany })
            }
    }

    @Test
    fun `when all parameters are tagless and not exactly-one, should write expected number of presence bits`() {
        // Index of an element in this list is the number of parameters in the signature
        listOf(0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3)
            .forEachIndexed { numParameters, expectedByteSize ->
                assertExpectedPresenceBitSizes(expectedByteSize, signature = List(numParameters) { taglessZeroToMany })
            }
    }

    private fun assertExpectedPresenceBitSizes(expectedByteSize: Int, signature: List<Parameter>) {
        val result = writePresenceBits { pb ->
            pb.initialize(signature)
            assertEquals(expectedByteSize, pb.byteSize)
            assertEquals(signature.size, pb.totalParameterCount)
        }
        assertEquals(expectedByteSize, result.size)
    }

    @Test
    fun `read 4 parameters`() {
        val signature = listOf(taggedZeroToMany, taggedZeroOrOne, taggedExactlyOne, taggedZeroToMany)
        // Bits are read in pairs from right to left
        // There are only three pairs of presence bits because "exactly-one" parameters do not get presence bits.
        val bytes = bitStringToByteArray("100010")

        val pb = PresenceBitmap()
        pb.initialize(signature)
        pb.readFrom(bytes, 0)

        assertEquals(PresenceBitmap.GROUP, pb[0])
        assertEquals(PresenceBitmap.VOID, pb[1])
        // Should automatically populate the value for the exactly-one parameter
        assertEquals(PresenceBitmap.EXPRESSION, pb[2])
        assertEquals(PresenceBitmap.GROUP, pb[3])
    }

    @Test
    fun `write 4 parameters`() {
        val signature = listOf(taggedZeroToMany, taggedZeroOrOne, taggedExactlyOne, taggedZeroToMany)

        val result = writePresenceBits { pb ->
            pb.initialize(signature)
            pb[0] = PresenceBitmap.EXPRESSION
            pb[1] = PresenceBitmap.GROUP
            pb[2] = PresenceBitmap.EXPRESSION
            pb[3] = PresenceBitmap.GROUP
        }

        assertEquals("00101001", result.toBitString())
    }

    @Test
    fun `write presence bitmap`() {
        // Ensures that the bits are written in the correct order for all possible sizes
        (PresenceBitmap.MAX_SUPPORTED_PARAMETERS downTo 0).forEach { signatureSize ->
            val signature = List(signatureSize) { taglessZeroToMany }
            (0 until signatureSize).forEach { i ->
                val parameterPresences = List(signatureSize) { j -> if (i == j) PresenceBitmap.GROUP else PresenceBitmap.EXPRESSION }
                val expected = createBitStringFromParameterPresences(parameterPresences)
                val actual = writePresenceBits { pb ->
                    pb.initialize(signature)
                    parameterPresences.forEachIndexed(pb::set)
                }
                assertEquals(expected, actual.toBitString())
            }
        }
    }

    @Test
    fun `read presence bitmap`() {
        // Ensures that the bits are read using the correct order
        (PresenceBitmap.MAX_SUPPORTED_PARAMETERS downTo 0).forEach { signatureSize ->
            val signature = List(signatureSize) { taglessZeroToMany }
            (0 until signatureSize).forEach { i ->
                val parameterPresences = List(signatureSize) { j -> if (i == j) PresenceBitmap.GROUP else PresenceBitmap.EXPRESSION }
                val inputBits = bitStringToByteArray(createBitStringFromParameterPresences(parameterPresences))

                val pb = PresenceBitmap()
                pb.initialize(signature)
                pb.readFrom(inputBits, 0)

                parameterPresences.forEachIndexed { l, expected -> assertEquals(expected, pb[l]) }
            }
        }
    }

    @Test
    fun `write presence bitmap with a required parameter`() {
        // Ensures that the bits are read using the correct order
        (PresenceBitmap.MAX_SUPPORTED_PARAMETERS downTo 0).forEach { signatureSize ->
            (0 until signatureSize).forEach { i ->
                val signature = List(signatureSize) { j -> if (j == i) taglessExactlyOne else taglessZeroToMany }
                val parameterPresences = List(signatureSize) { j ->
                    when {
                        j < i -> PresenceBitmap.RESERVED
                        j == i -> PresenceBitmap.EXPRESSION
                        j > i -> PresenceBitmap.GROUP
                        else -> TODO("Unreachable")
                    }
                }
                val expected = createBitStringFromParameterPresences(parameterPresences.filter { it != PresenceBitmap.EXPRESSION })

                val actual = writePresenceBits { pb ->
                    pb.initialize(signature)
                    parameterPresences.forEachIndexed(pb::set)
                }

                assertEquals(expected, actual.toBitString())
            }
        }
    }

    @Test
    fun `read presence bitmap with a required parameter`() {
        // Ensures that the bits are read using the correct order
        (PresenceBitmap.MAX_SUPPORTED_PARAMETERS downTo 0).forEach { signatureSize ->
            (0 until signatureSize).forEach { i ->
                val signature = List(signatureSize) { j -> if (j == i) taglessExactlyOne else taglessZeroToMany }
                val parameterPresences = List(signatureSize) { j ->
                    when {
                        j < i -> PresenceBitmap.RESERVED
                        j == i -> PresenceBitmap.EXPRESSION
                        j > i -> PresenceBitmap.GROUP
                        else -> TODO("Unreachable")
                    }
                }
                val inputBitString = createBitStringFromParameterPresences(parameterPresences.filter { it != PresenceBitmap.EXPRESSION })
                val inputBits = bitStringToByteArray(inputBitString)

                val pb = PresenceBitmap()
                pb.initialize(signature)
                pb.readFrom(inputBits, 0)

                parameterPresences.forEachIndexed { l, expected -> assertEquals(expected, pb[l]) }
            }
        }
    }

    private fun writePresenceBits(action: (PresenceBitmap) -> Unit): ByteArray {
        val pb = PresenceBitmap()
        action(pb)
        val buffer = WriteBuffer(BlockAllocatorProviders.basicProvider().vendAllocator(32)) {}
        buffer.reserve(pb.byteSize)
        pb.writeTo(buffer, 0)
        return buffer.toByteArray()
    }

    private fun WriteBuffer.toByteArray() = ByteArrayOutputStream().also { writeTo(it) }.toByteArray()
    private fun ByteArray.toBitString(): String = byteArrayToBitString(this)

    @ParameterizedTest
    @CsvSource(
        "       '', ''      ",
        "        0, 00000000",
        "      0 0, 00000000",
        "    0 0 0, 00000000",
        "  0 0 0 0, 00000000",
        "0 0 0 0 0, 00000000 00000000",
        "1 0 0 0 0, 00000001 00000000",
        "0 1 0 0 0, 00000100 00000000",
        "0 0 1 0 0, 00010000 00000000",
        "0 0 0 1 0, 01000000 00000000",
        "0 0 0 0 1, 00000000 00000001",
        "2 0 0 0 0, 00000010 00000000",
        "0 2 0 0 0, 00001000 00000000",
        "0 0 2 0 0, 00100000 00000000",
        "0 0 0 2 0, 10000000 00000000",
        "0 0 0 0 2, 00000000 00000010",
    )
    fun testCreateBitStringFromParameterPresences(presences: String, expectedBitString: String) {
        val presenceList = presences.takeIf { it.isNotBlank() }?.split(" ")?.map { it.toLong() } ?: emptyList()
        assertEquals(expectedBitString, createBitStringFromParameterPresences(presenceList))
    }

    /**
     * The purpose of this utility function is to create a bit string containing a whole number
     * of little endian bytes that represents a list of
     */
    private fun createBitStringFromParameterPresences(parameterPresences: List<Long>): String {
        val sb = StringBuilder()
        (0 until (((parameterPresences.size + 3) / 4) * 4)).forEach { i ->
            // Calculate the little-endian position
            val ii = i - 2 * (i % 4) + 3
            // If we go beyond the
            val parameterPresence = parameterPresences.getOrNull(ii) ?: 0
            val bits = when (parameterPresence) {
                0L -> "00"
                1L -> "01"
                2L -> "10"
                3L -> "11"
                else -> TODO("Unreachable")
            }
            sb.append(bits)
            if (i % 4 == 3) sb.append(' ')
        }
        return sb.toString().trim()
    }
}
