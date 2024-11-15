// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion

import com.amazon.ion.IonEncodingVersion.*
import com.amazon.ion.TestUtils.*
import com.amazon.ion.impl._Private_IonSystem
import com.amazon.ion.impl._Private_IonWriter
import com.amazon.ion.impl.bin.*
import com.amazon.ion.system.*
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.FilenameFilter
import java.io.OutputStream
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Nested
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

/**
 * Suite of tests for running round trip tests on user and system values for various Ion 1.1 encodings.
 */
class Ion_1_1_RoundTripTest {

    @Nested
    inner class Text : Ion_1_1_RoundTripTextBase() {
        private val builder = ION_1_1.textWriterBuilder()
            .withNewLineType(IonTextWriterBuilder.NewLineType.LF)
            .withSymbolInliningStrategy(SymbolInliningStrategy.ALWAYS_INLINE)

        override val writerFn: (OutputStream) -> IonWriter = builder::build
        override val newWriterForAppendable: (Appendable) -> IonWriter = builder::build
    }

    @Nested
    inner class TextWithSymbolTable : Ion_1_1_RoundTripTextBase() {
        private val builder = ION_1_1.textWriterBuilder()
            .withNewLineType(IonTextWriterBuilder.NewLineType.LF)
            .withSymbolInliningStrategy(SymbolInliningStrategy.NEVER_INLINE)

        override val writerFn: (OutputStream) -> IonWriter = builder::build
        override val newWriterForAppendable: (Appendable) -> IonWriter = builder::build
    }

    // Writer: Interned/Prefixed

    @Nested
    inner class BinaryWithInternedSymbolsAndPrefixedContainers_ReaderContinuableBufferDefault : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INTERNED_PREFIXED
        override val readerFn: (ByteArray) -> IonReader = READER_CONTINUABLE_BUFFER_DEFAULT
    }

    @Nested
    inner class BinaryWithInternedSymbolsAndPrefixedContainers_ReaderContinuableBuffer16 : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INTERNED_PREFIXED
        override val readerFn: (ByteArray) -> IonReader = READER_CONTINUABLE_BUFFER_16
    }

    @Nested
    inner class BinaryWithInternedSymbolsAndPrefixedContainers_ReaderContinuableStreamDefault : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INTERNED_PREFIXED
        override val readerFn: (ByteArray) -> IonReader = READER_CONTINUABLE_STREAM_DEFAULT
    }

    @Nested
    inner class BinaryWithInternedSymbolsAndPrefixedContainers_ReaderContinuableStream16 : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INTERNED_PREFIXED
        override val readerFn: (ByteArray) -> IonReader = READER_CONTINUABLE_STREAM_16
    }

    @Nested
    inner class BinaryWithInternedSymbolsAndPrefixedContainers_ReaderNonContinuableBufferDefault : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INTERNED_PREFIXED
        override val readerFn: (ByteArray) -> IonReader = READER_NON_CONTINUABLE_BUFFER_DEFAULT
    }

    @Nested
    inner class BinaryWithInternedSymbolsAndPrefixedContainers_ReaderNonContinuableBuffer16 : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INTERNED_PREFIXED
        override val readerFn: (ByteArray) -> IonReader = READER_NON_CONTINUABLE_BUFFER_16
    }

    @Nested
    inner class BinaryWithInternedSymbolsAndPrefixedContainers_ReaderNonContinuableStreamDefault : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INTERNED_PREFIXED
        override val readerFn: (ByteArray) -> IonReader = READER_NON_CONTINUABLE_STREAM_DEFAULT
    }

    @Nested
    inner class BinaryWithInternedSymbolsAndPrefixedContainers_ReaderNonContinuableStream16 : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INTERNED_PREFIXED
        override val readerFn: (ByteArray) -> IonReader = READER_NON_CONTINUABLE_STREAM_16
    }

    // Writer: Inline/Prefixed

    @Nested
    inner class BinaryWithInlineSymbolsAndPrefixedContainers_ReaderContinuableBufferDefault : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INLINE_PREFIXED
        override val readerFn: (ByteArray) -> IonReader = READER_CONTINUABLE_BUFFER_DEFAULT
    }

    @Nested
    inner class BinaryWithInlineSymbolsAndPrefixedContainers_ReaderContinuableBuffer16 : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INLINE_PREFIXED
        override val readerFn: (ByteArray) -> IonReader = READER_CONTINUABLE_BUFFER_16
    }

    @Nested
    inner class BinaryWithInlineSymbolsAndPrefixedContainers_ReaderContinuableStreamDefault : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INLINE_PREFIXED
        override val readerFn: (ByteArray) -> IonReader = READER_CONTINUABLE_STREAM_DEFAULT
    }

    @Nested
    inner class BinaryWithInlineSymbolsAndPrefixedContainers_ReaderContinuableStream16 : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INLINE_PREFIXED
        override val readerFn: (ByteArray) -> IonReader = READER_CONTINUABLE_STREAM_16
    }

    @Nested
    inner class BinaryWithInlineSymbolsAndPrefixedContainers_ReaderNonContinuableBufferDefault : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INLINE_PREFIXED
        override val readerFn: (ByteArray) -> IonReader = READER_NON_CONTINUABLE_BUFFER_DEFAULT
    }

    @Nested
    inner class BinaryWithInlineSymbolsAndPrefixedContainers_ReaderNonContinuableBuffer16 : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INLINE_PREFIXED
        override val readerFn: (ByteArray) -> IonReader = READER_NON_CONTINUABLE_BUFFER_16
    }

    @Nested
    inner class BinaryWithInlineSymbolsAndPrefixedContainers_ReaderNonContinuableStreamDefault : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INLINE_PREFIXED
        override val readerFn: (ByteArray) -> IonReader = READER_NON_CONTINUABLE_STREAM_DEFAULT
    }

    @Nested
    inner class BinaryWithInlineSymbolsAndPrefixedContainers_ReaderNonContinuableStream16 : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INLINE_PREFIXED
        override val readerFn: (ByteArray) -> IonReader = READER_NON_CONTINUABLE_STREAM_16
    }

    // Writer: Inline/Delimited

    @Nested
    inner class BinaryWithInlineSymbolsAndDelimitedContainers_ReaderContinuableBufferDefault : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INLINE_DELIMITED
        override val readerFn: (ByteArray) -> IonReader = READER_CONTINUABLE_BUFFER_DEFAULT
    }

    @Nested
    inner class BinaryWithInlineSymbolsAndDelimitedContainers_ReaderContinuableBuffer16 : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INLINE_DELIMITED
        override val readerFn: (ByteArray) -> IonReader = READER_CONTINUABLE_BUFFER_16
    }

    @Nested
    inner class BinaryWithInlineSymbolsAndDelimitedContainers_ReaderContinuableStreamDefault : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INLINE_DELIMITED
        override val readerFn: (ByteArray) -> IonReader = READER_CONTINUABLE_STREAM_DEFAULT
    }

    @Nested
    inner class BinaryWithInlineSymbolsAndDelimitedContainers_ReaderContinuableStream16 : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INLINE_DELIMITED
        override val readerFn: (ByteArray) -> IonReader = READER_CONTINUABLE_STREAM_16
    }

    @Nested
    inner class BinaryWithInlineSymbolsAndDelimitedContainers_ReaderNonContinuableBufferDefault : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INLINE_DELIMITED
        override val readerFn: (ByteArray) -> IonReader = READER_NON_CONTINUABLE_BUFFER_DEFAULT
    }

    @Nested
    inner class BinaryWithInlineSymbolsAndDelimitedContainers_ReaderNonContinuableBuffer16 : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INLINE_DELIMITED
        override val readerFn: (ByteArray) -> IonReader = READER_NON_CONTINUABLE_BUFFER_16
    }

    @Nested
    inner class BinaryWithInlineSymbolsAndDelimitedContainers_ReaderNonContinuableStreamDefault : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INLINE_DELIMITED
        override val readerFn: (ByteArray) -> IonReader = READER_NON_CONTINUABLE_STREAM_DEFAULT
    }

    @Nested
    inner class BinaryWithInlineSymbolsAndDelimitedContainers_ReaderNonContinuableStream16 : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INLINE_DELIMITED
        override val readerFn: (ByteArray) -> IonReader = READER_NON_CONTINUABLE_STREAM_16
    }

    // Writer: Interned / Delimited

    @Nested
    inner class BinaryWithInternedSymbolsAndDelimitedContainers_ReaderContinuableBufferDefault : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INTERNED_DELIMITED
        override val readerFn: (ByteArray) -> IonReader = READER_CONTINUABLE_BUFFER_DEFAULT
    }

    @Nested
    inner class BinaryWithInternedSymbolsAndDelimitedContainers_ReaderContinuableBuffer16 : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INTERNED_DELIMITED
        override val readerFn: (ByteArray) -> IonReader = READER_CONTINUABLE_BUFFER_16
    }

    @Nested
    inner class BinaryWithInternedSymbolsAndDelimitedContainers_ReaderContinuableStreamDefault : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INTERNED_DELIMITED
        override val readerFn: (ByteArray) -> IonReader = READER_CONTINUABLE_STREAM_DEFAULT
    }

    @Nested
    inner class BinaryWithInternedSymbolsAndDelimitedContainers_ReaderContinuableStream16 : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INTERNED_DELIMITED
        override val readerFn: (ByteArray) -> IonReader = READER_CONTINUABLE_STREAM_16
    }

    @Nested
    inner class BinaryWithInternedSymbolsAndDelimitedContainers_ReaderNonContinuableBufferDefault : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INTERNED_DELIMITED
        override val readerFn: (ByteArray) -> IonReader = READER_NON_CONTINUABLE_BUFFER_DEFAULT
    }

    @Nested
    inner class BinaryWithInternedSymbolsAndDelimitedContainers_ReaderNonContinuableBuffer16 : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INTERNED_DELIMITED
        override val readerFn: (ByteArray) -> IonReader = READER_NON_CONTINUABLE_BUFFER_16
    }

    @Nested
    inner class BinaryWithInternedSymbolsAndDelimitedContainers_ReaderNonContinuableStreamDefault : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INTERNED_DELIMITED
        override val readerFn: (ByteArray) -> IonReader = READER_NON_CONTINUABLE_STREAM_DEFAULT
    }

    @Nested
    inner class BinaryWithInternedSymbolsAndDelimitedContainers_ReaderNonContinuableStream16 : Ion_1_1_RoundTripBase() {
        override val writerFn: (OutputStream) -> IonWriter = WRITER_INTERNED_DELIMITED
        override val readerFn: (ByteArray) -> IonReader = READER_NON_CONTINUABLE_STREAM_16
    }
}

/**
 * Base class that contains text-specific cases
 */
abstract class Ion_1_1_RoundTripTextBase : Ion_1_1_RoundTripBase() {
    abstract val newWriterForAppendable: (Appendable) -> IonWriter
    override val readerFn: (ByteArray) -> IonReader = IonReaderBuilder.standard()::build

    @ParameterizedTest(name = "{0}")
    @MethodSource("testData")
    fun testUserValuesSurviveRoundTripWrittenToAppendable(name: String, ion: ByteArray) {
        val data: List<IonValue> = ION.loader.load(ion)
        val appendable = StringBuilder()
        val writer = newWriterForAppendable(appendable)
        data.forEach { it.writeTo(writer) }
        writer.close()
        val actual = appendable.toString()

        if (DEBUG_MODE) {
            println("Expected:")
            ion.printDisplayString()
            println("Actual:")
            println(actual)
        }

        assertReadersHaveEquivalentValues(
            ION.newReader(ion),
            ION.newReader(actual)
        )
    }
}

@OptIn(ExperimentalStdlibApi::class)
abstract class Ion_1_1_RoundTripBase {

    abstract val writerFn: (OutputStream) -> IonWriter
    abstract val readerFn: (ByteArray) -> IonReader
    val systemReaderFn: (ByteArray) -> IonReader = ION::newSystemReader

    @ParameterizedTest(name = "{0}")
    @MethodSource("testData")
    fun testUserValuesArePreservedWhenTransferringUserValues(name: String, ion: ByteArray) {

        // Read and compare the data.
        val actual = roundTripToByteArray { w -> newReader(ion).let(::iterate).forEach { it.writeTo(w) } }

        printDebugInfo(ion, actual)

        assertReadersHaveEquivalentValues(
            readerFn(ion),
            readerFn(actual)
        )
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testData")
    fun testUserValuesArePreservedWhenTransferringUserValuesUsingWriteValueForReader(name: String, ion: ByteArray) {

        // Read and compare the data.
        val actual = roundTripToByteArray { w -> newReader(ion).let { r -> while (r.next() != null) w.writeValue(r) } }

        printDebugInfo(ion, actual)

        assertReadersHaveEquivalentValues(
            readerFn(ion),
            readerFn(actual)
        )
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testData")
    fun testUserValuesArePreservedWhenTransferringUserValuesUsingWriteValueForIonValue(name: String, ion: ByteArray) {
        // Read and compare the data.
        val actual = roundTripToByteArray { w -> newReader(ion).let(::iterate).forEach { w.writeValue(it) } }

        printDebugInfo(ion, actual)

        assertReadersHaveEquivalentValues(
            readerFn(ion),
            readerFn(actual)
        )
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testData")
    @Disabled("Re-interpreting system directives is not supported yet.")
    open fun testUserValuesArePreservedWhenTransferringSystemValues(name: String, ion: ByteArray) {

        // Read and compare the data.
        val actual = roundTripToByteArray { w ->
            w as _Private_IonWriter
            w.writeValues(newSystemReader(ion)) { x -> x - 9 }
        }

        printDebugInfo(ion, actual)

        // Check the user values
        assertReadersHaveEquivalentValues(
            readerFn(ion),
            readerFn(actual)
        )
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testData")
    @Disabled("Re-interpreting system directives is not supported yet.")
    open fun testSystemValuesArePreservedWhenTransferringSystemValues(name: String, ion: ByteArray) {

        // Read and compare the data.
        val actual = roundTripToByteArray { w ->
            w as _Private_IonWriter
            w.writeValues(newSystemReader(ion)) { x -> x - 9 }
        }

        printDebugInfo(ion, actual)

        // Check the system values
        assertReadersHaveEquivalentValues(
            systemReaderFn(ion),
            // Skip the initial IVM since it ends up being doubled when we're copying.
            systemReaderFn(actual).apply { next() }
        )
    }

    private fun roundTripToByteArray(block: _Private_IonSystem.(IonWriter) -> Unit): ByteArray {
        // Create a new copy of the data in Ion 1.1
        val baos = object : ByteArrayOutputStream() {
            var closed = false
            override fun close() {
                assertFalse(closed)
                closed = true
                super.close()
            }
        }
        val writer = writerFn(baos)
        block(ION, writer)
        writer.close()
        return baos.toByteArray()
    }

    fun assertReadersHaveEquivalentValues(expectedDataReader: IonReader, actualDataReader: IonReader) {
        // Read and compare the data.
        val expectedData: Iterator<IonValue> = ION.iterate(expectedDataReader)
        val actualData: Iterator<IonValue> = ION.iterate(actualDataReader)

        var ie = 0
        while (expectedData.hasNext() && actualData.hasNext()) {
            val expected = expectedData.next()
            try {
                val actual = actualData.next()

                if (expected is IonSymbol && actual is IonSymbol) {
                    if (expected.typeAnnotationSymbols.isEmpty() &&
                        isIonVersionMarker(expected.symbolValue()) &&
                        actual.typeAnnotationSymbols.isEmpty() &&
                        isIonVersionMarker(actual.symbolValue())
                    ) {
                        // Both are IVMs. We won't actually compare them because we
                        // could be comparing data from different Ion versions
                        continue
                    }
                }

                assertEquals(expected, actual, "value $ie is different")
            } catch (e: IonException) {
                throw AssertionError("Encountered IonException when reading the transcribed version of value #$ie\nExpected: $expected", e)
            }
            ie++
        }

        // Make sure that both are fully consumed.
        var ia = ie
        while (expectedData.hasNext()) { expectedData.next(); ie++ }
        while (actualData.hasNext()) { actualData.next(); ia++ }

        assertEquals(ie, ia, "Data is unequal length")
        expectedDataReader.close()
        actualDataReader.close()
    }

    private fun isIonVersionMarker(symbol: SymbolToken?): Boolean {
        symbol ?: return false
        if (symbol.sid == 2) return true
        symbol.text ?: return false
        return ION_VERSION_MARKER_REGEX.matches(symbol.assumeText())
    }

    companion object {

        @JvmStatic
        protected val DEBUG_MODE = false

        @JvmStatic
        protected val ION = IonSystemBuilder.standard().build() as _Private_IonSystem
        private val ION_VERSION_MARKER_REGEX = Regex("^\\\$ion_[0-9]+_[0-9]+$")

        @JvmStatic
        private val BUFFER_CONFIGURATION_INITIAL_SIZE_16: IonBufferConfiguration = IonBufferConfiguration.Builder.standard().withInitialBufferSize(16).build()
        @JvmStatic
        protected val READER_NON_CONTINUABLE_BUFFER_DEFAULT: (ByteArray) -> IonReader = IonReaderBuilder.standard()::build
        @JvmStatic
        protected val READER_NON_CONTINUABLE_STREAM_DEFAULT: (ByteArray) -> IonReader = { IonReaderBuilder.standard().build(ByteArrayInputStream(it)) }
        @JvmStatic
        protected val READER_NON_CONTINUABLE_BUFFER_16: (ByteArray) -> IonReader = IonReaderBuilder.standard().withBufferConfiguration(BUFFER_CONFIGURATION_INITIAL_SIZE_16)::build
        @JvmStatic
        protected val READER_NON_CONTINUABLE_STREAM_16: (ByteArray) -> IonReader = { IonReaderBuilder.standard().withBufferConfiguration(BUFFER_CONFIGURATION_INITIAL_SIZE_16).build(ByteArrayInputStream(it)) }
        @JvmStatic
        protected val READER_CONTINUABLE_BUFFER_DEFAULT: (ByteArray) -> IonReader = IonReaderBuilder.standard().withIncrementalReadingEnabled(true)::build
        @JvmStatic
        protected val READER_CONTINUABLE_STREAM_DEFAULT: (ByteArray) -> IonReader = { IonReaderBuilder.standard().withIncrementalReadingEnabled(true).build(ByteArrayInputStream(it)) }
        @JvmStatic
        protected val READER_CONTINUABLE_BUFFER_16: (ByteArray) -> IonReader = IonReaderBuilder.standard().withIncrementalReadingEnabled(true).withBufferConfiguration(BUFFER_CONFIGURATION_INITIAL_SIZE_16)::build
        @JvmStatic
        protected val READER_CONTINUABLE_STREAM_16: (ByteArray) -> IonReader = { IonReaderBuilder.standard().withIncrementalReadingEnabled(true).withBufferConfiguration(BUFFER_CONFIGURATION_INITIAL_SIZE_16).build(ByteArrayInputStream(it)) }

        @JvmStatic
        protected val WRITER_INTERNED_PREFIXED: (OutputStream) -> IonWriter = ION_1_1.binaryWriterBuilder()
            .withSymbolInliningStrategy(SymbolInliningStrategy.NEVER_INLINE)
            .withLengthPrefixStrategy(LengthPrefixStrategy.ALWAYS_PREFIXED)::build
        @JvmStatic
        protected val WRITER_INLINE_PREFIXED: (OutputStream) -> IonWriter = ION_1_1.binaryWriterBuilder()
            .withSymbolInliningStrategy(SymbolInliningStrategy.ALWAYS_INLINE)
            .withLengthPrefixStrategy(LengthPrefixStrategy.ALWAYS_PREFIXED)::build
        @JvmStatic
        protected val WRITER_INTERNED_DELIMITED: (OutputStream) -> IonWriter = ION_1_1.binaryWriterBuilder()
            .withSymbolInliningStrategy(SymbolInliningStrategy.NEVER_INLINE)
            .withLengthPrefixStrategy(LengthPrefixStrategy.NEVER_PREFIXED)::build
        @JvmStatic
        protected val WRITER_INLINE_DELIMITED: (OutputStream) -> IonWriter = ION_1_1.binaryWriterBuilder()
            .withSymbolInliningStrategy(SymbolInliningStrategy.ALWAYS_INLINE)
            .withLengthPrefixStrategy(LengthPrefixStrategy.NEVER_PREFIXED)::build

        /**
         * Checks if this ByteArray contains Ion Binary.
         */
        private fun ByteArray.isIonBinary(): Boolean {
            return get(0) == 0xE0.toByte() &&
                get(1) == 0x01.toByte() &&
                get(2) in setOf<Byte>(0, 1) &&
                get(3) == 0xEA.toByte()
        }

        /**
         * Prints this ByteArray as hex octets if this contains Ion Binary, otherwise prints as UTF-8 decoded string.
         */
        @JvmStatic
        protected fun ByteArray.printDisplayString() {
            if (isIonBinary()) {
                map { it.toHexString(HexFormat.UpperCase) }
                    .windowed(4, 4, partialWindows = true)
                    .windowed(8, 8, partialWindows = true)
                    .forEach {
                        println(it.joinToString("   ") { it.joinToString(" ") })
                    }
            } else {
                println(toString(Charsets.UTF_8))
            }
        }

        fun printDebugInfo(expected: ByteArray, actual: ByteArray) {
            if (DEBUG_MODE) {
                println("Expected:")
                expected.printDisplayString()
                println("Actual:")
                actual.printDisplayString()
            }
        }

        private fun ionText(text: String): Array<Any> = arrayOf(text, text.encodeToByteArray())
        private fun ionBinary(name: String, bytes: String): Array<Any> = arrayOf(name, hexStringToByteArray(bytes))

        // Arguments here are an array containing a String for the test case name, and a ByteArray of the test data.
        @JvmStatic
        fun testData() = listOf(
            ionText("\$ion_1_1 true \$ion_1_0 true \$ion_1_1 true"),
            ionBinary("Binary IVMs", "E0 01 01 EA 6F E0 01 00 EA 10 E0 01 01 EA 6F"),
            ionBinary("{a:{$4:b}}", "E0 01 01 EA FD 0F 01 FF 61 D3 09 A1 62"),
            ionText("""a::a::c::a::0 a::a::0"""),
            ionText("""a::a::c::a::0 a::0"""),
            ionText("""foo::bar::baz::false foo::0"""),
            ionText("""a::b::c::0 d::0"""),
            ionText("""a::0 b::c::d::0"""),
            ionText("""a::b::c::d::0 a::b::c::0"""),
            ionText("""a::b::c::d::0 a::0 a::0"""),
            ionText("""abc"""),
            // This test case has a top-level annotation that is the same number of utf-8 bytes as $ion_symbol_table
            ionText("fake_symbol_table::{}"),
            ionText(
                """
                    ${'$'}ion_1_0
                    ${'$'}ion_symbol_table::{
                      symbols:[ "a", "b", "c", "d", "e" ]
                    }
                    $10 $11 $12 $13 $14
                    ${'$'}ion_1_0
                    ${'$'}ion_symbol_table::{
                      symbols:[ "rock", "paper", "scissors", "lizard", "spock" ]
                    }
                    $10 $11 $12 $13 $14
                """.trimIndent()
            ),
            ionText("foo::(bar::baz::{abc: zar::qux::xyz::123, def: 456})")
        ) + files().flatMap { f ->
            val ion = ION.loader.load(f)
            // If there are embedded documents, flatten them into separate test cases.
            if (ion.size == 1 && ion.first().hasTypeAnnotation("embedded_documents")) {
                (ion.first() as IonContainer).mapIndexed { i, ionValue ->
                    arrayOf<Any>("${f.path}[$i]", (ionValue as IonString).stringValue().toByteArray(Charsets.UTF_8))
                }
            } else {
                listOf(arrayOf<Any>(f.path, f.readBytes()))
            }
        }

        @JvmStatic
        fun files() = testdataFiles(
            And(GLOBAL_SKIP_LIST, LOCAL_SKIP_LIST),
            GOOD_IONTESTS_FILES
        )

        @JvmField
        val LOCAL_SKIP_LIST = setOf(
            // Has an unknown, imported symbol
            "symbolTablesUnknownText.ion",
            // Skipped because there are no user values in these, and IonReaderNonContinuableSystem will throw an exception.
            "blank.ion",
            "empty.ion",
            "emptyThreeByteNopPad.10n",
            "nopPad16Bytes.10n",
            "nopPadOneByte.10n",
            "T15.10n",
        ).let { FilenameFilter { _, name -> name !in it } }
    }
}
