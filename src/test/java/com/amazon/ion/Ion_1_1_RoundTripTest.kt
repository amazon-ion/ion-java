// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion

import com.amazon.ion.IonEncodingVersion.*
import com.amazon.ion.TestUtils.*
import com.amazon.ion.impl._Private_IonSystem
import com.amazon.ion.impl.bin.*
import com.amazon.ion.system.*
import java.io.ByteArrayOutputStream
import java.io.FilenameFilter
import java.io.OutputStream
import org.junit.jupiter.api.Assertions.assertEquals
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
        private val builder = ION_1_1.textWriterBuilder().withNewLineType(IonTextWriterBuilder.NewLineType.LF)

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

    @Nested
    inner class BinaryWithInternedSymbolsAndPrefixedContainers : Ion_1_1_RoundTripBase() {
        private val builder = ION_1_1.binaryWriterBuilder()
            .withSymbolInliningStrategy(SymbolInliningStrategy.NEVER_INLINE)
            .withDelimitedContainerStrategy(DelimitedContainerStrategy.ALWAYS_PREFIXED)

        override val writerFn: (OutputStream) -> IonWriter = builder::build
    }

    @Nested
    inner class BinaryWithInlineSymbolsAndPrefixedContainers : Ion_1_1_RoundTripBase() {
        private val builder = ION_1_1.binaryWriterBuilder()
            .withSymbolInliningStrategy(SymbolInliningStrategy.ALWAYS_INLINE)
            .withDelimitedContainerStrategy(DelimitedContainerStrategy.ALWAYS_PREFIXED)

        override val writerFn: (OutputStream) -> IonWriter = builder::build

        @Disabled("Ion binary reader can't seem to discover symbol tables with inline annotations")
        override fun testUserValuesArePreservedWhenTransferringSystemValues(name: String, ion: ByteArray) {
            super.testUserValuesArePreservedWhenTransferringSystemValues(name, ion)
        }
    }

    @Nested
    inner class BinaryWithInlineSymbolsAndDelimitedContainers : Ion_1_1_RoundTripBase() {
        private val builder = ION_1_1.binaryWriterBuilder()
            .withSymbolInliningStrategy(SymbolInliningStrategy.ALWAYS_INLINE)
            .withDelimitedContainerStrategy(DelimitedContainerStrategy.ALWAYS_DELIMITED)

        override val writerFn: (OutputStream) -> IonWriter = builder::build

        @Disabled("Ion binary reader can't seem to discover symbol tables with inline annotations")
        override fun testUserValuesArePreservedWhenTransferringSystemValues(name: String, ion: ByteArray) {
            super.testUserValuesArePreservedWhenTransferringSystemValues(name, ion)
        }
    }

    @Nested
    inner class BinaryWithInternedSymbolsAndDelimitedContainers : Ion_1_1_RoundTripBase() {
        private val builder = ION_1_1.binaryWriterBuilder()
            .withSymbolInliningStrategy(SymbolInliningStrategy.NEVER_INLINE)
            .withDelimitedContainerStrategy(DelimitedContainerStrategy.ALWAYS_DELIMITED)

        override val writerFn: (OutputStream) -> IonWriter = builder::build
    }
}

/**
 * Base class that contains text-specific cases
 */
abstract class Ion_1_1_RoundTripTextBase : Ion_1_1_RoundTripBase() {
    abstract val newWriterForAppendable: (Appendable) -> IonWriter

    @ParameterizedTest(name = "{0}")
    @MethodSource("testData")
    fun testUserValuesSurviveRoundTripWrittenToAppendable(name: String, ion: ByteArray) {
        val data: List<IonValue> = ION.loader.load(ion)
        val appendable = StringBuilder()
        val writer = newWriterForAppendable(appendable)
        data.forEach { it.writeTo(writer) }
        writer.close()
        val actual = appendable.toString()

        println("Expected:")
        ion.printDisplayString()
        println("Actual:")
        println(actual)

        assertReadersHaveEquivalentValues(
            ION.newReader(ion),
            ION.newReader(actual)
        )
    }
}

@OptIn(ExperimentalStdlibApi::class)
abstract class Ion_1_1_RoundTripBase {

    abstract val writerFn: (OutputStream) -> IonWriter

    @ParameterizedTest(name = "{0}")
    @MethodSource("testData")
    fun testUserValuesArePreservedWhenTransferringUserValues(name: String, ion: ByteArray) {

        // Read and compare the data.
        val actual = roundTripToByteArray { w -> newReader(ion).let(::iterate).forEach { it.writeTo(w) } }
        println("Expected:")
        ion.printDisplayString()
        println("Actual:")
        actual.printDisplayString()

        assertReadersHaveEquivalentValues(
            ION.newReader(ion),
            ION.newReader(actual)
        )
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testData")
    fun testUserValuesArePreservedWhenTransferringUserValuesUsingWriteValueForReader(name: String, ion: ByteArray) {

        // Read and compare the data.
        val actual = roundTripToByteArray { w -> newReader(ion).let { r -> while (r.next() != null) w.writeValue(r) } }
        println("Expected:")
        ion.printDisplayString()
        println("Actual:")
        actual.printDisplayString()

        assertReadersHaveEquivalentValues(
            ION.newReader(ion),
            ION.newReader(actual)
        )
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testData")
    fun testUserValuesArePreservedWhenTransferringUserValuesUsingWriteValueForIonValue(name: String, ion: ByteArray) {
        // Read and compare the data.
        val actual = roundTripToByteArray { w -> newReader(ion).let(::iterate).forEach { w.writeValue(it) } }
        println("Expected:")
        ion.printDisplayString()
        println("Actual:")
        actual.printDisplayString()

        assertReadersHaveEquivalentValues(
            ION.newReader(ion),
            ION.newReader(actual)
        )
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testData")
    open fun testUserValuesArePreservedWhenTransferringSystemValues(name: String, ion: ByteArray) {

        // Read and compare the data.
        val actual = roundTripToByteArray { w -> w.writeValues(newSystemReader(ion)) }
        println("Expected:")
        ion.printDisplayString()
        println("Actual:")
        actual.printDisplayString()

        // Check the user values
        assertReadersHaveEquivalentValues(
            ION.newReader(ion),
            ION.newReader(actual)
        )
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testData")
    open fun testSystemValuesArePreservedWhenTransferringSystemValues(name: String, ion: ByteArray) {

        // Read and compare the data.
        val actual = roundTripToByteArray { w -> w.writeValues(newSystemReader(ion)) }
        println("Expected:")
        ion.printDisplayString()
        println("Actual:")
        actual.printDisplayString()

        // Check the system values
        assertReadersHaveEquivalentValues(
            ION.newSystemReader(ion),
            // Skip the initial IVM since it ends up being doubled when we're copying.
            ION.newSystemReader(actual).apply { next() }
        )
    }

    private fun roundTripToByteArray(block: _Private_IonSystem.(IonWriter) -> Unit): ByteArray {
        // Create a new copy of the data in Ion 1.1
        val baos = ByteArrayOutputStream()
        val writer = writerFn(baos)
        block(ION, writer)
        writer.close()
        return baos.toByteArray()
    }

    /**
     * Prints this ByteArray as hex octets if this contains Ion Binary, otherwise prints as UTF-8 decoded string.
     */
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
                throw AssertionError("Encountered IonException when reading the transcribed version of value #$ie\n$expected", e)
            }
            ie++
        }

        // Make sure that both are fully consumed.
        var ia = ie
        while (expectedData.hasNext()) { expectedData.next(); ie++ }
        while (actualData.hasNext()) { actualData.next(); ia++ }

        assertEquals(ie, ia, "Data is unequal length")
    }

    /**
     * Checks if this ByteArray contains Ion Binary.
     */
    private fun ByteArray.isIonBinary(): Boolean {
        return get(0) == 0xE0.toByte() &&
            get(1) == 0x01.toByte() &&
            get(2) in setOf<Byte>(0, 1) &&
            get(3) == 0xEA.toByte()
    }

    private fun isIonVersionMarker(symbol: SymbolToken?): Boolean {
        symbol ?: return false
        if (symbol.sid == 2) return true
        symbol.text ?: return false
        return ION_VERSION_MARKER_REGEX.matches(symbol.assumeText())
    }

    companion object {
        @JvmStatic
        protected val ION = IonSystemBuilder.standard().build() as _Private_IonSystem
        private val ION_VERSION_MARKER_REGEX = Regex("^\\\$ion_[0-9]+_[0-9]+$")

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
