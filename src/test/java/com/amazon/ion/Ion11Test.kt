// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion

import com.amazon.ion.IonEncodingVersion.ION_1_1
import com.amazon.ion.TestUtils.And
import com.amazon.ion.TestUtils.GLOBAL_SKIP_LIST
import com.amazon.ion.TestUtils.GOOD_IONTESTS_FILES
import com.amazon.ion.TestUtils.TEXT_ONLY_FILTER
import com.amazon.ion.TestUtils.hexStringToByteArray
import com.amazon.ion.TestUtils.testdataFiles
import com.amazon.ion.impl.bin.DelimitedContainerStrategy
import com.amazon.ion.impl.bin.SymbolInliningStrategy
import com.amazon.ion.system.IonBinaryWriterBuilder
import com.amazon.ion.system.IonSystemBuilder
import com.amazon.ion.system.IonTextWriterBuilder
import com.amazon.ion.system._Private_IonBinaryWriterBuilder_1_1
import java.io.ByteArrayOutputStream
import java.io.FilenameFilter
import java.io.OutputStream
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

/**
 * TODO: Clean this up. Document why various tests are skipped. Etc.
 */
class Ion11Test {

    companion object {
        val ION = IonSystemBuilder.standard().build()

        fun ionText(text: String): Array<Any> = arrayOf(text, text.encodeToByteArray())
        fun ionBinary(text: String): Array<Any> = arrayOf("Binary: ${text.slice(0..10)}", hexStringToByteArray(text))

        // Arguments here are an array containing a String for the test case name, and a ByteArray of the test data.
        @JvmStatic
        fun ionData() = listOf(
            ionText("""a::a::c::a::0 a::a::0"""),
            ionText("""a::a::c::a::0 a::0"""),
            ionText("""foo::bar::baz::false foo::0"""),
            ionText("""a::b::c::0 d::0"""),
            ionText("""a::0 b::c::d::0"""),
            ionText("""a::b::c::d::0 a::b::c::0"""),
            ionText("""a::b::c::d::0 a::0 a::0"""),
            ionText("""abc"""),
        ) + files().flatMap { f ->
            val ion = ION.loader.load(f)
            // If there are embedded documents, flatten them into separate test cases.
            if (ion.size == 1 && ion.first().hasTypeAnnotation("embedded_documents")) {
                (ion.first() as IonContainer).mapIndexed { i, ionValue ->
                    arrayOf<Any>("${f.path}[$i]", (ionValue as IonString).stringValue().toByteArray(Charsets.UTF_8))
                }
            } else {
                listOf(arrayOf<Any>(f.path, ion.toString(IonTextWriterBuilder.standard()).toByteArray(Charsets.UTF_8)))
            }
        }

        @JvmField
        val FILES_TO_SKIP = setOf(
            "notVersionMarkers.ion",
            "symbolTablesUnknownText.ion"
        )

        @JvmStatic
        fun files() = testdataFiles(
            And(
                TEXT_ONLY_FILTER,
                GLOBAL_SKIP_LIST,
                FilenameFilter { _, name -> name !in FILES_TO_SKIP }
            ),
            GOOD_IONTESTS_FILES
        )
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ionData")
    fun writeIon11Text(name: String, ion: ByteArray) {
        val textOptions = IonTextWriterBuilder
            .standard()
            .withNewLineType(IonTextWriterBuilder.NewLineType.LF)

        textTest(ion) {
            val builder = ION_1_1.binaryWriterBuilder()
                .withSymbolInliningStrategy(SymbolInliningStrategy.ALWAYS_INLINE)
            (builder as _Private_IonBinaryWriterBuilder_1_1)._private_buildTextWriter(it, textOptions)
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ionData")
    fun writeIon11TextWithSymtab(name: String, ion: ByteArray) {
        val textOptions = IonTextWriterBuilder
            .standard()
            .withNewLineType(IonTextWriterBuilder.NewLineType.LF)

        textTest(ion) {
            val builder = ION_1_1.binaryWriterBuilder()
                .withSymbolInliningStrategy(SymbolInliningStrategy.NEVER_INLINE)
            (builder as _Private_IonBinaryWriterBuilder_1_1)._private_buildTextWriter(it, textOptions)
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ionData")
    fun writeIon11Binary(name: String, ion: ByteArray) {
        binaryTest(ion) {
            ION_1_1.binaryWriterBuilder().build(it)
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ionData")
    fun writeIon11BinaryInlineSymbols(name: String, ion: ByteArray) {

        binaryTest(ion) {
            ION_1_1.binaryWriterBuilder()
                .withSymbolInliningStrategy(SymbolInliningStrategy.ALWAYS_INLINE)
                .build(it)
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ionData")
    fun writeIon11BinaryDelimited(name: String, ion: ByteArray) {
        binaryTest(ion) {
            ION_1_1.binaryWriterBuilder()
                .withDelimitedContainerStrategy(DelimitedContainerStrategy.ALWAYS_DELIMITED)
                .build(it)
        }
    }

    fun textTest(ion: ByteArray, writerFn: (OutputStream) -> IonWriter) {
        val data: List<IonValue> = ION.loader.load(ion).map { it }
        val baos = ByteArrayOutputStream()
        val writer = writerFn(baos)
        data.forEach { it.writeTo(writer) }
        writer.close()
        println(baos.toByteArray().toString(Charsets.UTF_8))
        val loadedData = ION.loader.load(baos.toByteArray())
        println(loadedData)
        assertEquals(data, loadedData.toList())
    }

    fun binaryTest(ion: ByteArray, writerFn: (OutputStream) -> IonWriter) {
        val data: List<IonValue> = ION.loader.load(ion).map { it }
        val baos = ByteArrayOutputStream()
        val writer = writerFn(baos)
        data.forEach { it.writeTo(writer) }
        writer.close()

        ION.loader.load(ion).dump10Text()
        println("Ion 1.1 binary:")
        assertTrue(baos.toByteArray().isNotEmpty())
        baos.dump()
        val loadedData = ION.loader.load(baos.toByteArray())
        println("Round-tripped data")
        println(loadedData)
        assertEquals(data, loadedData.toList())
    }

    @OptIn(ExperimentalStdlibApi::class)
    fun ByteArrayOutputStream.dump() {
        this.toByteArray()
            .map { it.toHexString(HexFormat.UpperCase) }
            .windowed(4, 4, partialWindows = true)
            .windowed(8, 8, partialWindows = true)
            .forEach {
                println(it.joinToString("   ") { it.joinToString(" ") })
            }
    }

    fun List<IonValue>.dump10Binary() {
        val baos = ByteArrayOutputStream()
        val writer = IonBinaryWriterBuilder.standard().build(baos)
        forEach { it.writeTo(writer) }
        writer.close()
        println("Ion 1.0 Binary:")
        baos.dump()
    }

    fun IonValue.dump10Text() {
        // println("Ion 1.0 Text:")
        // println(this.toString(IonTextWriterBuilder.standard()))
    }
}
