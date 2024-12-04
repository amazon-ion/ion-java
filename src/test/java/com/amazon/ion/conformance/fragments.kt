// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.conformance

import com.amazon.ion.*
import com.amazon.ion.IonEncodingVersion.*
import com.amazon.ion.TestUtils.*
import com.amazon.ion.conformance.ConformanceTestBuilder.*
import com.amazon.ion.conformance.Encoding.*
import com.amazon.ion.impl.*
import com.amazon.ion.impl.bin.*
import com.amazon.ion.impl.macro.*
import com.amazon.ion.system.*
import com.amazon.ion.util.*
import com.amazon.ionelement.api.AnyElement
import com.amazon.ionelement.api.ElementType
import com.amazon.ionelement.api.IntElement
import com.amazon.ionelement.api.SeqElement
import com.amazon.ionelement.api.StringElement
import com.amazon.ionelement.api.SymbolElement
import com.amazon.ionelement.api.TextElement
import com.amazon.ionelement.api.ionInt
import com.amazon.ionelement.api.ionSexpOf
import com.amazon.ionelement.api.ionSymbol
import com.amazon.ionelement.api.loadSingleElement
import java.io.ByteArrayOutputStream
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.contract

/** Helper function for creating ivm fragments for the `ion_1_*` keywords */
fun ivm(sexp: SeqElement, major: Int, minor: Int): SeqElement {
    return ionSexpOf(listOf(ionSymbol("ivm"), ionInt(major.toLong()), ionInt(minor.toLong())), metas = sexp.metas)
}

@OptIn(ExperimentalContracts::class)
fun AnyElement.isFragment(): Boolean {
    contract { returns(true) implies (this@isFragment is SeqElement) }
    return this is SeqElement && this.head in FRAGMENT_KEYWORDS
}

// All known fragment keywords
// TODO: When we update the ion-tests commit to include https://github.com/amazon-ion/ion-tests/pull/129
//       we need to remove "bytes" from this list
private val FRAGMENT_KEYWORDS = setOf("ivm", "text", "binary", "bytes", "toplevel", "encoding", "mactab")
// Insert this between every fragment when transcoding to text
val SERIALIZED_TEXT_FRAGMENT_SEPARATOR = "\n".toByteArray(Charsets.UTF_8)

// TODO: Update these so that they provide raw writers. That will resolve some of the issues
//       such as not being able to write system values to the Ion 1.1 managed writer and not
//       being able to write invalid imports.
private sealed interface Encoding {
    val writerBuilder: IonWriterBuilder

    sealed interface Binary : Encoding
    sealed interface Text : Encoding
    sealed interface `1,0` : Encoding
    sealed interface `1,1` : Encoding

    object Binary10 : Binary, `1,0` {
        override val writerBuilder: IonWriterBuilder =
            ION_1_0.binaryWriterBuilder().withCatalog(ION_CONFORMANCE_TEST_CATALOG)
    }

    object Binary11 : Binary, `1,1` {
        override val writerBuilder: IonWriterBuilder =
            ION_1_1.binaryWriterBuilder().withCatalog(ION_CONFORMANCE_TEST_CATALOG) as IonWriterBuilder
    }

    object Text10 : Text, `1,0` {
        override val writerBuilder: IonWriterBuilder =
            (ION_1_0.textWriterBuilder() as _Private_IonTextWriterBuilder<*>)
                .withInvalidSidsAllowed(true)
                .withWriteTopLevelValuesOnNewLines(true)
                .withCatalog(ION_CONFORMANCE_TEST_CATALOG)
                .withInitialIvmHandling(IonWriterBuilder.InitialIvmHandling.SUPPRESS)
    }

    object Text11 : Text, `1,1` {
        override val writerBuilder: IonWriterBuilder =
            (ION_1_1.textWriterBuilder() as _Private_IonTextWriterBuilder<*>)
                .withInvalidSidsAllowed(true)
                .withWriteTopLevelValuesOnNewLines(true)
                .withInitialIvmHandling(IonWriterBuilder.InitialIvmHandling.SUPPRESS)
                .withCatalog(ION_CONFORMANCE_TEST_CATALOG)
                .also {
                    it as _Private_IonTextWriterBuilder_1_1
                    it.withSymbolInliningStrategy(SymbolInliningStrategy.ALWAYS_INLINE)
                }
    }
}

/**
 * If we have an invalid version, don't complain here. Bad fragments should propagate through
 * and be detected by the test expectations.
 */
private fun Encoding.getEncodingVersion(minor: Int): Encoding = when (minor) {
    0 -> if (this is Text) Text10 else Binary10
    1 -> if (this is Text) Text11 else Binary11
    // Unknown version -- just return this.
    else -> this
}

private val Encoding.ivmBytes: ByteArray
    get() = when (this) {
        Binary10 -> byteArrayOf(0xE0.toByte(), 1, 0, 0xEA.toByte())
        Binary11 -> byteArrayOf(0xE0.toByte(), 1, 1, 0xEA.toByte())
        Text10 -> "\$ion_1_0".toByteArray(Charsets.UTF_8)
        Text11 -> "\$ion_1_1".toByteArray(Charsets.UTF_8)
    }

/**
 * Read all fragments, transcoding and combining the data into Ion binary or Ion text UTF-8 encoded bytes.
 */
fun TestCaseSupport.readFragments(fragments: List<SeqElement>): ByteArray {
    debug { "Initializing Input Data..." }
    // TODO: Detect versions and switch accordingly.
    val encodeToBinary = 0 < fragments.count {
        debug { "Inspecting (${it.head} ...) at ${locationOf(it)}" }
        // TODO: When we update the ion-tests commit to include https://github.com/amazon-ion/ion-tests/pull/129
        //       we need to remove "bytes" from this check
        it.head == "bytes" || it.head == "binary"
    }

    val encoding: Encoding = if (encodeToBinary) Binary10 else Text10

    fun debugString(i: Int, bytes: ByteArray): String =
        with(bytes) { if (encodeToBinary) toPrettyHexString() else toString(Charsets.UTF_8) }
            .replaceIndent("  | ")
            .let { "Fragment $i\n$it" }

    val serializedFragments = mutableListOf<ByteArray>()

    // All documents start as Ion 1.0, but we must explicitly ensure that the IVM is present if
    // transcoding fragments to binary.
    if (encodeToBinary) serializedFragments.add(encoding.ivmBytes)

    fragments.foldIndexed(encoding) { i, encodingVersion, fragment ->
        val (bytes, continueWithVersion) = readFragment(fragment, encodingVersion)
        serializedFragments.add(bytes)
        debug { debugString(i, bytes) }
        // If it's text, we need to ensure there is whitespace between fragments
        if (encodingVersion is Text) serializedFragments.add(SERIALIZED_TEXT_FRAGMENT_SEPARATOR)
        continueWithVersion
    }
    return serializedFragments.joinToByteArray()
}

/** Reads a single fragment */
private fun TestCaseSupport.readFragment(fragment: SeqElement, encoding: Encoding): Pair<ByteArray, Encoding> {
    return when (fragment.head) {
        "ivm" -> readIvmFragment(fragment, encoding)
        "text" -> readTextFragment(fragment, encoding)
        "binary" -> readBytesFragment(fragment, encoding)
        // TODO: When we update the ion-tests commit to include https://github.com/amazon-ion/ion-tests/pull/129
        //       we need to remove "bytes" from this when expression
        "bytes" -> readBytesFragment(fragment, encoding)
        "toplevel" -> readTopLevelFragment(fragment, encoding)
        "mactab" -> readMactabFragment(fragment, encoding)
        "encoding" -> TODO("encoding")
        else -> reportSyntaxError(fragment, "not a valid fragment")
    }
}

/** Reads an `IVM` fragment and returns a byte array with an IVM for the given [encoding]. */
private fun TestCaseSupport.readIvmFragment(fragment: SeqElement, encoding: Encoding): Pair<ByteArray, Encoding> {
    val major = fragment.values[1].longValue
    val minor = fragment.values[2].longValue
    val ivmBytes = if (encoding is Text) {
        "\$ion_${major}_$minor".toByteArray()
    } else {
        byteArrayOf(0xE0.toByte(), major.toByte(), minor.toByte(), 0xEA.toByte())
    }
    // If the IVM is for an unknown version, then the ivmBytes will not match the returned Encoding.
    // This is generally fine because the test should be expecting the invalid IVM. If there's something
    // wrong with the test framework, it could manifest in strange ways.
    return ivmBytes to encoding.getEncodingVersion(minor.toInt())
}

/**
 * Reads a `text` fragment. Does not transcode, but (to-do) keeps track of whether an IVM is encountered,
 * and returns the text as a UTF-8 [ByteArray] along with the current encoding version at the end of the fragment.
 */
private fun TestCaseSupport.readTextFragment(fragment: SeqElement, encoding: Encoding): Pair<ByteArray, Encoding> {
    if (encoding !is Text) {
        TODO("Changing between binary and text is not supported.")
    }
    val text = fragment.tail.joinToString("\n") {
        // TODO: Detect and update the encoding if there's an IVM midstream
        (it as? StringElement)?.textValue
            ?: reportSyntaxError(it, "text fragment may only contain strings")
    }
    return text.toByteArray(Charsets.UTF_8) to encoding
}

/**
 * Reads a `bytes` fragment. Does not transcode, but (to-do) keeps track of whether an IVM is encountered,
 * and returns bytes and the current encoding version at the end of the fragment.
 */
private fun TestCaseSupport.readBytesFragment(fragment: SeqElement, encoding: Encoding): Pair<ByteArray, Encoding> {
    require(encoding is Binary)
    // TODO: Detect and update the encoding if there's an IVM midstream
    return readBytes(fragment) to encoding
}

/**
 * Reads a `bytes` clause, returning a [ByteArray].
 */
fun TestCaseSupport.readBytes(sexp: SeqElement): ByteArray {
    val bytes = mutableListOf<ByteArray>()
    sexp.tail.forEach {
        when (it) {
            is StringElement -> hexStringToByteArray(cleanCommentedHexBytes(it.stringValue))
            is IntElement -> byteArrayOf(it.longValue.toByte())
            else -> reportSyntaxError(it, "Not a valid element in a binary clause")
        }.let(bytes::add)
    }
    return bytes.joinToByteArray()
}

/**
 * Reads a `toplevel` clause, transcoding it to the requested [encoding].
 */
private fun TestCaseSupport.readTopLevelFragment(fragment: SeqElement, encoding: Encoding): Pair<ByteArray, Encoding> {
    val baos = ByteArrayOutputStream()
    var currentEncoding = encoding
    var currentWriter = encoding.writerBuilder.build(baos)

    fragment.tail.forEach {
        // TODO: Check for IVMs and update `currentEncoding` and `currentWriter` accordingly
        //       Alternately, we could check for IVMs and split into multiple fragments so that
        //       each fragment can be written separately.
        if (it is SymbolElement && it.textValue.matches(Regex("#?\\\$ion_\\d+_\\d+"))) {
            TODO("change Ion version while in in toplevel fragment")
        }
        it.asAnyElement().demangledWriteTo(currentWriter)
    }
    currentWriter.close()
    val bytes = baos.toByteArray()
        // Drop the initial IVM
        .let { if (encoding is Binary) it.drop(4).toByteArray() else it }
        .let { if (encoding is Text11) it.drop("\$ion_1_1".length).toByteArray() else it }
    return bytes to currentEncoding
}

private fun TestCaseSupport.readMactabFragment(fragment: SeqElement, encoding: Encoding): Pair<ByteArray, Encoding> {
    val baos = ByteArrayOutputStream()
    var currentEncoding = encoding
    var currentWriter = encoding.writerBuilder.build(baos)

    // TODO: Consider replacing this to use literal values instead of the `set_macros` macro to
    //       minimize dependencies in tests.

    // Can't have a mactab for an Ion 1.0 segment, so this should be safe
    currentWriter as MacroAwareIonWriter
    currentWriter.startMacro(SystemMacro.SetMacros)
    currentWriter.startExpressionGroup()
    fragment.tail.forEach {
        it.writeTo(currentWriter)
    }
    currentWriter.endExpressionGroup()
    currentWriter.endMacro()
    currentWriter.close()
    val bytes = baos.toByteArray()
        // Drop the initial IVM
        .let { if (encoding is Binary) it.drop(4).toByteArray() else it }
        .let { if (encoding is Text11) it.drop("\$ion_1_1".length).toByteArray() else it }
    return bytes to currentEncoding
}

/**
 * Writes this [AnyElement] to an [IonWriter], applying the de-mangling logic described at
 * [Conformance – Abstract Syntax Forms](https://github.com/amazon-ion/ion-tests/tree/master/conformance#abstract-syntax-forms).
 */
private fun AnyElement.demangledWriteTo(writer: IonWriter) {
    writer.setTypeAnnotationSymbols(*annotations.map(::demangleSymbolToken).toTypedArray())
    if (isNull) {
        writer.writeNull(type.toIonType())
    } else when (type) {
        ElementType.BOOL -> writer.writeBool(booleanValue)
        ElementType.INT -> writer.writeInt(bigIntegerValue)
        ElementType.FLOAT -> writer.writeFloat(doubleValue)
        ElementType.DECIMAL -> writer.writeDecimal(decimalValue)
        ElementType.TIMESTAMP -> writer.writeTimestamp(timestampValue)
        ElementType.SYMBOL -> writer.writeSymbolToken(demangleSymbolToken(symbolValue))
        ElementType.STRING -> writer.writeString(stringValue)
        ElementType.CLOB -> writer.writeClob(bytesValue.copyOfBytes())
        ElementType.BLOB -> writer.writeBlob(bytesValue.copyOfBytes())
        ElementType.LIST -> {
            writer.stepIn(IonType.LIST)
            listValues.forEach { it.demangledWriteTo(writer) }
            writer.stepOut()
        }
        ElementType.SEXP -> {
            val head = sexpValues.firstOrNull()
            if (head is TextElement && head.textValue.startsWith("#$:")) {
                val tail = sexpValues.drop(1)
                if (head.textValue == "#$::") {
                    // Write an expression group
                    writer as IonManagedWriter_1_1
                    val rawWriter = writer.getRawUserWriter()
                    rawWriter.stepInExpressionGroup(usingLengthPrefix = true)
                    tail.forEach { it.demangledWriteTo(writer) }
                    rawWriter.stepOut()
                } else {
                    // Write an e-expression
                    writer.writeDemangledEExpression(head, tail)
                }
            } else {
                writer.stepIn(IonType.SEXP)
                sexpValues.forEach { it.demangledWriteTo(writer) }
                writer.stepOut()
            }
        }
        ElementType.STRUCT -> {
            writer.stepIn(IonType.STRUCT)
            structFields.forEach { (k, v) ->
                writer.setFieldNameSymbol(demangleSymbolToken(k))
                v.demangledWriteTo(writer)
            }
            writer.stepOut()
        }
        ElementType.NULL -> TODO("Unreachable")
    }
}

private fun IonWriter.writeDemangledEExpression(head: TextElement, tail: List<AnyElement>) {
    this as IonManagedWriter_1_1
    val rawWriter = this.getRawUserWriter()

    // Drop the first 3 characters (the `#$:`) and then parse as Ion
    val macroReference = loadSingleElement(head.textValue.drop(3))
    val annotations = macroReference.annotations
    if (annotations.isNotEmpty()) {
        if (annotations.singleOrNull() == "\$ion") {
            val systemMacro = when (macroReference) {
                is SymbolElement -> SystemMacro[macroReference.textValue]!!
                is IntElement -> SystemMacro[macroReference.longValue.toInt()]!!
                else -> throw IllegalArgumentException("Not a valid macro reference: $head")
            }
            rawWriter.stepInEExp(systemMacro)
            tail.forEach { it.demangledWriteTo(this) }
            rawWriter.stepOut()
        } else {
            TODO("demangled, non-system, qualified e-expressions")
        }
    } else if (macroReference is IntElement) {
        val macro = if (rawWriter is IonRawBinaryWriter_1_1) {
            // For this to work in binary, we need to look up the signature.
            TODO("For Ion binary, we need to look up the macro definition")
        } else {
            // For Ion Text, we can cheat and use a placeholder because the macro arg isn't used.
            SystemMacro.None
        }
        rawWriter.stepInEExp(macroReference.longValue.toInt(), usingLengthPrefix = false, macro)
        tail.forEach { it.demangledWriteTo(this) }
        rawWriter.stepOut()
    } else if (macroReference is SymbolElement) {
        if (rawWriter is IonRawBinaryWriter_1_1) {
            TODO("For Ion binary, we need to look up the address for the macro and invoke by ID")
        }
        rawWriter.stepInEExp(macroReference.textValue)
        tail.forEach { it.demangledWriteTo(this) }
        rawWriter.stepOut()
    } else {
        throw IllegalArgumentException("Not a valid macro reference: $head")
    }
}

private fun demangleSymbolToken(text: String): SymbolToken {
    return if (text.startsWith("#\$ion_")) {
        // Escaped IVM or system symbol
        FakeSymbolToken(text.drop(1), -1)
    } else if (text.startsWith("#$:")) {
        // E-Expression macro id -- Should be unreachable; handled elsewhere
        TODO("Should be unreachable! demangled e-expressions - $text")
    } else if (text.startsWith("#$")) {
        // Escaped SID
        val id = text.drop(2).toInt()
        FakeSymbolToken(null, id)
    } else {
        FakeSymbolToken(text, -1)
    }
}
