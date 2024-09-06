// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.*
import com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID
import com.amazon.ion.impl.*
import com.amazon.ion.impl._Private_IonWriter.IntTransformer
import com.amazon.ion.impl.bin.LengthPrefixStrategy.*
import com.amazon.ion.impl.bin.SymbolInliningStrategy.*
import com.amazon.ion.system.*
import java.io.OutputStream
import java.math.BigDecimal
import java.math.BigInteger
import java.util.*

/**
 * A managed writer for Ion 1.1 that is generic over whether the raw encoding is text or binary.
 *
 * TODO:
 *  - Handling of shared symbol tables
 *  - Proper handling of user-supplied symbol tables
 *  - Auto-flush (for binary and text)
 *
 * TODO: What package does this really belong in?
 *       See also [ManagedWriterOptions], [SymbolInliningStrategy], and [DelimitedContainerStrategy].
 */
internal class IonManagedWriter_1_1(
    private val userData: IonRawWriter_1_1,
    private val systemData: IonRawWriter_1_1,
    private val options: ManagedWriterOptions_1_1,
    private val onClose: () -> Unit,
) : _Private_IonWriter {

    private val systemSymbolTableMap = hashMapOf<String, Int>()

    init {
        // Since this is Ion 1.1, we must always start with the IVM.
        systemData.writeIVM()
        var id = 1
        Symbols.systemSymbolTable().iterateDeclaredSymbolNames().forEach {
            systemSymbolTableMap[it] = id++
        }
    }

    companion object {
        private val ION_VERSION_MARKER_REGEX = Regex("^\\\$ion_\\d+_\\d+$")

        @JvmStatic
        fun textWriter(output: OutputStream, managedWriterOptions: ManagedWriterOptions_1_1, textOptions: _Private_IonTextWriterBuilder_1_1): IonManagedWriter_1_1 {
            // TODO support all options configurable via IonTextWriterBuilder_1_1
            val appender = {
                val bufferedOutput = BufferedOutputStreamFastAppendable(output, BlockAllocatorProviders.basicProvider().vendAllocator(4096))
                _Private_IonTextAppender.forFastAppendable(bufferedOutput, Charsets.UTF_8)
            }

            return IonManagedWriter_1_1(
                userData = IonRawTextWriter_1_1(
                    options = textOptions,
                    output = appender(),
                ),
                systemData = IonRawTextWriter_1_1(
                    options = textOptions,
                    output = appender(),
                ),
                options = managedWriterOptions.copy(internEncodingDirectiveSymbols = false),
                onClose = output::close,
            )
        }

        @JvmStatic
        fun textWriter(output: Appendable, managedWriterOptions: ManagedWriterOptions_1_1, textOptions: _Private_IonTextWriterBuilder_1_1): IonManagedWriter_1_1 {
            val appender = {
                val bufferedOutput = BufferedAppendableFastAppendable(output)
                _Private_IonTextAppender.forFastAppendable(bufferedOutput, Charsets.UTF_8)
            }

            return IonManagedWriter_1_1(
                userData = IonRawTextWriter_1_1(
                    options = textOptions,
                    output = appender(),
                ),
                systemData = IonRawTextWriter_1_1(
                    options = textOptions,
                    output = appender(),
                ),
                options = managedWriterOptions.copy(internEncodingDirectiveSymbols = false),
                onClose = {},
            )
        }

        @JvmStatic
        fun binaryWriter(output: OutputStream, managedWriterOptions: ManagedWriterOptions_1_1, binaryOptions: _Private_IonBinaryWriterBuilder_1_1): IonManagedWriter_1_1 {
            // TODO: Add autoflush
            return IonManagedWriter_1_1(
                userData = IonRawBinaryWriter_1_1(
                    out = output,
                    buffer = WriteBuffer(BlockAllocatorProviders.basicProvider().vendAllocator(binaryOptions.blockSize)) {},
                    lengthPrefixPreallocation = 1
                ),
                systemData = IonRawBinaryWriter_1_1(
                    out = output,
                    buffer = WriteBuffer(BlockAllocatorProviders.basicProvider().vendAllocator(binaryOptions.blockSize)) {},
                    lengthPrefixPreallocation = 1
                ),
                options = managedWriterOptions.copy(internEncodingDirectiveSymbols = true),
                onClose = output::close,
            )
        }
    }

    // We take a slightly different approach here by handling the encoding context as a prior encoding context
    // plus a list of symbols added by the current encoding context.

    /** TODO: Document what this is for... */
    private var canAppendToLocalSymbolTable: Boolean = false
    /** The symbol table for the prior encoding context */
    private var symbolTable: HashMap<String, Int> = HashMap(systemSymbolTableMap)
    /** Max symbol ID of the prior encoding context. */
    private var priorMaxId: Int = symbolTable.size
    /** Symbols to be interned since the prior encoding context. */
    private var newSymbols: HashMap<String, Int> = LinkedHashMap() // Preserves insertion order.

    /**
     * Transformer for symbol IDs encountered during writeValues. Can be used to upgrade Ion 1.0 symbol IDs to the
     * Ion 1.1 equivalents.
     */
    private var sidTransformer: IntTransformer? = null

    private fun intern(text: String): Int {
        // Check the current symbol table
        var sid = symbolTable[text]
        if (sid != null) return sid
        // Check the to-be-appended symbols
        sid = newSymbols[text]
        if (sid != null) return sid
        // Add to the to-be-appended symbols
        sid = priorMaxId + newSymbols.size + 1
        newSymbols[text] = sid
        return sid
    }

    /** Writes a Local Symbol Table for the current encoding context, and updates the prior context. */
    private fun writeSymbolTable() {
        if (newSymbols.isEmpty()) return
        val useSid = options.internEncodingDirectiveSymbols

        with(systemData) {

            if (useSid) writeAnnotations(SystemSymbols.ION_SYMBOL_TABLE_SID) else writeAnnotations(SystemSymbols.ION_SYMBOL_TABLE)

            systemData.stepInStruct(usingLengthPrefix = false)
            if (canAppendToLocalSymbolTable) {
                // LST Append
                if (useSid) writeFieldName(SystemSymbols.IMPORTS_SID) else writeFieldName(SystemSymbols.IMPORTS)
                if (useSid) writeSymbol(SystemSymbols.ION_SYMBOL_TABLE_SID) else writeSymbol(SystemSymbols.ION_SYMBOL_TABLE)
            }
            // ... and write the new symbols
            if (useSid) writeFieldName(SystemSymbols.SYMBOLS_SID) else writeFieldName(SystemSymbols.SYMBOLS)
            stepInList(usingLengthPrefix = false)
            newSymbols.forEach { (text, _) -> writeString(text) }
            stepOut()
            stepOut()
        }

        symbolTable.putAll(newSymbols)

        newSymbols.clear()
        canAppendToLocalSymbolTable = true
    }

    private fun resetLocalSymbolTable() {
        if (depth != 0) throw IllegalStateException("Cannot reset the encoding context while stepped in any value.")
        symbolTable = HashMap(systemSymbolTableMap)
        priorMaxId = symbolTable.size
        canAppendToLocalSymbolTable = false
    }

    override fun getCatalog(): IonCatalog {
        TODO("Not part of the public API.")
    }

    /** No facets supported */
    override fun <T : Any?> asFacet(facetType: Class<T>?): T? = null

    override fun getSymbolTable(): SymbolTable {
        TODO("Why do we need to expose this to users in the first place?")
    }

    /**
     * Extension function for [SymbolInliningStrategy] to accept [SymbolToken].
     * Indicates whether a particular [SymbolToken] should be written inline (as opposed to writing as a SID).
     * Symbols with unknown text must always be written as SIDs.
     */
    private fun SymbolInliningStrategy.shouldWriteInline(symbolKind: SymbolKind, symbol: SymbolToken): Boolean {
        return symbol.text?.let { shouldWriteInline(symbolKind, it) } ?: false
    }

    override fun setFieldName(name: String) {
        handleSymbolToken(UNKNOWN_SYMBOL_ID, name, SymbolKind.FIELD_NAME, userData)
    }

    override fun setFieldNameSymbol(name: SymbolToken) {
        handleSymbolToken(name.sid, name.text, SymbolKind.FIELD_NAME, userData)
    }

    override fun addTypeAnnotation(annotation: String) {
        handleSymbolToken(UNKNOWN_SYMBOL_ID, annotation, SymbolKind.ANNOTATION, userData)
    }

    override fun setTypeAnnotations(annotations: Array<String>?) {
        // Interning happens in addTypeAnnotation
        userData._private_clearAnnotations()
        annotations?.forEach { addTypeAnnotation(it) }
    }

    override fun setTypeAnnotationSymbols(annotations: Array<SymbolToken>?) {
        userData._private_clearAnnotations()
        annotations?.forEach { handleSymbolToken(it.sid, it.text, SymbolKind.ANNOTATION, userData) }
    }

    override fun stepIn(containerType: IonType?) {
        val newDepth = depth + 1
        when (containerType) {
            IonType.LIST -> userData.stepInList(options.writeLengthPrefix(ContainerType.LIST, newDepth))
            IonType.SEXP -> userData.stepInSExp(options.writeLengthPrefix(ContainerType.SEXP, newDepth))
            IonType.STRUCT -> {
                if (depth == 0 && userData._private_hasFirstAnnotation(SystemSymbols.ION_SYMBOL_TABLE_SID, SystemSymbols.ION_SYMBOL_TABLE)) {
                    throw IonException("User-defined symbol tables not permitted by the Ion 1.1 managed writer.")
                }
                userData.stepInStruct(options.writeLengthPrefix(ContainerType.STRUCT, newDepth))
            }
            else -> throw IllegalArgumentException("Not a container type: $containerType")
        }
    }

    override fun stepOut() = userData.stepOut()

    override fun isInStruct(): Boolean = userData.isInStruct()

    private inline fun <T> T?.writeMaybeNull(type: IonType, writeNotNull: (T) -> Unit) {
        if (this == null) {
            writeNull(type)
        } else {
            writeNotNull(this)
        }
    }

    override fun writeSymbol(content: String?) {
        if (content == null) {
            userData.writeNull(IonType.SYMBOL)
        } else {
            handleSymbolToken(UNKNOWN_SYMBOL_ID, content, SymbolKind.VALUE, userData)
        }
    }

    override fun writeSymbolToken(content: SymbolToken?) {
        if (content == null) {
            userData.writeNull(IonType.SYMBOL)
        } else {
            val text: String? = content.text
            if (content.sid == SystemSymbols.ION_1_0_SID) throw IonException("Can't write a top-level symbol that is the same as the IVM.")
            if (text == SystemSymbols.ION_1_0) throw IonException("Can't write a top-level symbol that is the same as the IVM.")
            handleSymbolToken(content.sid, content.text, SymbolKind.VALUE, userData)
        }
    }

    private inline fun IonRawWriter_1_1.write(kind: SymbolKind, sid: Int) = when (kind) {
        SymbolKind.VALUE -> writeSymbol(sid)
        SymbolKind.FIELD_NAME -> writeFieldName(sid)
        SymbolKind.ANNOTATION -> writeAnnotations(sid)
    }

    private inline fun IonRawWriter_1_1.write(kind: SymbolKind, text: String) = when (kind) {
        SymbolKind.VALUE -> writeSymbol(text)
        SymbolKind.FIELD_NAME -> writeFieldName(text)
        SymbolKind.ANNOTATION -> writeAnnotations(text)
    }

    /** Helper function that determines whether to write a symbol token as a SID or inline symbol */
    private inline fun handleSymbolToken(sid: Int, text: String?, kind: SymbolKind, rawWriter: IonRawWriter_1_1, preserveEncoding: Boolean = false) {
        if (text == null) {
            // No text. Decide whether to write $0 or some other SID
            if (sid == UNKNOWN_SYMBOL_ID) {
                // No (known) SID either.
                throw UnknownSymbolException("Cannot write a symbol token with unknown text and unknown SID.")
            } else {
                rawWriter.write(kind, sidTransformer?.transform(sid) ?: sid)
            }
        } else if (preserveEncoding && sid < 0) {
            rawWriter.write(kind, text)
        } else if (options.shouldWriteInline(kind, text)) {
            rawWriter.write(kind, text)
        } else {
            rawWriter.write(kind, intern(text))
        }
    }

    override fun writeNull() = userData.writeNull()
    override fun writeNull(type: IonType?) = userData.writeNull(type ?: IonType.NULL)
    override fun writeBool(value: Boolean) = userData.writeBool(value)
    override fun writeInt(value: Long) = userData.writeInt(value)

    override fun writeInt(value: BigInteger?) = value.writeMaybeNull(IonType.INT, userData::writeInt)
    override fun writeFloat(value: Double) = userData.writeFloat(value)
    override fun writeDecimal(value: BigDecimal?) = value.writeMaybeNull(IonType.DECIMAL, userData::writeDecimal)
    override fun writeTimestamp(value: Timestamp?) = value.writeMaybeNull(IonType.TIMESTAMP, userData::writeTimestamp)
    override fun writeString(value: String?) = value.writeMaybeNull(IonType.STRING, userData::writeString)

    override fun writeClob(value: ByteArray?) = value.writeMaybeNull(IonType.CLOB, userData::writeClob)
    override fun writeClob(value: ByteArray?, start: Int, len: Int) = value.writeMaybeNull(IonType.CLOB) { userData.writeClob(it, start, len) }

    override fun writeBlob(value: ByteArray?) = value.writeMaybeNull(IonType.BLOB, userData::writeBlob)
    override fun writeBlob(value: ByteArray?, start: Int, len: Int) = value.writeMaybeNull(IonType.BLOB) { userData.writeBlob(it, start, len) }

    override fun isFieldNameSet(): Boolean {
        return userData._private_hasFieldName()
    }

    override fun getDepth(): Int {
        return userData.depth()
    }

    override fun writeIonVersionMarker() {
        if (depth == 0) {
            // Make sure we write out any symbol tables and buffered values before the IVM
            finish()
            systemData.writeIVM()
        } else {
            writeSymbol("\$ion_1_1")
        }
    }

    @Deprecated("Use IonValue.writeTo(IonWriter) instead.")
    override fun writeValue(value: IonValue) = value.writeTo(this)

    @Deprecated("Use writeTimestamp instead.")
    override fun writeTimestampUTC(value: Date?) {
        TODO("Use writeTimestamp instead.")
    }

    override fun isStreamCopyOptimized(): Boolean = false

    override fun writeValues(reader: IonReader, symbolIdTransformer: IntTransformer) {
        sidTransformer = symbolIdTransformer
        try {
            writeValues(reader)
        } finally {
            sidTransformer = null
        }
    }

    override fun writeValues(reader: IonReader) {
        // There's a possibility that we could have interference between encoding contexts if we're transferring from a
        // system reader. However, this is the same behavior as the other implementations.

        val startingDepth = reader.depth
        while (true) {
            val nextType = reader.next()
            if (nextType == null) {
                // Nothing more *and* we're at the starting depth? We're all done.
                if (reader.depth == startingDepth) return
                // Otherwise, step out and continue.
                userData.stepOut()
                reader.stepOut()
            } else {
                transferScalarOrStepIn(reader, nextType)
            }
        }
    }

    override fun writeValue(reader: IonReader, symbolIdTransformer: IntTransformer) {
        sidTransformer = symbolIdTransformer
        try {
            writeValue(reader)
        } finally {
            sidTransformer = null
        }
    }

    override fun writeValue(reader: IonReader) {
        // There's a possibility that we could have interference between encoding contexts if we're transferring from a
        // system reader. However, this is the same behavior as the other implementations.

        if (reader.type == null) return
        val startingDepth = reader.depth
        transferScalarOrStepIn(reader, reader.type)
        if (reader.depth != startingDepth) {
            // We stepped into a container, so write the content of the container and then step out.
            writeValues(reader)
            reader.stepOut()
            userData.stepOut()
        }
    }

    /**
     * Can only be called when the reader is positioned on a value. Having [currentType] in the
     * function signature helps to enforce that requirement because [currentType] is not allowed
     * to be `null`.
     */
    private fun transferScalarOrStepIn(reader: IonReader, currentType: IonType) {
        // TODO: If the Ion 1.1 symbol table differs at all from the Ion 1.0 symbol table, and we're copying
        //       from Ion 1.0, we will have to adjust any SIDs that we are writing.

        reader.typeAnnotationSymbols.forEach {
            handleSymbolToken(it.sid, it.text, SymbolKind.ANNOTATION, userData, preserveEncoding = true)
        }
        if (isInStruct) {
            // TODO: Can't use reader.fieldId, reader.fieldName because it will throw UnknownSymbolException.
            //       However, this might mean we're unnecessarily constructing `SymbolToken` instances.
            val fieldName = reader.fieldNameSymbol
            handleSymbolToken(fieldName.sid, fieldName.text, SymbolKind.FIELD_NAME, userData, preserveEncoding = true)
        }

        if (reader.isNullValue) {
            userData.writeNull(currentType)
        } else when (currentType) {
            IonType.BOOL -> userData.writeBool(reader.booleanValue())
            IonType.INT -> {
                if (reader.integerSize == IntegerSize.BIG_INTEGER) {
                    userData.writeInt(reader.bigIntegerValue())
                } else {
                    userData.writeInt(reader.longValue())
                }
            }
            IonType.FLOAT -> userData.writeFloat(reader.doubleValue())
            IonType.DECIMAL -> userData.writeDecimal(reader.decimalValue())
            IonType.TIMESTAMP -> userData.writeTimestamp(reader.timestampValue())
            IonType.SYMBOL -> {
                if (reader.isCurrentValueAnIvm()) {
                    // TODO: What about the case where it's an IVM, but the writer is not at depth==0? Should we write
                    //       it as a symbol or just ignore it? (This can only happen if the writer is stepped in, but
                    //       the reader starts at depth==0.)

                    // Just in case—call finish to flush the current system values, then user values, and then write the IVM.
                    finish()
                    userData.writeIVM()
                } else {
                    val symbol = reader.symbolValue()
                    handleSymbolToken(symbol.sid, symbol.text, SymbolKind.VALUE, userData, preserveEncoding = true)
                }
            }
            IonType.STRING -> userData.writeString(reader.stringValue())
            IonType.CLOB -> userData.writeClob(reader.newBytes())
            IonType.BLOB -> userData.writeBlob(reader.newBytes())
            // TODO: See if we can preserve the encoding of containers (delimited vs length-prefixed)
            IonType.LIST -> {
                userData.stepInList(options.writeLengthPrefix(ContainerType.LIST, reader.depth))
                reader.stepIn()
            }
            IonType.SEXP -> {
                userData.stepInSExp(options.writeLengthPrefix(ContainerType.SEXP, reader.depth))
                reader.stepIn()
            }
            IonType.STRUCT -> {
                userData.stepInStruct(options.writeLengthPrefix(ContainerType.STRUCT, reader.depth))
                reader.stepIn()
            }
            else -> TODO("NULL and DATAGRAM are unreachable.")
        }
    }

    private fun IonReader.isCurrentValueAnIvm(): Boolean {
        if (depth != 0 || type != IonType.SYMBOL || typeAnnotationSymbols.isNotEmpty()) return false
        val symbol = symbolValue() ?: return false
        if (symbol.sid == 2) return true
        symbol.text ?: return false
        return ION_VERSION_MARKER_REGEX.matches(symbol.assumeText())
    }

    // Stream termination

    override fun close() {
        flush()
        systemData.close()
        userData.close()
        onClose()
    }

    override fun flush() {
        writeSymbolTable()
        systemData.flush()
        userData.flush()
    }

    override fun finish() {
        flush()
        resetLocalSymbolTable()
    }
}
