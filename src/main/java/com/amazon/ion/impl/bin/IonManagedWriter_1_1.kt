// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.*
import com.amazon.ion.impl.*
import com.amazon.ion.impl.bin.DelimitedContainerStrategy.*
import com.amazon.ion.impl.bin.SymbolInliningStrategy.*
import com.amazon.ion.system.*
import java.io.OutputStream
import java.math.BigDecimal
import java.math.BigInteger

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
) : _Private_IonManagedWriter, AbstractIonWriter(WriteValueOptimization.NONE) {

    init {
        // Since this is Ion 1.1, we must always start with the IVM.
        systemData.writeIVM()
    }

    companion object {
        @JvmStatic
        fun textWriter(output: OutputStream, managedWriterOptions: ManagedWriterOptions_1_1, textOptions: IonTextWriterBuilder): IonManagedWriter_1_1 {
            textOptions as _Private_IonTextWriterBuilder

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
        fun textWriter(output: Appendable, managedWriterOptions: ManagedWriterOptions_1_1, textOptions: IonTextWriterBuilder): IonManagedWriter_1_1 {
            textOptions as _Private_IonTextWriterBuilder

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
                    buffer = WriteBuffer(BlockAllocatorProviders.basicProvider().vendAllocator(binaryOptions.blockSize),) {},
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
    private var symbolTable: SymbolTable = Symbols.systemSymbolTable()
    /** Max symbol ID of the prior encoding context. */
    private var priorMaxId: Int = symbolTable.maxId
    /** Symbols to be interned since the prior encoding context. */
    private var newSymbols = arrayListOf<String>()

    private fun intern(text: String): Int {
        // Check the current symbol table
        var sid = symbolTable.findSymbol(text)
        if (sid != SymbolTable.UNKNOWN_SYMBOL_ID) return sid
        // Check the to-be-appended symbols
        sid = newSymbols.indexOf(text)
        if (sid != SymbolTable.UNKNOWN_SYMBOL_ID) return sid + priorMaxId + 1
        // Add to the to-be-appended symbols
        sid = priorMaxId + newSymbols.size + 1
        newSymbols.add(text)
        return sid
    }

    /** Writes a Local Symbol Table for the current encoding context, and updates the prior context. */
    private fun writeSymbolTable() {
        if (newSymbols.isEmpty()) return
        val useSid = options.internEncodingDirectiveSymbols

        with(systemData) {

            if (useSid) writeAnnotations(SystemSymbols.ION_SYMBOL_TABLE_SID) else writeAnnotations(SystemSymbols.ION_SYMBOL_TABLE)

            systemData.stepInStruct(delimited = false)
            if (canAppendToLocalSymbolTable) {
                // LST Append
                if (useSid) writeFieldName(SystemSymbols.IMPORTS_SID) else writeFieldName(SystemSymbols.IMPORTS)
                if (useSid) writeSymbol(SystemSymbols.ION_SYMBOL_TABLE_SID) else writeSymbol(SystemSymbols.ION_SYMBOL_TABLE)
            }
            // ... and write the new symbols
            if (useSid) writeFieldName(SystemSymbols.SYMBOLS_SID) else writeFieldName(SystemSymbols.SYMBOLS)
            stepInList(delimited = false)
            newSymbols.forEach { writeString(it) }
            stepOut()
            stepOut()
        }

        symbolTable = LocalSymbolTable.DEFAULT_LST_FACTORY.newLocalSymtab(symbolTable).apply {
            newSymbols.forEach { this@apply.intern(it) }
        }

        newSymbols.clear()
        canAppendToLocalSymbolTable = true
    }

    private fun resetLocalSymbolTable() {
        if (depth != 0) throw IllegalStateException("Cannot reset the encoding context while stepped in any value.")
        symbolTable = Symbols.systemSymbolTable()
        priorMaxId = symbolTable.maxId
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
        handleSymbolText(name, options.shouldWriteInline(SymbolKind.FIELD_NAME, name), userData::writeFieldName, userData::writeFieldName)
    }

    override fun setFieldNameSymbol(name: SymbolToken) {
        handleSymbolToken(name, options.shouldWriteInline(SymbolKind.FIELD_NAME, name), userData::writeFieldName, userData::writeFieldName)
    }

    override fun addTypeAnnotation(annotation: String) {
        if (annotation == SystemSymbols.ION_SYMBOL_TABLE && depth == 0) {
            throw IonException("User-defined symbol tables not permitted by the managed writer.")
        }
        handleSymbolText(annotation, options.shouldWriteInline(SymbolKind.ANNOTATION, annotation), userData::writeAnnotations, userData::writeAnnotations)
    }

    override fun setTypeAnnotations(annotations: Array<String>?) {
        // Interning happens in addTypeAnnotation
        userData._private_clearAnnotations()
        annotations?.forEach { addTypeAnnotation(it) }
    }

    override fun setTypeAnnotationSymbols(annotations: Array<SymbolToken>?) {
        userData._private_clearAnnotations()
        annotations?.forEachIndexed { i, it ->
            // TODO: This is handled inconsistently. If you add annotations one at a time using addTypeAnnotation,
            //       we don't know whether the $ion_symbol_table annotation is the first one or not.
            if (depth == 0 && i == 0) {
                if (it.sid == SystemSymbols.ION_SYMBOL_TABLE_SID || it.text == SystemSymbols.ION_SYMBOL_TABLE)
                    throw IonException("User-defined symbol tables not permitted by the managed writer.")
            }
            handleSymbolToken(it, options.shouldWriteInline(SymbolKind.ANNOTATION, it), userData::writeAnnotations, userData::writeAnnotations)
        }
    }

    override fun stepIn(containerType: IonType?) {
        val newDepth = depth + 1
        when (containerType) {
            IonType.LIST -> userData.stepInList(options.writeDelimited(ContainerType.LIST, newDepth))
            IonType.SEXP -> userData.stepInSExp(options.writeDelimited(ContainerType.SEXP, newDepth))
            IonType.STRUCT -> userData.stepInStruct(options.writeDelimited(ContainerType.STRUCT, newDepth))
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
            handleSymbolText(content, options.shouldWriteInline(SymbolKind.VALUE, content), userData::writeSymbol, userData::writeSymbol)
        }
    }

    override fun writeSymbolToken(content: SymbolToken?) {
        if (content == null) {
            userData.writeNull(IonType.SYMBOL)
        } else {
            val text: String? = content.text
            if (content.sid == SystemSymbols.ION_1_0_SID) throw IonException("Can't write a top-level symbol that is the same as the IVM.")
            if (text == SystemSymbols.ION_1_0) throw IonException("Can't write a top-level symbol that is the same as the IVM.")
            handleSymbolToken(content, options.shouldWriteInline(SymbolKind.VALUE, content), userData::writeSymbol, userData::writeSymbol)
        }
    }

    /** Helper function that determines whether to write a symbol token as a SID or inline symbol */
    private inline fun handleSymbolToken(sym: SymbolToken, inline: Boolean, writeSymbolText: (String) -> Unit, writeSymbolId: (Int) -> Unit) {
        val text: String? = sym.text
        if (text == null) {
            if (sym.sid < priorMaxId) {
                // It's in the system symbol table or local table but was constructed without the text for some reason.
                writeSymbolId(sym.sid)
            } else {
                // Unknown Local Symbol
                writeSymbolId(0)
            }
        } else {
            handleSymbolText(text, inline, writeSymbolText, writeSymbolId)
        }
    }

    private inline fun handleSymbolText(text: String, inline: Boolean, writeSymbolText: (String) -> Unit, writeSymbolId: (Int) -> Unit) {
        if (inline) {
            writeSymbolText(text)
        } else {
            writeSymbolId(intern(text))
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

    override fun writeString(data: ByteArray?, offset: Int, length: Int) = data.writeMaybeNull(IonType.STRING) { bytes ->
        // TODO: We should probably plumb this through to the Ion 1.1 raw writer rather than decoding it here
        userData.writeString(bytes.decodeToString(offset, length + offset, throwOnInvalidSequence = true))
    }

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
        // Make sure we write out any symbol tables and buffered values before the IVM
        flush()
        userData.writeIVM()
    }

    override fun writeBytes(data: ByteArray?, off: Int, len: Int) {
        TODO("Not implemented. Is this actually needed?")
    }

    override fun getRawWriter(): _Private_IonRawWriter = TODO("Not yet implemented")

    override fun requireLocalSymbolTable() {
        // Can this be a no-op?
        TODO("Not yet implemented")
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
        // TODO: This method should probably be called `flush()` instead of `finish()`.
        systemData.finish()
        userData.finish()
    }

    override fun finish() {
        flush()
        resetLocalSymbolTable()
    }
}
