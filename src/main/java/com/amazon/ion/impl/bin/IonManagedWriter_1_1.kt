// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.*
import com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID
import com.amazon.ion.impl.*
import com.amazon.ion.impl._Private_IonWriter.IntTransformer
import com.amazon.ion.impl.bin.LengthPrefixStrategy.*
import com.amazon.ion.impl.bin.SymbolInliningStrategy.*
import com.amazon.ion.impl.macro.*
import com.amazon.ion.system.*
import java.io.OutputStream
import java.math.BigDecimal
import java.math.BigInteger
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.LinkedHashMap

/**
 * A managed writer for Ion 1.1 that is generic over whether the raw encoding is text or binary.
 *
 * TODO:
 *  - Handling of shared symbol tables
 *  - Proper handling of user-supplied symbol tables
 *  - Auto-flush (for binary and text)
 *
 * TODO: What package does this really belong in?
 *
 * See also [ManagedWriterOptions_1_1], [SymbolInliningStrategy], and [LengthPrefixStrategy].
 */
internal class IonManagedWriter_1_1(
    private val userData: IonRawWriter_1_1,
    private val systemData: IonRawWriter_1_1,
    private val options: ManagedWriterOptions_1_1,
    private val onClose: () -> Unit,
) : _Private_IonWriter, MacroAwareIonWriter {

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

    /** The symbol table for the prior encoding context */
    private var symbolTable: HashMap<String, Int> = HashMap(systemSymbolTableMap)
    /** Symbols to be interned since the prior encoding context. */
    private var newSymbols: HashMap<String, Int> = LinkedHashMap() // Preserves insertion order.

    /** The macro table of the prior encoding context. Map value is the user-space address. */
    private var macroTable: HashMap<Macro, Int> = LinkedHashMap()
    /** Macros to be added since the last encoding directive was flushed. Map value is the user-space address. */
    private var newMacros: HashMap<Macro, Int> = LinkedHashMap()
    /** Macro names by user-space address, including new macros. */
    private var macroNames = ArrayList<String?>()
    /** Macro definitions by user-space address, including new macros. */
    private var macrosById = ArrayList<Macro>()

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
        sid = symbolTable.size + newSymbols.size + 1
        newSymbols[text] = sid
        return sid
    }

    private fun internMacro(macro: Macro): Int {
        // Check the current macro table
        var id = macroTable[macro]
        if (id != null) return id
        // Check the to-be-appended macros
        id = newMacros[macro]
        if (id != null) return id
        // Add to the to-be-appended symbols
        id = macrosById.size
        macrosById.add(macro)
        macroNames.add(null)
        newMacros[macro] = id
        return id
    }

    /** Converts a named macro reference to an address */
    private fun MacroRef.ByName.intoId(): MacroRef.ById = MacroRef.ById(macroNames.indexOf(name))

    /** Converts a macro address to a macro name. If no name is found, returns the original address. */
    private fun MacroRef.ById.intoNamed(): MacroRef = macroNames[id]?.let { MacroRef.ByName(it) } ?: this

    private fun resetEncodingContext() {
        if (depth != 0) throw IllegalStateException("Cannot reset the encoding context while stepped in any value.")
        symbolTable = HashMap(systemSymbolTableMap)
        macroNames.clear()
        macrosById.clear()
        macroTable.clear()
        newMacros.clear()
    }

    /** Helper function for writing encoding directives */
    private inline fun writeSystemSexp(content: IonRawWriter_1_1.() -> Unit) {
        systemData.stepInSExp(usingLengthPrefix = false)
        systemData.content()
        systemData.stepOut()
    }

    /**
     * Writes an encoding directive for the current encoding context, and updates internal state accordingly.
     * This always appends to the current encoding context. If there is nothing to append, calling this function
     * is a no-op.
     */
    private fun writeEncodingDirective() {
        if (newSymbols.isEmpty() && newMacros.isEmpty()) return

        systemData.writeAnnotations(SystemSymbols.ION_ENCODING)
        writeSystemSexp {
            writeSymbolTableClause()
            writeMacroTableClause()
        }

        // NOTE: We don't update symbolTable until after the macro_table is written because
        //       the new symbols aren't available until _after_ this encoding directive.
        symbolTable.putAll(newSymbols)
        newSymbols.clear()
        macroTable.putAll(newMacros)
        newMacros.clear()
    }

    /**
     * Writes the `(symbol_table ...)` clause into the encoding expression.
     * If the symbol table would be empty, writes nothing, which is equivalent
     * to an empty symbol table.
     */
    private fun writeSymbolTableClause() {
        val hasSymbolsToAdd = newSymbols.isNotEmpty()
        val hasSymbolsToRetain = symbolTable.size > SystemSymbols.ION_1_0_MAX_ID
        if (!hasSymbolsToAdd && !hasSymbolsToRetain) return

        writeSystemSexp {
            systemData.writeSymbol(SystemSymbols.SYMBOL_TABLE)
            // Add previous symbol table
            if (hasSymbolsToRetain) {
                writeSymbol(SystemSymbols.ION_ENCODING)
            }
            // Add new symbols
            if (hasSymbolsToAdd) {
                stepInList(usingLengthPrefix = false)
                newSymbols.forEach { (text, _) -> writeString(text) }
                stepOut()
            }
        }
    }

    /**
     * Writes the `(macro_table ...)` clause into the encoding expression.
     * If the macro table would be empty, writes nothing, which is equivalent
     * to an empty macro table.
     */
    private fun writeMacroTableClause() {
        val hasMacrosToAdd = newMacros.isNotEmpty()
        val hasMacrosToRetain = macroTable.isNotEmpty()
        if (!hasMacrosToAdd && !hasMacrosToRetain) return

        writeSystemSexp {
            writeSymbol(SystemSymbols.MACRO_TABLE)
            if (hasMacrosToRetain) {
                writeSymbol(SystemSymbols.ION_ENCODING)
            }
            newMacros.forEach { (macro, address) ->
                val name = macroNames[address]
                when (macro) {
                    is TemplateMacro -> writeMacroDefinition(name, macro)
                    is SystemMacro -> exportSystemMacro(macro)
                }
            }
        }
    }

    private fun exportSystemMacro(macro: SystemMacro) {
        // TODO: Support for aliases
        writeSystemSexp {
            writeSymbol(SystemSymbols.EXPORT)
            writeAnnotations(SystemSymbols.ION)
            writeSymbol(macro.macroName)
        }
    }

    private fun writeMacroDefinition(name: String?, macro: TemplateMacro) {
        writeSystemSexp {
            writeSymbol(SystemSymbols.MACRO)
            if (name != null) writeSymbol(name) else writeNull()

            // Signature
            writeSystemSexp {
                macro.signature.forEach { parameter ->
                    if (parameter.type != Macro.ParameterEncoding.Tagged) {
                        writeAnnotations(parameter.type.ionTextName)
                    }
                    writeSymbol(parameter.variableName)
                    if (parameter.cardinality != Macro.ParameterCardinality.ExactlyOne) {
                        // TODO: Consider adding a method to the raw writer that can write a single-character
                        //       symbol without constructing a string. It might be a minor performance improvement.
                        // TODO: See if we can write this without a space between the parameter name and the sigil.
                        writeSymbol(parameter.cardinality.sigil.toString())
                    }
                }
            }

            // Template Body

            // TODO: See if there's any benefit to using a smaller number type, if we can
            //       memoize this in the macro definition, or replace it with a list of precomputed
            //       step-out indices.
            /** Tracks where and how many times to step out. */
            val numberOfTimesToStepOut = IntArray(macro.body.size + 1)

            macro.body.forEachIndexed { index, expression ->
                if (numberOfTimesToStepOut[index] > 0) {
                    repeat(numberOfTimesToStepOut[index]) { stepOut() }
                }

                when (expression) {
                    is Expression.DataModelValue -> {
                        // If we need to write a sexp, we must use the annotate macro.
                        // If we're writing a symbol, we can put them inside the "literal" special form.
                        if (expression.type != IonType.SEXP && expression.type != IonType.SYMBOL) {
                            expression.annotations.forEach {
                                if (it.text != null) {
                                    // TODO: If it's already in the symbol table we could check the
                                    //       symbol-inline strategy and possibly write a SID.
                                    writeAnnotations(it.text)
                                } else {
                                    writeAnnotations(it.sid)
                                }
                            }
                        }

                        if (expression is Expression.NullValue) {
                            writeNull(expression.type)
                        } else when (expression.type) {
                            IonType.NULL -> error("Unreachable")
                            IonType.BOOL -> writeBool((expression as Expression.BoolValue).value)
                            IonType.INT -> {
                                if (expression is Expression.LongIntValue)
                                    writeInt(expression.value)
                                else
                                    writeInt((expression as Expression.BigIntValue).value)
                            }
                            IonType.FLOAT -> writeFloat((expression as Expression.FloatValue).value)
                            IonType.DECIMAL -> writeDecimal((expression as Expression.DecimalValue).value)
                            IonType.TIMESTAMP -> writeTimestamp((expression as Expression.TimestampValue).value)
                            IonType.SYMBOL -> {
                                writeSystemSexp {
                                    writeSymbol(SystemSymbols.LITERAL)
                                    expression.annotations.forEach {
                                        if (it.text != null) {
                                            // TODO: If it's already in the symbol table we could check the
                                            //       symbol-inline strategy and possibly write a SID.
                                            writeAnnotations(it.text)
                                        } else {
                                            writeAnnotations(it.sid)
                                        }
                                    }
                                    val symbolToken = (expression as Expression.SymbolValue).value
                                    if (symbolToken.text != null) {
                                        // TODO: If it's already in the symbol table we could check the
                                        //       symbol-inline strategy and possibly write a SID.
                                        writeSymbol(symbolToken.text)
                                    } else {
                                        writeSymbol(symbolToken.sid)
                                    }
                                }
                            }
                            IonType.STRING -> writeString((expression as Expression.StringValue).value)
                            IonType.CLOB -> writeClob((expression as Expression.ClobValue).value)
                            IonType.BLOB -> writeBlob((expression as Expression.BlobValue).value)
                            IonType.LIST -> {
                                expression as Expression.HasStartAndEnd
                                stepInList(usingLengthPrefix = false)
                                numberOfTimesToStepOut[expression.endExclusive]++
                            }
                            IonType.SEXP -> {
                                expression as Expression.HasStartAndEnd
                                if (expression.annotations.isNotEmpty()) {
                                    stepInSExp(usingLengthPrefix = false)
                                    numberOfTimesToStepOut[expression.endExclusive]++
                                    writeSymbol(SystemSymbols.ANNOTATE)

                                    // Write the annotations as symbols within an expression group
                                    writeSystemSexp {
                                        writeSymbol(SystemSymbols.TDL_EXPRESSION_GROUP)
                                        expression.annotations.forEach {
                                            if (it.text != null) {
                                                // TODO: If it's already in the symbol table we could check the
                                                //       symbol-inline strategy and possibly write a SID.

                                                // Write the annotation as a string so that we don't have to use
                                                // `literal` to prevent it from being interpreted as a variable
                                                writeString(it.text)
                                            } else {
                                                // TODO: See if there is a less verbose way to use SIDs in TDL
                                                writeSystemSexp {
                                                    writeSymbol(SystemSymbols.LITERAL)
                                                    writeSymbol(it.sid)
                                                }
                                            }
                                        }
                                    }
                                }
                                // Start a `(make_sexp [ ...` invocation
                                stepInSExp(usingLengthPrefix = false)
                                numberOfTimesToStepOut[expression.endExclusive]++
                                writeSymbol(SystemSymbols.MAKE_SEXP)

                                if (expression.startInclusive != expression.endExclusive) {
                                    stepInList(usingLengthPrefix = false)
                                    numberOfTimesToStepOut[expression.endExclusive]++
                                }
                            }
                            IonType.STRUCT -> {
                                expression as Expression.HasStartAndEnd
                                stepInStruct(usingLengthPrefix = false)
                                numberOfTimesToStepOut[expression.endExclusive]++
                            }
                            IonType.DATAGRAM -> error("Unreachable")
                        }
                    }
                    is Expression.FieldName -> {
                        val text = expression.value.text
                        if (text == null) {
                            writeFieldName(expression.value.sid)
                        } else {
                            // TODO: If it's already in the symbol table we could check the symbol-inline strategy and possibly write a SID.
                            writeFieldName(text)
                        }
                    }
                    is Expression.ExpressionGroup -> {
                        stepInSExp(usingLengthPrefix = false)
                        numberOfTimesToStepOut[expression.endExclusive]++
                        writeSymbol(SystemSymbols.TDL_EXPRESSION_GROUP)
                    }
                    is Expression.MacroInvocation -> {
                        stepInSExp(usingLengthPrefix = false)
                        when (expression.address) {
                            is MacroRef.ById -> writeInt(expression.address.id.toLong())
                            is MacroRef.ByName -> writeSymbol(expression.address.name)
                        }
                        numberOfTimesToStepOut[expression.endExclusive]++
                    }
                    is Expression.VariableRef -> writeSymbol(macro.signature[expression.signatureIndex].variableName)
                    else -> error("Unreachable")
                }
            }

            // Step out for anything where endExclusive is beyond the end of the expression list.
            repeat(numberOfTimesToStepOut.last()) { stepOut() }
        }
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

    override fun writeObject(obj: WriteAsIon) {
        obj.writeToMacroAware(this)
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
        writeEncodingDirective()
        systemData.flush()
        userData.flush()
    }

    override fun finish() {
        flush()
        resetEncodingContext()
    }

    override fun addMacro(macro: Macro): MacroRef {
        val id = internMacro(macro)
        return if (options.eExpressionIdentifierStrategy == ManagedWriterOptions_1_1.EExpressionIdentifierStrategy.BY_NAME) {
            macroNames[id]
                ?.let { MacroRef.ByName(it) }
                ?: MacroRef.ById(id)
        } else {
            MacroRef.ById(id)
        }
    }

    override fun addMacro(name: String, macro: Macro): MacroRef {
        val id = internMacro(macro)
        macroNames[id] = name
        return if (options.eExpressionIdentifierStrategy == ManagedWriterOptions_1_1.EExpressionIdentifierStrategy.BY_NAME) {
            MacroRef.ByName(name)
        } else {
            MacroRef.ById(id)
        }
    }

    override fun startMacro(macroRef: MacroRef) {
        val useNames = options.eExpressionIdentifierStrategy == ManagedWriterOptions_1_1.EExpressionIdentifierStrategy.BY_NAME
        val ref = when (macroRef) {
            is MacroRef.ById -> if (useNames) macroRef.intoNamed() else macroRef
            is MacroRef.ByName -> if (useNames) macroRef else macroRef.intoId()
        }
        when (ref) {
            is MacroRef.ById -> userData.stepInEExp(ref.id, options.writeLengthPrefix(ContainerType.EEXP, depth + 1), macrosById[ref.id])
            is MacroRef.ByName -> userData.stepInEExp(ref.name)
        }
    }

    override fun startMacro(macro: Macro) {
        val id = addMacro(macro)
        startMacro(id)
    }

    override fun startExpressionGroup() {
        userData.stepInExpressionGroup(options.writeLengthPrefix(ContainerType.EXPRESSION_GROUP, depth + 1))
    }

    override fun endMacro() {
        userData.stepOut()
    }

    override fun endExpressionGroup() {
        userData.stepOut()
    }
}
