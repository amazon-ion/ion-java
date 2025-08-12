// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.*
import com.amazon.ion.SymbolTable.*
import com.amazon.ion.eexp.*
import com.amazon.ion.impl.*
import com.amazon.ion.impl._Private_IonWriter.*
import com.amazon.ion.impl.bin.LengthPrefixStrategy.*
import com.amazon.ion.impl.bin.SymbolInliningStrategy.*
import com.amazon.ion.impl.macro.*
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
 *
 * See also [ManagedWriterOptions_1_1], [SymbolInliningStrategy], and [LengthPrefixStrategy].
 */
class IonManagedWriter_1_1 private constructor(
    private val userData: IonRawWriter_1_1,
    private val systemData: PrivateIonRawWriter_1_1,
    private val options: ManagedWriterOptions_1_1,
    private val onClose: () -> Unit,
) : _Private_IonWriter, MacroAwareIonWriter {

    internal fun getRawUserWriter(): IonRawWriter_1_1 = userData

    companion object {
        private val ION_VERSION_MARKER_REGEX = Regex("^\\\$ion_\\d+_\\d+$")

        // These are chosen subjectively to be neither too big nor too small.
        private const val MAX_PARAMETERS_IN_ONE_LINE_SIGNATURE = 4
        private const val MAX_SYMBOLS_IN_SINGLE_LINE_SYMBOL_TABLE = 10
        private const val MAX_EXPRESSIONS_IN_SINGLE_LINE_MACRO_BODY = 8

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

    // Since this is Ion 1.1, we must always start with the IVM.
    private var needsIVM: Boolean = true

    // We take a slightly different approach here by handling the encoding context as a prior encoding context
    // plus a list of symbols added by the current encoding context.
    /** The symbol table for the prior encoding context */
    private var symbolTable: HashMap<String, Int> = HashMap()

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
    /** The first symbol ID in the current encoding context. */
    private var firstLocalSid: Int = 0
    /** True if the current encoding context contains the system symbols. */
    private var areSystemSymbolsInScope = true

    /**
     * Transformer for symbol IDs encountered during writeValues. Can be used to upgrade Ion 1.0 symbol IDs to the
     * Ion 1.1 equivalents.
     */
    private var sidTransformer: IntTransformer? = null

    /**
     * Adds a new symbol to the table for this writer, or finds an existing definition of it. This writer does not
     * implement [IonWriter.getSymbolTable], so this method supplies some of that functionality.
     *
     * @return an SID for the given symbol text
     * @see SymbolTable.intern
     */
    fun intern(text: String): Int {
        // Check the current symbol table
        var sid = symbolTable[text]
        if (sid != null) return sid
        // Check the to-be-appended symbols
        sid = newSymbols[text]
        if (sid != null) return sid
        // Add to the to-be-appended symbols
        sid = firstLocalSid + symbolTable.size + newSymbols.size + 1
        newSymbols[text] = sid
        return sid
    }

    /**
     * Checks for macro invocations in the body of a TemplateMacro and ensure that those macros are added to the
     * macro table.
     *
     * This is essentially a recursive, memoized, topological sort. Given a dependency graph, it runs in O(V + E) time.
     * Memoization is done using the macro table, so the O(V + E) cost is only paid the first time a macro is added to
     * the macro table.
     */
    private fun addMacroDependencies(macro: Macro) {
        macro.dependencies.forEach {
            if (it !is SystemMacro && it !in macroTable && it !in newMacros) {
                addMacroDependencies(it)
                assignMacroAddress(it)
            }
        }
    }

    /**
     * Adds a named macro to the macro table
     *
     * Steps:
     * - If the name is not already in use...
     *    - And the macro is already in `newMacros`...
     *      1. Get the address of the macro in `newMacros`
     *      2. Add the name to `macroNames` for the that address
     *      3. return the address
     *    - Else...
     *      1. Add a new entry for the macro to `newMacros` and get a new address
     *      2. Add the name to `macroNames` for the new address
     *      3. Return the new address
     * - If the name is already in use...
     *   - And it is associated with the same macro...
     *      1. Return the address associated with the name
     *   - And it is associated with a different macro...
     *      - This is where the managed writer take an opinion. (Or be configurable.)
     *        - It could mangle the name
     *        - It could remove the name from a macro in macroTable, but then it would have to immediately flush to
     *          make sure that any prior e-expressions are still valid. In addition, we would need to re-export all
     *          the other macros from `_` (the default module).
     *        - For now, we're just throwing an Exception.
     *
     * Visible for testing.
     */
    internal fun getOrAssignMacroAddressAndName(name: String, macro: Macro): Int {
        // TODO: This is O(n), but could be O(1).
        var existingAddress = macroNames.indexOf(name)
        if (existingAddress < 0) {
            // Name is not already in use
            existingAddress = newMacros.getOrDefault(macro, -1)

            val address = if (existingAddress < 0) {
                // Macro is not in newMacros

                // If it's in macroTable, we can skip adding dependencies
                if (macro !in macroTable) addMacroDependencies(macro)
                // Add to newMacros and get a macro address
                assignMacroAddress(macro)
            } else {
                // Macro already exists in newMacros, but doesn't have a name
                existingAddress
            }
            // Set the name of the macro
            macroNames[address] = name
            return address
        } else if (macrosById[existingAddress] == macro) {
            // Macro already in table, and already using the same name
            return existingAddress
        } else {
            // Name is already in use for a different macro.
            // This macro may or may not be in the table under a different name, but that's
            // not particularly relevant unless we want to try to fall back to a different name.
            TODO("Name shadowing is not supported yet. Call finish() before attempting to shadow an existing macro.")
        }
    }

    /**
     * Steps for adding an anonymous macro to the macro table
     *    1. Check macroTable, if found, return that address
     *    2. Check newMacros, if found, return that address
     *    3. Add to newMacros, return new address
     *
     *  Visible for testing
     */
    internal fun getOrAssignMacroAddress(macro: Macro): Int {
        var address = macroTable.getOrDefault(macro, -1)
        if (address >= 0) return address
        address = newMacros.getOrDefault(macro, -1)
        if (address >= 0) return address

        addMacroDependencies(macro)
        return assignMacroAddress(macro)
    }

    override fun startEncodingSegmentWithIonVersionMarker() {
        if (!newSymbols.isEmpty() || !newMacros.isEmpty()) {
            throw IonException("Cannot start a new encoding segment while the previous segment is active.")
        }
        needsIVM = false
        flush()
        systemData.writeIVM()
        resetEncodingContext()
    }

    override fun startEncodingSegmentWithEncodingDirective(
        macros: Map<MacroRef, Macro>,
        isMacroTableAppend: Boolean,
        symbols: List<String>,
        isSymbolTableAppend: Boolean,
        encodingDirectiveAlreadyWritten: Boolean
    ) {
        // It is assumed that the IVM is written manually when using endEncodingSegment.
        needsIVM = false
        // First, flush the previous segment. This method begins a new segment.
        flush()
        firstLocalSid = if (isSymbolTableAppend) {
            if (areSystemSymbolsInScope) SystemSymbols_1_1.size() else 0
        } else {
            symbolTable.clear()
            areSystemSymbolsInScope = false
            0
        }
        for (symbol in symbols) {
            intern(symbol)
        }
        if (!isMacroTableAppend) {
            macroNames.clear()
            macrosById.clear()
            macroTable.clear()
            newMacros.clear()
        }
        for ((macroRef, macro) in macros.entries) {
            if (macroRef.hasName()) {
                getOrAssignMacroAddressAndName(macroRef.name!!, macro)
            } else {
                getOrAssignMacroAddress(macro)
            }
        }
        if (encodingDirectiveAlreadyWritten) {
            // This prevents another encoding directive from being written for this context.
            symbolTable.putAll(newSymbols)
            newSymbols.clear()
            macroTable.putAll(newMacros)
            newMacros.clear()
        } else {
            writeVerboseEncodingDirective()
        }
    }

    /** Unconditionally adds a macro to the macro table data structures and returns the new address. */
    private fun assignMacroAddress(macro: Macro): Int {
        val address = macrosById.size
        macrosById.add(macro)
        macroNames.add(null)
        newMacros[macro] = address
        return address
    }

    // Only called by `finish()`
    private fun resetEncodingContext() {
        if (depth != 0) throw IllegalStateException("Cannot reset the encoding context while stepped in any value.")
        symbolTable.clear()
        macroNames.clear()
        macrosById.clear()
        macroTable.clear()
        newMacros.clear()

        needsIVM = true
        firstLocalSid = 0
        areSystemSymbolsInScope = true
    }

    /** Helper function for writing encoding directives */
    private inline fun writeSystemSexp(content: PrivateIonRawWriter_1_1.() -> Unit) {
        systemData.stepInSExp(usingLengthPrefix = false)
        systemData.content()
        systemData.stepOut()
    }

    /** Helper function for writing encoding directives */
    private inline fun writeSystemMacro(macro: SystemMacro, content: PrivateIonRawWriter_1_1.() -> Unit) {
        systemData.stepInEExp(macro)
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

        writeSymbolTableClause()
        symbolTable.putAll(newSymbols)
        newSymbols.clear()
        // NOTE: Once we have emitted the symbol table update with set/add_symbols those symbols become available
        //       for use in set/add_macros (if relevant)

        writeMacroTableClause()
        macroTable.putAll(newMacros)
        newMacros.clear()
    }

    /**
     * Writes an encoding directive for the current encoding context using the verbose `$ion::(module _ ...)` syntax,
     * and updates internal state accordingly. This always appends to the current encoding context. If there is nothing
     * to append, calling this function is a no-op.
     */
    private fun writeVerboseEncodingDirective() {
        if (newSymbols.isEmpty() && newMacros.isEmpty()) return

        systemData.writeAnnotations(SystemSymbols_1_1.ION)
        writeSystemSexp {
            writeSymbol(SystemSymbols_1_1.MODULE)
            writeSymbol(SystemSymbols.DEFAULT_MODULE)
            writeVerboseMacroTableClause()
            writeVerboseSymbolTableClause()
        }
        symbolTable.putAll(newSymbols)
        newSymbols.clear()
        macroTable.putAll(newMacros)
        newMacros.clear()
    }

    /**
     * Updates the symbols in the encoding context by invoking
     * the `add_symbols` or `set_symbols` system macro.
     * If the symbol table would be empty, writes nothing, which is equivalent
     * to an empty symbol table.
     */
    private fun writeSymbolTableClause() {
        val hasSymbolsToAdd = newSymbols.isNotEmpty()
        val hasSymbolsToRetain = symbolTable.isNotEmpty()
        if (!hasSymbolsToAdd) return

        val macro = if (!hasSymbolsToRetain) SystemMacro.SetSymbols else SystemMacro.AddSymbols

        // Add new symbols
        writeSystemMacro(macro) {
            stepInExpressionGroup(usingLengthPrefix = false)
            if (newSymbols.size <= MAX_SYMBOLS_IN_SINGLE_LINE_SYMBOL_TABLE) forceNoNewlines(true)
            newSymbols.forEach { (text, _) -> writeString(text) }
            stepOut()
        }
        systemData.forceNoNewlines(false)
    }

    /**
     * Writes the `(symbol_table ...)` clause into the encoding expression using the
     * verbose s-expression syntax.
     * If the symbol table would be empty, writes nothing, which is equivalent
     * to an empty symbol table.
     */
    private fun writeVerboseSymbolTableClause() {
        val hasSymbolsToAdd = newSymbols.isNotEmpty()
        val hasSymbolsToRetain = symbolTable.isNotEmpty()
        if (!hasSymbolsToAdd && !hasSymbolsToRetain) return

        writeSystemSexp {
            forceNoNewlines(true)
            systemData.writeSymbol(SystemSymbols_1_1.SYMBOLS)

            // Add previous symbol table
            if (hasSymbolsToRetain) {
                if (newSymbols.size > 0) forceNoNewlines(false)
                writeSymbol(SystemSymbols.DEFAULT_MODULE)
            }

            // Add new symbols
            if (hasSymbolsToAdd) {
                stepInList(usingLengthPrefix = false)
                if (newSymbols.size <= MAX_SYMBOLS_IN_SINGLE_LINE_SYMBOL_TABLE) forceNoNewlines(true)
                newSymbols.forEach { (text, _) -> writeString(text) }
                stepOut()
            }
            forceNoNewlines(true)
        }
        systemData.forceNoNewlines(false)
    }

    /**
     * Adds macros to the encoding context by invoking
     * the `add_macros` or `set_macros` system macro.
     * If the macro table would be empty, writes nothing, which is equivalent
     * to an empty macro table.
     */
    private fun writeMacroTableClause() {
        val hasMacrosToAdd = newMacros.isNotEmpty()
        val hasMacrosToRetain = macroTable.isNotEmpty()
        if (!hasMacrosToAdd) return

        val macro = if (!hasMacrosToRetain) SystemMacro.SetMacros else SystemMacro.AddMacros

        writeSystemMacro(macro) {
            forceNoNewlines(false)
            stepInExpressionGroup(usingLengthPrefix = false)
            newMacros.forEach { (macro, address) ->
                val name = macroNames[address]
                when (macro) {
                    is TemplateMacro -> writeMacroDefinition(name, macro)
                    is SystemMacro -> {
                        if (name != macro.macroName) {
                            exportSystemMacro(macro, name)
                        }
                        // Else, no need to export the macro since it's already known by the desired name
                    }
                }
            }
            stepOut()
        }
        systemData.forceNoNewlines(false)
    }

    /**
     * Writes the `(macro_table ...)` clause into the encoding expression using the
     * verbose s-expression syntax.
     * If the macro table would be empty, writes nothing, which is equivalent
     * to an empty macro table.
     */
    private fun writeVerboseMacroTableClause() {
        val hasMacrosToAdd = newMacros.isNotEmpty()
        val hasMacrosToRetain = macroTable.isNotEmpty()
        if (!hasMacrosToAdd && !hasMacrosToRetain) return

        writeSystemSexp {
            forceNoNewlines(true)
            writeSymbol(SystemSymbols_1_1.MACROS)
            if (newMacros.size > 0) forceNoNewlines(false)
            if (hasMacrosToRetain) {
                writeSymbol(SystemSymbols.DEFAULT_MODULE)
            }
            forceNoNewlines(false)
            newMacros.forEach { (macro, address) ->
                val name = macroNames[address]
                when (macro) {
                    is TemplateMacro -> writeMacroDefinition(name, macro)
                    is SystemMacro -> {
                        if (name != macro.macroName) {
                            exportSystemMacro(macro, name)
                        }
                        // Else, no need to export the macro since it's already known by the desired name
                    }
                }
            }
            forceNoNewlines(true)
        }
        systemData.forceNoNewlines(false)
    }

    private fun exportSystemMacro(macro: SystemMacro, alias: String?) {
        writeSystemSexp {
            forceNoNewlines(true)
            writeSymbol(SystemSymbols_1_1.EXPORT)
            writeAnnotations(SystemSymbols_1_1.ION)
            writeSymbol(macro.macroName)
            if (alias != null && alias != macro.macroName) {
                writeSymbol(alias)
            }
        }
        systemData.forceNoNewlines(false)
    }

    private fun writeMacroDefinition(name: String?, macro: TemplateMacro) {
        writeSystemSexp {
            forceNoNewlines(true)
            writeSymbol(SystemSymbols_1_1.MACRO)
            if (name != null) writeSymbol(name) else writeNull()

            if (macro.signature.size > MAX_PARAMETERS_IN_ONE_LINE_SIGNATURE) forceNoNewlines(false)

            // Signature
            writeSystemSexp {
                macro.signature.forEach { parameter ->
                    if (parameter.type != Macro.ParameterEncoding.Tagged) {
                        writeAnnotations(parameter.type.ionTextName)
                    }
                    writeSymbol(parameter.variableName)
                    if (parameter.cardinality != Macro.ParameterCardinality.ExactlyOne) {
                        writeMacroParameterCardinality(parameter.cardinality)
                    }
                }
            }

            if (macro.body.size > MAX_EXPRESSIONS_IN_SINGLE_LINE_MACRO_BODY) forceNoNewlines(false)

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
                        expression.annotations.forEach {
                            if (it.text != null) {
                                // TODO: If it's already in the symbol table we could check the
                                //       symbol-inline strategy and possibly write a SID.
                                writeAnnotations(it.text)
                            } else {
                                writeAnnotations(it.sid)
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
                                val symbolToken = (expression as Expression.SymbolValue).value
                                if (symbolToken.text != null) {
                                    // TODO: If it's already in the symbol table we could check the
                                    //       symbol-inline strategy and possibly write a SID.
                                    writeSymbol(symbolToken.text)
                                } else {
                                    writeSymbol(symbolToken.sid)
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
                                stepInSExp(usingLengthPrefix = false)
                                numberOfTimesToStepOut[expression.endExclusive]++
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
                        stepInTdlExpressionGroup()
                        numberOfTimesToStepOut[expression.endExclusive]++
                    }
                    is Expression.MacroInvocation -> {
                        val invokedMacro = expression.macro
                        if (invokedMacro is SystemMacro) {
                            stepInTdlSystemMacroInvocation(invokedMacro.systemSymbol)
                        } else {
                            val invokedAddress = macroTable[invokedMacro]
                                ?: newMacros[invokedMacro]
                                ?: throw IllegalStateException("A macro in the macro table is missing a dependency")
                            val invokedName = macroNames[invokedAddress]
                            if (options.invokeTdlMacrosByName && invokedName != null) {
                                stepInTdlMacroInvocation(invokedName)
                            } else {
                                stepInTdlMacroInvocation(invokedAddress)
                            }
                        }
                        numberOfTimesToStepOut[expression.endExclusive]++
                    }
                    is Expression.VariableRef -> writeTdlVariableExpansion(macro.signature[expression.signatureIndex].variableName)
                    else -> error("Unreachable")
                }
            }

            // Step out for anything where endExclusive is beyond the end of the expression list.
            repeat(numberOfTimesToStepOut.last()) { stepOut() }
            forceNoNewlines(true)
        }
        systemData.forceNoNewlines(false)
    }

    override fun getCatalog(): IonCatalog {
        TODO("Not part of the public API.")
    }

    /** No facets supported */
    override fun <T : Any?> asFacet(facetType: Class<T>?): T? = null

    override fun getSymbolTable(): SymbolTable {
        TODO("Why do we need to expose this to users in the first place?")
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
                if (depth == 0 && userData._private_hasFirstAnnotation(SystemSymbols_1_1.ION_SYMBOL_TABLE.id, SystemSymbols_1_1.ION_SYMBOL_TABLE.text)) {
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
            // TODO: Check to see if the SID refers to a user symbol with text that looks like an IVM
            if (text == SystemSymbols_1_1.ION_1_0.text && depth == 0) throw IonException("Can't write a top-level symbol that is the same as the IVM.")
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

    private inline fun IonRawWriter_1_1.write(kind: SymbolKind, symbol: SystemSymbols_1_1) = when (kind) {
        SymbolKind.VALUE -> writeSymbol(symbol)
        SymbolKind.FIELD_NAME -> writeFieldName(symbol)
        SymbolKind.ANNOTATION -> writeAnnotations(symbol)
    }

    /** Helper function that determines whether to write a symbol token as a SID or inline symbol */
    private inline fun handleSymbolToken(sid: Int, text: String?, kind: SymbolKind, rawWriter: IonRawWriter_1_1, preserveEncoding: Boolean = false) {
        if (text == null) {
            // No text. Decide whether to write $0 or some other SID
            if (sid == UNKNOWN_SYMBOL_ID) {
                // No (known) SID either.
                throw UnknownSymbolException("Cannot write a symbol token with unknown text and unknown SID.")
            } else if (sid == 0) {
                rawWriter.write(kind, 0)
            } else {
                rawWriter.write(kind, sidTransformer?.transform(sid) ?: sid)
            }
        } else if (preserveEncoding && sid < 0) {
            rawWriter.write(kind, text)
        } else if (options.shouldWriteInline(kind, text)) {
            rawWriter.write(kind, text)
        } else if (SystemSymbols_1_1.contains(text)) {
            rawWriter.write(kind, SystemSymbols_1_1[text]!!)
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

    override fun writeObject(objekt: WriteAsIon) {
        val builder = DirectEExpressionBuilder(this)
        val eExpression = objekt.writeWithEExpression(builder)
        if (eExpression == null) {
            objekt.writeTo(this)
        } else if (eExpression == DirectEExpression) {
            // It was already written as the builder methods were being invoked.
        } else {
            eExpression as PreparedEExpression
            eExpression.writeWithEExpression(builder)
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
            if (it.text == SystemSymbols_1_1.ION_SYMBOL_TABLE.text) {
                userData.writeAnnotations(SystemSymbols_1_1.ION_SYMBOL_TABLE)
            } else {
                handleSymbolToken(it.sid, it.text, SymbolKind.ANNOTATION, userData, preserveEncoding = true)
            }
        }
        if (isInStruct) {
            // TODO: Can't use reader.fieldId, reader.fieldName because it will throw UnknownSymbolException.
            //       However, this might mean we're unnecessarily constructing `SymbolToken` instances.
            val fieldName = reader.fieldNameSymbol
            // If there is no field name, it still may have been set externally, e.g.
            // writer.setFieldName(...); writer.writeValue(reader);
            // This occurs when serializing a sequence of Expressions, which hold field names separate from
            // values.
            if (fieldName != null) {
                handleSymbolToken(fieldName.sid, fieldName.text, SymbolKind.FIELD_NAME, userData, preserveEncoding = true)
            }
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
        if (symbol.text == null) {
            // TODO FIX: Ion 1.1 system symbols can be removed from the encoding context, so an IVM may not always
            //  have symbol ID 2.
            return symbol.sid == 2
        }
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
        if (needsIVM) {
            systemData.writeIVM()
            needsIVM = false
        }
        writeEncodingDirective()
        systemData.flush()
        userData.flush()
    }

    override fun finish() {
        flush()
        resetEncodingContext()
    }

    override fun startMacro(macro: Macro) {
        if (macro is SystemMacro) {
            startSystemMacro(macro)
        } else {
            val address = getOrAssignMacroAddress(macro)
            // Note: macroNames[address] will be null if the macro is unnamed.
            startMacro(macroNames[address], address, macro)
        }
    }

    override fun startMacro(name: String, macro: Macro) {
        if (macro is SystemMacro && macro.macroName == name) {
            startSystemMacro(macro)
        } else {
            val address = getOrAssignMacroAddressAndName(name, macro)
            startMacro(name, address, macro)
        }
    }

    private fun startMacro(name: String?, address: Int, definition: Macro) {
        val useNames = options.eExpressionIdentifierStrategy == ManagedWriterOptions_1_1.EExpressionIdentifierStrategy.BY_NAME
        if (useNames && name != null) {
            userData.stepInEExp(name)
        } else {
            val includeLengthPrefix = options.writeLengthPrefix(ContainerType.EEXP, depth + 1)
            userData.stepInEExp(address, includeLengthPrefix, definition)
        }
    }

    private fun startSystemMacro(macro: SystemMacro) = userData.stepInEExp(macro)

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
