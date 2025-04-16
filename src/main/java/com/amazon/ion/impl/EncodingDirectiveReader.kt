// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl

import com.amazon.ion.*
import com.amazon.ion.SystemSymbols.*
import com.amazon.ion.impl.macro.*
import com.amazon.ion.impl.macro.MacroRef.Companion.byId
import com.amazon.ion.impl.macro.MacroRef.Companion.byName

/**
 * Reads encoding directives from the given [IonReader]. This performs a similar function to
 * [IonReaderContinuableCoreBinary.EncodingDirectiveReader], though that one requires more logic to handle continuable
 * input. The two could be unified at the expense of higher complexity than is needed by the non-continuable text
 * implementation. If the text reader is replaced with a continuable implementation in the future,
 * IonReaderContinuableCoreBinary.EncodingDirectiveReader should be moved to the top level and shared by both readers.
 * If that were to happen, this class would no longer be needed.
 */
internal class EncodingDirectiveReader(private val reader: IonReader, private val readerAdapter: ReaderAdapter) {

    private var localMacroMaxOffset: Int = -1
    private var state: State = State.READING_VALUE

    var isSymbolTableAppend = false
    var isMacroTableAppend = false
    var newSymbols: MutableList<String> = ArrayList(8)
    var newMacros: MutableMap<MacroRef, Macro> = HashMap()
    var isSymbolTableAlreadyClassified = false
    var isMacroTableAlreadyClassified = false

    private enum class State {
        IN_DIRECTIVE_SEXP,
        IN_MODULE_DIRECTIVE_SEXP_AWAITING_MODULE_NAME,
        IN_MODULE_DIRECTIVE_SEXP,
        IN_SYMBOL_TABLE_SEXP,
        IN_SYMBOL_TABLE_LIST,
        IN_MACRO_TABLE_SEXP,
        COMPILING_MACRO,
        READING_VALUE
    }

    private fun classifyDirective() {
        errorIf(reader.type != IonType.SYMBOL) { "Ion encoding directives must start with a directive keyword; found ${reader.type}" }
        val name: String = reader.stringValue()
        // TODO: Add support for `import` and `encoding` directives
        if (SystemSymbols_1_1.MODULE.text == name) {
            state = State.IN_MODULE_DIRECTIVE_SEXP_AWAITING_MODULE_NAME
        } else if (SystemSymbols_1_1.IMPORT.text == name) {
            throw IonException("'import' directive not yet supported")
        } else if (SystemSymbols_1_1.ENCODING.text == name) {
            throw IonException("'encoding' directive not yet supported")
        } else {
            throw IonException(String.format("'%s' is not a valid directive keyword", name))
        }
    }

    private fun classifySexpWithinModuleDirective() {
        val name: String = reader.stringValue()
        state = if (SystemSymbols_1_1.SYMBOLS.text == name) {
            State.IN_SYMBOL_TABLE_SEXP
        } else if (SystemSymbols_1_1.MACROS.text == name) {
            State.IN_MACRO_TABLE_SEXP
        } else {
            throw IonException("'$name' clause not supported in module definition")
        }
    }

    /**
     * Classifies a symbol table as either 'set' or 'append'. The caller must ensure the reader is positioned within a
     * symbol table (after the symbol 'symbol_table') before calling. Returns true if the end of the symbol table has
     * been reached; otherwise, returns false with the reader positioned within a list in the symbol table.
     */
    private fun classifySymbolTable(): Boolean {
        val type: IonType = reader.type
        if (isSymbolTableAlreadyClassified) {
            if (type != IonType.LIST) { // TODO support module name imports
                throw IonException("symbol_table s-expression must contain list(s) of symbols.")
            }
            reader.stepIn()
            state = State.IN_SYMBOL_TABLE_LIST
            return false
        }
        isSymbolTableAlreadyClassified = true
        if (IonType.isText(type)) {
            if (DEFAULT_MODULE == reader.stringValue() && !isSymbolTableAppend) {
                isSymbolTableAppend = true
                if (reader.next() == null) {
                    return true
                }
                if (reader.type != IonType.LIST) {
                    throw IonException("symbol_table s-expression must begin with a list.")
                }
            } else {
                throw IonException("symbol_table s-expression must begin with either '_' or a list.")
            }
        } else if (type != IonType.LIST) {
            throw IonException("symbol_table s-expression must begin with either '_' or a list.")
        }
        reader.stepIn()
        state = State.IN_SYMBOL_TABLE_LIST
        return false
    }

    /**
     * Classifies a macro table as either 'set' or 'append'. The caller must ensure the reader is positioned within a
     * macro table (after the symbol 'macro_table') before calling. Returns true if the end of the macro table has
     * been reached; otherwise, returns false with the reader positioned on an s-expression in the macro table.
     */
    private fun classifyMacroTable(): Boolean {
        val type: IonType = reader.type
        if (isMacroTableAlreadyClassified) {
            if (type != IonType.SEXP) {
                throw IonException("macro_table s-expression must contain s-expressions.")
            }
            return false
        }
        isMacroTableAlreadyClassified = true
        if (IonType.isText(type)) {
            if (SystemSymbols.DEFAULT_MODULE == reader.stringValue() && !isMacroTableAppend) {
                isMacroTableAppend = true
                if (reader.next() == null) {
                    return true
                }
                if (reader.type != IonType.SEXP) {
                    throw IonException("macro_table s-expression must begin with s-expression(s).")
                }
            } else {
                throw IonException("macro_table s-expression must begin with either '_' or s-expression(s).")
            }
        } else if (type == IonType.SEXP) {
            localMacroMaxOffset = -1
        } else {
            throw IonException("macro_table s-expression must begin with either '_' or s-expression(s).")
        }
        return false
    }

    /**
     * Utility function to make error cases more concise.
     * @param condition the condition under which an IonException should be thrown
     * @param lazyErrorMessage the message to use in the exception
     */
    private inline fun errorIf(condition: Boolean, lazyErrorMessage: () -> String) {
        if (condition) {
            throw IonException(lazyErrorMessage())
        }
    }

    /**
     * Reads an encoding directive. After this method returns, the caller should access this class's properties to
     * retrieve the symbols and macros declared within the directive.
     */
    fun readEncodingDirective(encodingContext: EncodingContext) {

        val macroCompiler = MacroCompiler({ key -> resolveMacro(encodingContext, key) }, readerAdapter)

        reader.stepIn()
        state = State.IN_DIRECTIVE_SEXP
        while (true) {
            when (state) {

                State.IN_DIRECTIVE_SEXP -> {
                    errorIf(reader.next() == null) { "invalid Ion directive; missing directive keyword" }
                    classifyDirective()
                }
                State.IN_MODULE_DIRECTIVE_SEXP_AWAITING_MODULE_NAME -> {
                    errorIf(reader.next() == null) { "invalid module directive; missing module name" }
                    errorIf(reader.type != IonType.SYMBOL) { "invalid module directive; module name must be a symbol" }
                    // TODO: Support other module names
                    errorIf(DEFAULT_MODULE != reader.stringValue()) { "IonJava currently supports only the default module" }
                    state = State.IN_MODULE_DIRECTIVE_SEXP
                }
                State.IN_MODULE_DIRECTIVE_SEXP -> {
                    if (reader.next() == null) {
                        reader.stepOut()
                        state = State.READING_VALUE
                        return
                    }
                    if (reader.type != IonType.SEXP) {
                        throw IonException("module definition must contain only s-expressions.")
                    }
                    reader.stepIn()
                    if (reader.next() == null || !IonType.isText(reader.type)) {
                        throw IonException("S-expressions within module definitions must begin with a text token.")
                    }
                    classifySexpWithinModuleDirective()
                }

                State.IN_SYMBOL_TABLE_SEXP -> {
                    if (reader.next() == null || classifySymbolTable()) {
                        reader.stepOut()
                        state = State.IN_MODULE_DIRECTIVE_SEXP
                        continue
                    }
                }

                State.IN_SYMBOL_TABLE_LIST -> {
                    if (reader.next() == null) {
                        reader.stepOut()
                        state = State.IN_SYMBOL_TABLE_SEXP
                        continue
                    }
                    if (!IonType.isText(reader.type)) {
                        throw IonException("The symbol_table must contain text.")
                    }
                    newSymbols.add(reader.stringValue())
                }

                State.IN_MACRO_TABLE_SEXP -> {
                    if (reader.next() == null || classifyMacroTable()) {
                        reader.stepOut()
                        state = State.IN_MODULE_DIRECTIVE_SEXP
                        continue
                    }
                    state = State.COMPILING_MACRO
                    val newMacro: Macro = macroCompiler.compileMacro()
                    newMacros[byId(++localMacroMaxOffset)] = newMacro
                    if (macroCompiler.macroName != null) {
                        newMacros[byName(macroCompiler.macroName!!)] = newMacro
                    }
                    state = State.IN_MACRO_TABLE_SEXP
                }

                // TODO handle other legal encoding directive s-expression shapes.
                // TODO add strict enforcement of the schema around e.g. repeats

                else -> throw IllegalStateException(state.toString())
            }
        }
    }

    private fun resolveMacro(context: EncodingContext, address: MacroRef): Macro? {
        var newMacro = newMacros[address]
        if (newMacro == null) {
            newMacro = context.macroTable.get(address)
        }
        return newMacro
    }

    /**
     * @return true if the reader is currently being used by the [MacroCompiler].
     */
    fun isMacroCompilationInProgress(): Boolean {
        return state == State.COMPILING_MACRO
    }

    /**
     * Prepares the EncodingDirectiveReader to read a new encoding directive.
     */
    fun reset() {
        isSymbolTableAppend = false
        isSymbolTableAlreadyClassified = false
        newSymbols.clear()
        isMacroTableAppend = false
        newMacros.clear()
        isMacroTableAlreadyClassified = false
    }
}
