// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl

import com.amazon.ion.*
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
internal class EncodingDirectiveReader(private val reader: IonReader, readerAdapter: ReaderAdapter) {

    private var macroCompiler: MacroCompiler = MacroCompiler({ key: Any? -> newMacros[key] }, readerAdapter)
    private var localMacroMaxOffset: Int = -1
    private var state: State = State.READING_VALUE

    var isSymbolTableAppend = false
    var newSymbols: MutableList<String> = ArrayList(8)
    var newMacros: MutableMap<MacroRef, Macro> = HashMap()

    private enum class State {
        IN_ION_ENCODING_SEXP,
        IN_SYMBOL_TABLE_SEXP,
        IN_SYMBOL_TABLE_LIST,
        IN_MACRO_TABLE_SEXP,
        COMPILING_MACRO,
        READING_VALUE
    }

    private fun classifySexpWithinEncodingDirective() {
        val name: String = reader.stringValue()
        state = if (SystemSymbols_1_1.SYMBOL_TABLE.text == name) {
            State.IN_SYMBOL_TABLE_SEXP
        } else if (SystemSymbols_1_1.MACRO_TABLE.text == name) {
            State.IN_MACRO_TABLE_SEXP
        } else {
            throw IonException(String.format("\$ion_encoding expressions '%s' not supported.", name))
        }
    }

    private fun classifySymbolTable() {
        val type: IonType = reader.type
        if (IonType.isText(type)) {
            if (SystemSymbols.ION_ENCODING == reader.stringValue() && !isSymbolTableAppend) {
                if (reader.next() == null || reader.type != IonType.LIST) {
                    throw IonException("symbol_table s-expression must begin with a list.")
                }
                isSymbolTableAppend = true
            } else {
                throw IonException("symbol_table s-expression must begin with either \$ion_encoding or a list.")
            }
        } else if (type != IonType.LIST) {
            throw IonException("symbol_table s-expression must begin with either \$ion_encoding or a list.")
        }
        reader.stepIn()
        state = State.IN_SYMBOL_TABLE_LIST
    }

    /**
     * Reads an encoding directive. After this method returns, the caller should access this class's properties to
     * retrieve the symbols and macros declared within the directive.
     */
    fun readEncodingDirective() {
        reader.stepIn()
        state = State.IN_ION_ENCODING_SEXP
        while (true) {
            when (state) {

                State.IN_ION_ENCODING_SEXP -> {
                    if (reader.next() == null) {
                        reader.stepOut()
                        state = State.READING_VALUE
                        return
                    }
                    if (reader.type != IonType.SEXP) {
                        throw IonException("Ion encoding directives must contain only s-expressions.")
                    }
                    reader.stepIn()
                    if (reader.next() == null || !IonType.isText(reader.type)) {
                        throw IonException("S-expressions within encoding directives must begin with a text token.")
                    }
                    classifySexpWithinEncodingDirective()
                }

                State.IN_SYMBOL_TABLE_SEXP -> {
                    if (reader.next() == null) {
                        reader.stepOut()
                        state = State.IN_ION_ENCODING_SEXP
                        continue
                    }
                    classifySymbolTable()
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
                    if (reader.next() == null) {
                        reader.stepOut()
                        state = State.IN_ION_ENCODING_SEXP
                        continue
                    }
                    if (reader.type != IonType.SEXP) {
                        throw IonException("macro_table s-expression must contain s-expressions.")
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
        newSymbols.clear()
        newMacros.clear()
    }
}
