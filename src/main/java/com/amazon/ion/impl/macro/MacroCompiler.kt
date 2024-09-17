// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.*
import com.amazon.ion.impl.*
import com.amazon.ion.impl.macro.Expression.*
import com.amazon.ion.util.confirm

/**
 * [MacroCompiler] wraps an [IonReader]. When directed to do so, it will take over advancing and getting values from the
 * reader in order to read one [TemplateMacro].
 *
 * This is currently implemented using [IonReader], but it could be adapted to work with
 * [IonReaderContinuableCore][com.amazon.ion.impl.IonReaderContinuableCore].
 */
// TODO determine a strategy for generalizing or templatizing this for use in both text and binary
class MacroCompiler(
    private val reader: IonReaderContinuableCore,
    private val getMacro: (MacroRef) -> Macro?,
) {
    // TODO: Make sure that we can throw exceptions if there's an over-sized value.

    /** The name of the macro that was read. Returns `null` if no macro name is available. */
    var macroName: String? = null
        private set // Only mutable internally

    private val signature: MutableList<Macro.Parameter> = mutableListOf()
    private val expressions: MutableList<TemplateBodyExpression> = mutableListOf()

    /**
     * Compiles a template macro definition from the reader. Caller is responsible for positioning the reader at—but not
     * stepped into—the macro template s-expression.
     *
     * TODO: if we switch the macro compiler to use a continuable reader, change the return type of this
     *       to a compiler state enum, and add a separate function to get the compiled macro once it is ready.
     */
    fun compileMacro(): TemplateMacro {
        macroName = null
        signature.clear()
        expressions.clear()

        confirm(reader.encodingType == IonType.SEXP) { "macro compilation expects a sexp starting with the keyword `macro`" }
        reader.confirmNoAnnotations("a macro definition sexp")
        reader.readContainer {
            reader.nextValue()
            confirm(reader.encodingType == IonType.SYMBOL && reader.stringValue() == "macro") { "macro compilation expects a sexp starting with the keyword `macro`" }

            nextAndCheckType(IonType.SYMBOL, IonType.NULL, "macro name")
            confirmNoAnnotations("macro name")
            if (encodingType != IonType.NULL) {
                macroName = stringValue().also { confirm(isIdentifierSymbol(it)) { "invalid macro name: '$it'" } }
            }
            nextAndCheckType(IonType.SEXP, "macro signature")
            confirmNoAnnotations("macro signature")
            readSignature()
            confirm(nextValue() != IonCursor.Event.END_CONTAINER) { "Macro definition is missing a template body expression." }
            compileTemplateBodyExpression(isQuoted = false)
            confirm(nextValue() == IonCursor.Event.END_CONTAINER) { "Unexpected $type after template body expression." }
        }
        return TemplateMacro(signature.toList(), expressions.toList())
    }

    /**
     * Reads the macro signature, populating parameters in [signature].
     * Caller is responsible for making sure that the reader is positioned on (but not stepped into) the signature sexp.
     */
    private fun readSignature() {
        var pendingParameter: Macro.Parameter? = null

        reader.forEachInContainer {
            if (encodingType != IonType.SYMBOL) throw IonException("parameter must be a symbol; found $type")

            val symbolText = stringValue()

            val cardinality = Macro.ParameterCardinality.fromSigil(symbolText)

            if (cardinality != null) {
                confirmNoAnnotations("cardinality sigil")
                // The symbol is a cardinality modifier
                if (pendingParameter == null) {
                    throw IonException("Found an orphaned cardinality in macro signature")
                } else {
                    signature.add(pendingParameter!!.copy(cardinality = cardinality))
                    pendingParameter = null
                    return@forEachInContainer
                }
            }

            // If we have a pending parameter, add it to the signature before we read the next parameter
            if (pendingParameter != null) signature.add(pendingParameter!!)

            // Read the next parameter name
            val annotations = getTypeAnnotationSymbols()
            confirm(annotations.isEmptyOr(Macro.ParameterEncoding.Tagged.ionTextName)) { "unsupported parameter encoding $annotations" }
            confirm(isIdentifierSymbol(symbolText)) { "invalid parameter name: '$symbolText'" }
            confirm(signature.none { it.variableName == symbolText }) { "redeclaration of parameter '$symbolText'" }
            pendingParameter = Macro.Parameter(symbolText, Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.ExactlyOne)
        }
        // If we have a pending parameter than hasn't been added to the signature, add it here.
        if (pendingParameter != null) signature.add(pendingParameter!!)
    }

    private fun isIdentifierSymbol(symbol: String): Boolean {
        if (symbol.isEmpty()) return false

        // If the symbol's text matches an Ion keyword, it's not an identifier symbol.
        // Eg, the symbol 'false' must be quoted and is not an identifier symbol.
        if (_Private_IonTextAppender.isIdentifierKeyword(symbol)) return false

        if (!_Private_IonTextAppender.isIdentifierStart(symbol[0].code)) return false

        return symbol.all { c -> _Private_IonTextAppender.isIdentifierPart(c.code) }
    }

    /**
     * Compiles the current value on the reader into a [TemplateBodyExpression] and adds it to [expressions].
     * Caller is responsible for ensuring that the reader is positioned on a value.
     *
     * If called when the reader is not positioned on any value, throws [IllegalStateException].
     */
    private fun compileTemplateBodyExpression(isQuoted: Boolean) {
        // NOTE: `toList()` does not allocate for an empty list.
        val annotations: List<SymbolToken> = reader.getTypeAnnotationSymbols()

        if (reader.isNullValue) {
            expressions.add(NullValue(annotations, reader.encodingType))
        } else when (reader.encodingType) {
            IonType.BOOL -> expressions.add(BoolValue(annotations, reader.booleanValue()))
            IonType.INT -> expressions.add(
                when (reader.integerSize!!) {
                    IntegerSize.INT,
                    IntegerSize.LONG -> LongIntValue(annotations, reader.longValue())
                    IntegerSize.BIG_INTEGER -> BigIntValue(annotations, reader.bigIntegerValue())
                }
            )
            IonType.FLOAT -> expressions.add(FloatValue(annotations, reader.doubleValue()))
            IonType.DECIMAL -> expressions.add(DecimalValue(annotations, reader.decimalValue()))
            IonType.TIMESTAMP -> expressions.add(TimestampValue(annotations, reader.timestampValue()))
            IonType.STRING -> expressions.add(StringValue(annotations, reader.stringValue()))
            IonType.BLOB -> expressions.add(BlobValue(annotations, reader.newBytes()))
            IonType.CLOB -> expressions.add(ClobValue(annotations, reader.newBytes()))
            IonType.SYMBOL -> {
                if (isQuoted) {
                    expressions.add(SymbolValue(annotations, reader.symbolValue()))
                } else {
                    val name = reader.stringValue()
                    reader.confirmNoAnnotations("on variable reference '$name'")
                    val index = signature.indexOfFirst { it.variableName == name }
                    confirm(index >= 0) { "variable '$name' is not recognized" }
                    expressions.add(VariableRef(index))
                }
            }
            IonType.LIST -> compileSequence(isQuoted) { start, end -> ListValue(annotations, start, end) }
            IonType.SEXP -> {
                if (isQuoted) {
                    compileSequence(isQuoted = true) { start, end -> SExpValue(annotations, start, end) }
                } else {
                    reader.confirmNoAnnotations(location = "a macro invocation")
                    compileMacroInvocation()
                }
            }
            IonType.STRUCT -> compileStruct(annotations, isQuoted)
            // IonType.NULL, IonType.DATAGRAM, null
            else -> throw IllegalStateException("Found ${reader.encodingType}; this should be unreachable.")
        }
    }

    /**
     * Compiles a struct in a macro template.
     * When calling, the reader should be positioned at the struct, but not stepped into it.
     * If this function returns normally, it will be stepped out of the struct.
     * Caller will need to call [IonReader.next] to get the next value.
     */
    private fun compileStruct(annotations: List<SymbolToken>, isQuoted: Boolean) {
        val start = expressions.size
        expressions.add(Placeholder)
        val templateStructIndex = mutableMapOf<String, ArrayList<Int>>()
        reader.forEachInContainer {
            val fieldName: SymbolToken = fieldNameSymbol
            expressions.add(FieldName(fieldName))
            fieldName.text?.let {
                val valueIndex = expressions.size
                // Default is an array list with capacity of 1, since the most common case is that a field name occurs once.
                templateStructIndex.getOrPut(it) { ArrayList(1) } += valueIndex
            }
            compileTemplateBodyExpression(isQuoted)
        }
        val end = expressions.size
        expressions[start] = StructValue(annotations, start, end, templateStructIndex)
    }

    /**
     * Compiles a list or sexp in a macro template.
     * When calling, the reader should be positioned at the sequence, but not stepped into it.
     * If this function returns normally, it will be stepped out of the sequence.
     * Caller will need to call [IonReader.next] to get the next value.
     */
    private inline fun compileSequence(isQuoted: Boolean, newTemplateBodySequence: (Int, Int) -> TemplateBodyExpression) {
        val seqStart = expressions.size
        expressions.add(Placeholder)
        reader.forEachInContainer { compileTemplateBodyExpression(isQuoted) }
        val seqEnd = expressions.size
        expressions[seqStart] = newTemplateBodySequence(seqStart, seqEnd)
    }

    /**
     * Compiles a macro invocation in a macro template.
     * When calling, the reader should be positioned at the sexp, but not stepped into it.
     * If this function returns normally, it will be stepped out of the sexp.
     * Caller will need to call [IonReader.next] to get the next value.
     */
    private fun compileMacroInvocation() {
        reader.stepIntoContainer()
        reader.nextValue()
        val macroRef = when (reader.encodingType) {
            IonType.SYMBOL -> {
                val macroName = reader.stringValue()
                // TODO: Come up with a consistent strategy for handling special forms.
                when (macroName) {
                    "literal" -> {
                        // It's the "literal" special form; skip compiling a macro invocation and just treat all contents as literals
                        reader.forEachRemaining { compileTemplateBodyExpression(isQuoted = true) }
                        reader.stepOutOfContainer()
                        return
                    }
                    ";" -> {
                        val macroStart = expressions.size
                        expressions.add(Placeholder)
                        reader.forEachRemaining { compileTemplateBodyExpression(isQuoted = false) }
                        val macroEnd = expressions.size
                        expressions[macroStart] = ExpressionGroup(macroStart, macroEnd)
                        reader.stepOutOfContainer()
                        return
                    }
                    else -> MacroRef.ByName(macroName)
                }
            }
            IonType.INT -> MacroRef.ById(reader.intValue())
            else -> throw IonException("macro invocation must start with an id (int) or identifier (symbol); found ${reader.encodingType ?: "nothing"}\"")
        }

        val macro = getMacro(macroRef) ?: throw IonException("Unrecognized macro: $macroRef")

        val macroStart = expressions.size
        expressions.add(Placeholder)
        reader.forEachRemaining { compileTemplateBodyExpression(isQuoted = false) }
        val macroEnd = expressions.size
        expressions[macroStart] = MacroInvocation(macro, macroStart, macroEnd)

        reader.stepOutOfContainer()
    }

    // Helper functions

    /** Utility method for checking that annotations are empty or a single array with the given annotations */
    private fun List<SymbolToken>.isEmptyOr(text: String): Boolean = isEmpty() || (size == 1 && this[0].assumeText() == text)

    /** Throws [IonException] if any annotations are on the current value in this [IonReader]. */
    private fun IonReaderContinuableCore.confirmNoAnnotations(location: String) {
        confirm(!hasAnnotations()) { "found annotations on $location" }
    }

    /** Moves to the next type and throw [IonException] if it is not the `expected` [IonType]. */
    private fun IonReaderContinuableCore.nextAndCheckType(expected: IonType, location: String) {
        confirm(nextValue() != IonCursor.Event.NEEDS_DATA && encodingType == expected) { "$location must be a $expected; found ${encodingType ?: "nothing"}" }
    }

    /** Moves to the next type and throw [IonException] if it is not the `expected` [IonType]. */
    private fun IonReaderContinuableCore.nextAndCheckType(expected0: IonType, expected1: IonType, location: String) {
        confirm(nextValue() != IonCursor.Event.NEEDS_DATA && (encodingType == expected0 || encodingType == expected1)) { "$location must be a $expected0 or $expected1; found ${encodingType ?: "nothing"}" }
    }

    /** Steps into a container, executes [block], and steps out. */
    private inline fun IonReaderContinuableCore.readContainer(block: IonReaderContinuableCore.() -> Unit) { stepIntoContainer(); block(); stepOutOfContainer() }

    /** Executes [block] for each remaining value at the current reader depth. */
    private inline fun IonReaderContinuableCore.forEachRemaining(block: IonReaderContinuableCore.(IonType) -> Unit) { while (nextValue() != IonCursor.Event.END_CONTAINER) { block(encodingType) } }

    /** Steps into a container, executes [block] for each value at that reader depth, and steps out. */
    private inline fun IonReaderContinuableCore.forEachInContainer(block: IonReaderContinuableCore.(IonType) -> Unit) = readContainer { forEachRemaining(block) }

    private fun IonReaderContinuableCore.getTypeAnnotationSymbols(): List<SymbolToken> {
        if (!hasAnnotations()) {
            return emptyList()
        }
        val annotations = arrayListOf<SymbolToken>()
        consumeAnnotationTokens { annotations += it }
        return annotations
    }
}
