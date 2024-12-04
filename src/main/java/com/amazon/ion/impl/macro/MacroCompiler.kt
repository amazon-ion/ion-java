// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.*
import com.amazon.ion.impl.*
import com.amazon.ion.impl.bin.Ion_1_1_Constants.*
import com.amazon.ion.impl.macro.Expression.*
import com.amazon.ion.util.*

/**
 * [MacroCompiler] wraps an Ion reader. When directed to do so, it will take over advancing and getting values from the
 * reader in order to read one [TemplateMacro].
 */
internal class MacroCompiler(
    private val getMacro: (MacroRef) -> Macro?,
    private val reader: ReaderAdapter
) {

    /** The name of the macro that was read. Returns `null` if no macro name is available. */
    var macroName: String? = null
        private set // Only mutable internally

    private val signature: MutableList<Macro.Parameter> = mutableListOf()
    private val expressions: MutableList<TemplateBodyExpression> = mutableListOf()

    /**
     * Compiles a template macro definition from the reader. Caller is responsible for positioning the reader at—but not
     * stepped into—the macro template s-expression.
     */
    fun compileMacro(): TemplateMacro {
        macroName = null
        signature.clear()
        expressions.clear()

        confirm(reader.encodingType() == IonType.SEXP) { "macro compilation expects a sexp starting with the keyword `macro`" }
        reader.confirmNoAnnotations("a macro definition sexp")
        reader.readContainer {
            reader.nextValue()
            confirm(reader.encodingType() == IonType.SYMBOL && reader.stringValue() == "macro") { "macro compilation expects a sexp starting with the keyword `macro`" }

            reader.nextAndCheckType(IonType.SYMBOL, IonType.NULL, "macro name")

            reader.confirmNoAnnotations("macro name")
            if (reader.encodingType() != IonType.NULL) {
                macroName = reader.stringValue().also { confirm(isIdentifierSymbol(it)) { "invalid macro name: '$it'" } }
            }
            reader.nextAndCheckType(IonType.SEXP, "macro signature")
            reader.confirmNoAnnotations("macro signature")
            reader.readSignature()
            confirm(reader.nextValue()) { "Macro definition is missing a template body expression." }
            reader.compileTemplateBodyExpression()
            confirm(!reader.nextValue()) { "Unexpected ${reader.encodingType()} after template body expression." }
        }
        return TemplateMacro(signature.toList(), expressions.toList())
    }

    /**
     * Reads the macro signature, populating parameters in [signature].
     * Caller is responsible for making sure that the reader is positioned on (but not stepped into) the signature sexp.
     */
    private fun ReaderAdapter.readSignature() {
        var pendingParameter: Macro.Parameter? = null

        forEachInContainer {
            if (encodingType() != IonType.SYMBOL) throw IonException("parameter must be a symbol; found ${encodingType()}")

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
            val parameterEncoding = when (annotations.size) {
                0 -> Macro.ParameterEncoding.Tagged
                1 -> {
                    val encodingText = annotations[0].text
                    val encoding = Macro.ParameterEncoding.entries.singleOrNull { it.ionTextName == encodingText }
                    if (encoding == null) {
                        // TODO: Check for macro-shaped parameter encodings, and only if it's still null, we throw.
                        throw IonException("Unknown parameter encoding: $encodingText")
                    }
                    encoding
                }
                2 -> TODO("Qualified references for macro-shaped parameters")
                else -> throw IonException("unsupported parameter encoding $annotations")
            }
            confirm(isIdentifierSymbol(symbolText)) { "invalid parameter name: '$symbolText'" }
            confirm(signature.none { it.variableName == symbolText }) { "redeclaration of parameter '$symbolText'" }
            pendingParameter = Macro.Parameter(symbolText, parameterEncoding, Macro.ParameterCardinality.ExactlyOne)
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
    private fun ReaderAdapter.compileTemplateBodyExpression() {
        // NOTE: `toList()` does not allocate for an empty list.
        val annotations: List<SymbolToken> = getTypeAnnotationSymbols()

        if (isNullValue()) {
            expressions.add(NullValue(annotations, encodingType()!!))
        } else when (encodingType()) {
            IonType.BOOL -> expressions.add(BoolValue(annotations, booleanValue()))
            IonType.INT -> expressions.add(
                when (integerSize()!!) {
                    IntegerSize.INT,
                    IntegerSize.LONG -> LongIntValue(annotations, longValue())
                    IntegerSize.BIG_INTEGER -> BigIntValue(annotations, bigIntegerValue())
                }
            )
            IonType.FLOAT -> expressions.add(FloatValue(annotations, doubleValue()))
            IonType.DECIMAL -> expressions.add(DecimalValue(annotations, decimalValue()))
            IonType.TIMESTAMP -> expressions.add(TimestampValue(annotations, timestampValue()))
            IonType.STRING -> expressions.add(StringValue(annotations, stringValue()))
            IonType.BLOB -> expressions.add(BlobValue(annotations, newBytes()))
            IonType.CLOB -> expressions.add(ClobValue(annotations, newBytes()))
            IonType.SYMBOL -> expressions.add(SymbolValue(annotations, symbolValue()))
            IonType.LIST -> compileList(annotations)
            IonType.SEXP -> compileSExpression(annotations)
            IonType.STRUCT -> compileStruct(annotations)
            // IonType.NULL, IonType.DATAGRAM, null
            else -> throw IllegalStateException("Found ${encodingType()}; this should be unreachable.")
        }
    }

    /**
     * Compiles a struct in a macro template.
     * When calling, the reader should be positioned at the struct, but not stepped into it.
     * If this function returns normally, it will be stepped out of the struct.
     * Caller will need to call [IonReader.next] to get the next value.
     */
    private fun ReaderAdapter.compileStruct(annotations: List<SymbolToken>) {
        val start = expressions.size
        expressions.add(Placeholder)
        val templateStructIndex = mutableMapOf<String, ArrayList<Int>>()
        forEachInContainer {
            val fieldName: SymbolToken = fieldNameSymbol()
            expressions.add(FieldName(fieldName))
            fieldName.text?.let {
                val valueIndex = expressions.size
                // Default is an array list with capacity of 1, since the most common case is that a field name occurs once.
                templateStructIndex.getOrPut(it) { ArrayList(1) } += valueIndex
            }
            compileTemplateBodyExpression()
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
    private fun ReaderAdapter.compileList(annotations: List<SymbolToken>) {
        val start = expressions.size
        stepIntoContainer()
        expressions.add(Placeholder)
        compileExpressionTail(start) { end -> ListValue(annotations, start, end) }
    }

    /**
     * Compiles an unclassified S-Expression in a template body expression.
     * When calling, the reader should be positioned at the sexp, but not stepped into it.
     * If this function returns normally, it will be stepped out of the sexp.
     * Caller will need to call [IonReader.next] to get the next value.
     */
    private fun ReaderAdapter.compileSExpression(sexpAnnotations: List<SymbolToken>) {
        val start = expressions.size
        stepIntoContainer()
        expressions.add(Placeholder)
        if (nextValue()) {
            if (encodingType() == IonType.SYMBOL) {
                when (stringValue()) {
                    TDL_VARIABLE_EXPANSION_SIGIL -> {
                        confirm(sexpAnnotations.isEmpty()) { "Variable expansion may not be annotated" }
                        confirmNoAnnotations("Variable expansion operator")
                        compileVariableExpansion(start)
                        return
                    }

                    TDL_EXPRESSION_GROUP_SIGIL -> {
                        confirm(sexpAnnotations.isEmpty()) { "Expression group may not be annotated" }
                        confirmNoAnnotations("Expression group operator")
                        compileExpressionTail(start) { end -> ExpressionGroup(start, end) }
                        return
                    }

                    TDL_MACRO_INVOCATION_SIGIL -> {
                        confirm(sexpAnnotations.isEmpty()) { "Macro invocation may not be annotated" }
                        confirmNoAnnotations("Macro invocation operator")
                        nextValue()
                        val macro = readMacroReference()
                        compileExpressionTail(start) { end -> MacroInvocation(macro, start, end) }
                        return
                    }
                }
            }
            // Compile the value we're already positioned on before compiling the rest of the s-expression
            compileTemplateBodyExpression()
        }
        compileExpressionTail(start) { end -> SExpValue(sexpAnnotations, start, end) }
    }

    /**
     * Must be positioned on the (expected) macro reference.
     */
    private fun ReaderAdapter.readMacroReference(): Macro {

        val annotations = getTypeAnnotationSymbols()
        val isQualifiedSystemMacro = annotations.size == 1 && SystemSymbols_1_1.ION.text == annotations[0].getText()

        val macroRef = when (encodingType()) {
            IonType.SYMBOL -> {
                val macroName = stringValue()
                // TODO: Come up with a consistent strategy for handling special forms.
                MacroRef.ByName(macroName)
            }

            IonType.INT -> {
                val sid = intValue()
                if (sid < 0) throw IonException("Macro ID must be non-negative: $sid")
                MacroRef.ById(intValue())
            }
            else -> throw IonException("macro invocation must start with an id (int) or identifier (symbol); found ${encodingType() ?: "nothing"}\"")
        }
        val m = if (isQualifiedSystemMacro) SystemMacro.getMacroOrSpecialForm(macroRef) else getMacro(macroRef)
        return m ?: throw IonException("Unrecognized macro: $macroRef")
    }

    private fun ReaderAdapter.compileVariableExpansion(placeholderIndex: Int) {
        nextValue()
        confirm(encodingType() == IonType.SYMBOL) { "Variable names must be symbols" }
        val name = stringValue()
        confirmNoAnnotations("on variable reference '$name'")
        val index = signature.indexOfFirst { it.variableName == name }
        confirm(index >= 0) { "variable '$name' is not recognized" }
        expressions[placeholderIndex] = VariableRef(index)
        confirm(!nextValue()) { "Variable expansion should contain only the variable name." }
        stepOutOfContainer()
    }

    private inline fun ReaderAdapter.compileExpressionTail(seqStart: Int, constructor: (Int) -> TemplateBodyExpression) {
        forEachRemaining { compileTemplateBodyExpression() }
        val seqEnd = expressions.size
        expressions[seqStart] = constructor(seqEnd)
        stepOutOfContainer()
    }

    // Helper functions

    /** Utility method for checking that annotations are empty or a single array with the given annotations */
    private fun List<SymbolToken>.isEmptyOr(text: String): Boolean = isEmpty() || (size == 1 && this[0].assumeText() == text)

    /** Throws [IonException] if any annotations are on the current value in this [IonReader]. */
    private fun ReaderAdapter.confirmNoAnnotations(location: String) {
        confirm(!hasAnnotations()) { "found annotations on $location" }
    }

    /** Moves to the next type and throw [IonException] if it is not the `expected` [IonType]. */
    private fun ReaderAdapter.nextAndCheckType(expected: IonType, location: String) {
        confirm(nextValue() && encodingType() == expected) { "$location must be a $expected; found ${encodingType() ?: "nothing"}" }
    }

    /** Moves to the next type and throw [IonException] if it is not the `expected` [IonType]. */
    private fun ReaderAdapter.nextAndCheckType(expected0: IonType, expected1: IonType, location: String) {
        confirm(nextValue() && (encodingType() == expected0 || encodingType() == expected1)) { "$location must be a $expected0 or $expected1; found ${encodingType() ?: "nothing"}" }
    }

    /** Steps into a container, executes [block], and steps out. */
    private inline fun ReaderAdapter.readContainer(block: () -> Unit) { stepIntoContainer(); block(); stepOutOfContainer() }

    /** Executes [block] for each remaining value at the current reader depth. */
    private inline fun ReaderAdapter.forEachRemaining(block: (IonType) -> Unit) { while (nextValue()) { block(encodingType()!!) } }

    /** Steps into a container, executes [block] for each value at that reader depth, and steps out. */
    private inline fun ReaderAdapter.forEachInContainer(block: (IonType) -> Unit) = readContainer { forEachRemaining(block) }
}
