// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl

import com.amazon.ion.IonCatalog
import com.amazon.ion.IonException
import com.amazon.ion.IonReader
import com.amazon.ion.IonType
import com.amazon.ion.IonValue
import com.amazon.ion.Macro
import com.amazon.ion.SymbolTable
import com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID
import com.amazon.ion.SymbolToken
import com.amazon.ion.SystemSymbols
import com.amazon.ion.Timestamp
import com.amazon.ion.UnknownSymbolException
import com.amazon.ion.bytecode.bin11.OpCode
import com.amazon.ion.impl.LengthPrefixStrategy.ContainerType
import com.amazon.ion.impl.SymbolInliningStrategy.SymbolKind
import com.amazon.ion.ion_1_1.IonRawWriter_1_1
import com.amazon.ion.ion_1_1.IonWriter_1_1
import com.amazon.ion.ion_1_1.MacroImpl
import com.amazon.ion.ion_1_1.TaglessScalarType
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import java.math.BigDecimal
import java.math.BigInteger
import java.util.Date

/**
 * A managed writer for Ion 1.1 that is generic over whether the raw encoding is text or binary.
 *
 * TODO:
 *  - Handling of shared symbol tables
 *  - Proper handling of user-supplied symbol tables
 *  - Auto-flush (for binary and text)
 *  - Check that arguments match the signatures of macros/templates (in all cases).
 *  - Check that values in TE lists/sexps are the right encoding (in all cases).
 *  - If a macro has a fixed size because it only has tagless parameters, always write it without a length prefix
 *  - Determine if it's faster to read template definitions that use prefixed vs delimited containers, and make this
 *    always do the right thing.
 *
 * See also [ManagedWriterOptions_1_1], [SymbolInliningStrategy], and [LengthPrefixStrategy].
 */
internal class IonManagedWriter_1_1(
    @SuppressFBWarnings("EI_EXPOSE_REP2", justification = "We're intentionally storing a reference to a mutable object because we need to write to it.")
    private val userData: IonRawWriter_1_1,
    @SuppressFBWarnings("EI_EXPOSE_REP2", justification = "We're intentionally storing a reference to a mutable object because we need to write to it.")
    private val systemData: IonRawWriter_1_1,
    @SuppressFBWarnings("EI_EXPOSE_REP2", justification = "Not mutable")
    private val options: ManagedWriterOptions_1_1,
    private val onClose: Runnable,
) : _Private_IonWriter, IonWriter_1_1 {

    companion object {
        private val ION_VERSION_MARKER_REGEX = Regex("^\\\$ion_\\d+_\\d+$")
    }

    // Since this is Ion 1.1, we must always start with the IVM.
    private var needsIVM: Boolean = true

    private val encodingContextManager = IonManagedWriterEncodingContext_1_1()

    private var annotationsTextBuffer = arrayOfNulls<String>(8)
    private var annotationsIdBuffer = IntArray(8)
    private var numAnnotations = 0
    private var fieldNameText: String? = null
    private var fieldNameId = -1

    private var containers = _Private_RecyclingStack(16) { ContainerInfo() }
    init {
        // Push a container to be at the top-level so we don't need to do null checks.
        containers.push { it.reset() }
    }

    /** Stores info about the types of the child elements. */
    private class ContainerInfo(
        @JvmField var signature: Array<MacroImpl.Parameter> = EMPTY_ARRAY,
        /** 0 for tagged value, >0 for tagless primitive, <0 for macroId */
        @JvmField var childType: Int = TAGGED_VALUE,
        /** If i < 0, this container does not iterate over multiple type (i.e. TE List/Sexp) */
        @JvmField var i: Int = HOMOGENEOUS_TYPE,
    ) {

        companion object {
            @JvmStatic
            private val EMPTY_ARRAY = emptyArray<MacroImpl.Parameter>()
            const val TAGGED_VALUE = 0
            private const val HOMOGENEOUS_TYPE = -1
        }

        fun childIsTaglessMacroShaped() = childType < 0
        fun childIsTaglessScalar() = childType > 0
        fun childIsTaggedValue() = childType == 0

        fun isMacroInvocation() = i != HOMOGENEOUS_TYPE

        val childMacroId: Int get() = -1 - childType

        fun resetForMacroInvocation(signature: Array<MacroImpl.Parameter>) {
            this.signature = signature
            i = 0
            prepareNextChildType()
        }

        fun resetForMacroTESequence(macroId: Int) {
            this.signature = EMPTY_ARRAY
            this.childType = -1 - macroId
            i = HOMOGENEOUS_TYPE
        }

        fun resetForScalarTESequence(opcode: Int) {
            this.signature = EMPTY_ARRAY
            this.childType = opcode
            i = HOMOGENEOUS_TYPE
        }

        fun reset() {
            this.signature = EMPTY_ARRAY
            this.childType = TAGGED_VALUE
            i = HOMOGENEOUS_TYPE // They're all tagged values
        }

        /**
         * Prepares this ContainerInfo to provide information about the type of the next child value.
         * Returns false if the end of the macro signature has been reached. Otherwise, returns true.
         */
        fun prepareNextChildType(): Boolean {
            if (i != HOMOGENEOUS_TYPE) {
                if (i < signature.size) {
                    childType = signature[i++].opcode
                } else {
                    childType = OpCode.DELIMITED_CONTAINER_END
                    return false
                }
            }
            return true
        }

        fun expectedChildTypeName(): String {
            return if (childIsTaggedValue()) {
                "tagged value"
            } else {
                TaglessScalarType.getTaglessScalarTypeForOpcode(childType)?.textEncodingName ?: "e-expression"
            }
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

    override fun setFieldName(name: String) {
        fieldNameText = name
        fieldNameId = -1
    }

    override fun setFieldNameSymbol(name: SymbolToken) {
        fieldNameId = name.sid
        fieldNameText = name.text
    }

    /**
     * Ensures that there is enough space in the annotation buffers for [n] annotations.
     * If more space is needed, it over-allocates by 8 to ensure that we're not continually allocating when annotations
     * are being added one by one.
     */
    private fun ensureAnnotationSpace(n: Int) {
        if (annotationsIdBuffer.size < n || annotationsTextBuffer.size < n) {
            val oldIds = annotationsIdBuffer
            annotationsIdBuffer = IntArray(n + 8)
            oldIds.copyInto(annotationsIdBuffer)
            val oldText = annotationsTextBuffer
            annotationsTextBuffer = arrayOfNulls(n + 8)
            oldText.copyInto(annotationsTextBuffer)
        }
    }

    private fun _private_clearAnnotations() {
        numAnnotations = 0
        // erase the first entries to ensure old values don't leak into `_private_hasFirstAnnotation()`
        annotationsIdBuffer[0] = -1
        annotationsTextBuffer[0] = null
    }

    private fun isIonSymbolTableAnnotationPresent(): Boolean {
        if (numAnnotations == 0) return false
        return annotationsIdBuffer[0] == SystemSymbols.ION_SYMBOL_TABLE_SID || annotationsTextBuffer[0] == SystemSymbols.ION_SYMBOL_TABLE
    }

    override fun addTypeAnnotation(annotation: String) {
        val index = numAnnotations++
        ensureAnnotationSpace(numAnnotations)
        annotationsIdBuffer[index] = UNKNOWN_SYMBOL_ID
        annotationsTextBuffer[index] = annotation
    }

    override fun setTypeAnnotations(annotations: Array<String>?) {
        _private_clearAnnotations()
        numAnnotations = annotations?.size ?: 0
        ensureAnnotationSpace(numAnnotations)
        annotations?.forEachIndexed { index, text ->
            annotationsIdBuffer[index] = UNKNOWN_SYMBOL_ID
            annotationsTextBuffer[index] = text
        }
    }

    override fun setTypeAnnotationSymbols(annotations: Array<SymbolToken>?) {
        _private_clearAnnotations()
        numAnnotations = annotations?.size ?: 0
        ensureAnnotationSpace(numAnnotations)
        annotations?.forEachIndexed { index, token ->
            annotationsIdBuffer[index] = token.sid
            annotationsTextBuffer[index] = token.text
        }
    }

    override fun stepIn(containerType: IonType?) {
        val newDepth = depth + 1
        when (containerType) {
            IonType.LIST -> {
                writeTaggedValueFieldNameAndAnnotations()
                userData.stepInList(options.writeLengthPrefix(ContainerType.LIST, newDepth))
            }
            IonType.SEXP -> {
                writeTaggedValueFieldNameAndAnnotations()
                userData.stepInSExp(options.writeLengthPrefix(ContainerType.SEXP, newDepth))
            }
            IonType.STRUCT -> {
                if (depth == 0 && isIonSymbolTableAnnotationPresent()) {
                    throw IonException("Ion 1.0 symbol tables not permitted in the Ion 1.1 managed writer.")
                }
                writeTaggedValueFieldNameAndAnnotations()
                userData.stepInStruct(options.writeLengthPrefix(ContainerType.STRUCT, newDepth))
            }
            else -> throw IllegalArgumentException("Not a container type: $containerType")
        }
        containers.push { it.reset() }
    }

    override fun stepInTaglessElementList(name: String?, macro: Macro) {
        require(macro is MacroImpl) { "Provided macro is not an Ion 1.1 macro." }
        writeTaggedValueFieldNameAndAnnotations()
        val macroId = encodingContextManager.getOrAssignMacroAddress(macro, name.takeIf { options.useMacroNames })
        userData.stepInTaglessElementList(macroId, name, lengthPrefixed = options.writeLengthPrefix(ContainerType.EEXP, macroId))
        containers.push { it.resetForMacroTESequence(macroId) }
    }

    override fun stepInTaglessElementList(scalar: TaglessScalarType) {
        writeTaggedValueFieldNameAndAnnotations()
        userData.stepInTaglessElementList(scalar.getOpcode())
        containers.push { it.resetForScalarTESequence(scalar.getOpcode()) }
    }

    override fun stepInTaglessElementSExp(name: String?, macro: Macro) {
        require(macro is MacroImpl) { "Provided macro is not an Ion 1.1 macro." }
        writeTaggedValueFieldNameAndAnnotations()
        val macroId = encodingContextManager.getOrAssignMacroAddress(macro, name.takeIf { options.useMacroNames })
        userData.stepInTaglessElementSExp(macroId, name, lengthPrefixed = options.writeLengthPrefix(ContainerType.EEXP, macroId))
        containers.push { it.resetForMacroTESequence(macroId) }
    }

    override fun stepInTaglessElementSExp(scalar: TaglessScalarType) {
        writeTaggedValueFieldNameAndAnnotations()
        userData.stepInTaglessElementSExp(scalar.getOpcode())
        containers.push { it.resetForScalarTESequence(scalar.getOpcode()) }
    }

    override fun stepOut() {
        // TODO: Make sure you can't use this function to step out of a macro?
        containers.pop()
        userData.stepOut()
    }

    override fun isInStruct(): Boolean = userData.isInStruct()

    private inline fun <T> T?.writeMaybeNull(type: IonType, writeNotNull: (T) -> Unit) {
        if (this == null) {
            writeNull(type)
        } else {
            writeNotNull(this)
        }
    }

    private fun writeTaggedValueFieldNameAndAnnotations() {
        val currentContainer = containers.peek()
        if (!currentContainer.childIsTaggedValue()) {
            // TODO: Improve this message to have the expected type and the actual type.
            throw IonException("Tagless value expected, but not provided.")
        }
        currentContainer.prepareNextChildType()

        if (isInStruct()) {
            if (!isFieldNameSet) throw IonException("Values in a struct must have a field name.")
            handleSymbolToken(fieldNameId, fieldNameText, SymbolKind.FIELD_NAME, userData)
            fieldNameId = -1
            fieldNameText = null
        }

        val numAnnotations = this.numAnnotations
        if (numAnnotations > 0) {
            for (i in 0 until numAnnotations) {
                val annotationText = annotationsTextBuffer[i]
                val sid = annotationsIdBuffer[i]
                if (sid >= 0) {
                    handleSymbolToken(UNKNOWN_SYMBOL_ID, annotationText, SymbolKind.ANNOTATION, userData)
                } else {
                    handleSymbolToken(sid, annotationText, SymbolKind.ANNOTATION, userData)
                }
            }
            this.numAnnotations = 0
        }
    }

    override fun writeSymbol(content: String?) {
        val container = containers.peek()
        if (container.childIsTaggedValue()) {
            if (content == null) {
                writeTaggedValueFieldNameAndAnnotations()
                userData.writeNull(IonType.SYMBOL)
            } else {
                if (numAnnotations > 0 && depth == 0 && ION_VERSION_MARKER_REGEX.matches(content)) {
                    throw IonException("Can't write a top-level symbol that is the same as an IVM.")
                }
                writeTaggedValueFieldNameAndAnnotations()
                handleSymbolToken(UNKNOWN_SYMBOL_ID, content, SymbolKind.VALUE, userData)
            }
        } else {
            content ?: throw IonException("Cannot write null.symbol for tagless symbol")
            if (container.childType != OpCode.TE_SYMBOL_FS) {
                val expectedType = container.expectedChildTypeName()
                throw IllegalArgumentException("Cannot write a symbol when a tagless $expectedType is expected.")
            }
            if (options.shouldWriteInline(SymbolKind.VALUE, content)) {
                userData.writeTaglessSymbol(container.childType, content)
            } else {
                userData.writeTaglessSymbol(container.childType, encodingContextManager.intern(content))
            }
            container.prepareNextChildType()
        }
    }

    override fun writeSymbolToken(content: SymbolToken?) {
        val container = containers.peek()
        if (content?.text != null) return writeSymbol(content.text)
        if (container.childIsTaggedValue()) {
            writeTaggedValueFieldNameAndAnnotations()
            if (content == null) {
                userData.writeNull(IonType.SYMBOL)
            } else {
                // TODO: Check to see if the SID refers to a user symbol with text that looks like an IVM
                handleSymbolToken(content.sid, content.text, SymbolKind.VALUE, userData)
            }
        } else {
            if (container.childType != OpCode.TE_SYMBOL_FS) {
                val expectedType = container.expectedChildTypeName()
                throw IllegalArgumentException("Cannot write a symbol when a tagless $expectedType is expected.")
            }
            content ?: throw IonException("Cannot write null.symbol for tagless symbol")
            // content.text is already known to be null
            userData.writeTaglessSymbol(container.childType, 0)
            container.prepareNextChildType()
        }
    }

    private fun IonRawWriter_1_1.write(kind: SymbolKind, sid: Int) = when (kind) {
        SymbolKind.VALUE -> writeSymbol(sid)
        SymbolKind.FIELD_NAME -> writeFieldName(sid)
        SymbolKind.ANNOTATION -> writeAnnotations(sid)
    }

    private fun IonRawWriter_1_1.write(kind: SymbolKind, text: String) = when (kind) {
        SymbolKind.VALUE -> writeSymbol(text)
        SymbolKind.FIELD_NAME -> writeFieldName(text)
        SymbolKind.ANNOTATION -> writeAnnotations(text)
    }

    /** Helper function that determines whether to write a symbol token as a SID or inline symbol */
    private fun handleSymbolToken(sid: Int, text: String?, kind: SymbolKind, rawWriter: IonRawWriter_1_1, preserveEncoding: Boolean = false) {
        if (text == null) {
            // No text. Decide whether to write $0 or some other SID
            if (sid == UNKNOWN_SYMBOL_ID) {
                // No (known) SID either.
                throw UnknownSymbolException("Cannot write a symbol token with unknown text and unknown SID.")
            } else {
                rawWriter.write(kind, sid)
            }
        } else if (preserveEncoding && sid < 0) {
            rawWriter.write(kind, text)
        } else if (options.shouldWriteInline(kind, text)) {
            rawWriter.write(kind, text)
        } else {
            rawWriter.write(kind, encodingContextManager.intern(text))
        }
    }

    override fun writeNull() = writeNull(IonType.NULL)

    override fun writeNull(type: IonType?) {
        if (type == IonType.STRUCT && depth == 0 && isIonSymbolTableAnnotationPresent()) {
            throw IonException("Ion 1.0 symbol tables not permitted in the Ion 1.1 managed writer.")
        }
        writeTaggedValueFieldNameAndAnnotations()
        userData.writeNull(type ?: IonType.NULL)
    }

    override fun writeBool(value: Boolean) {
        writeTaggedValueFieldNameAndAnnotations()
        userData.writeBool(value)
    }

    override fun writeInt(value: Long) {
        val container = containers.peek()
        if (container.childIsTaggedValue()) {
            writeTaggedValueFieldNameAndAnnotations()
            userData.writeInt(value)
        } else {
            userData.writeTaglessInt(container.childType, value)
            container.prepareNextChildType()
        }
    }

    override fun writeInt(value: BigInteger?) {
        val container = containers.peek()
        if (container.childIsTaggedValue()) {
            writeTaggedValueFieldNameAndAnnotations()
            value.writeMaybeNull(IonType.INT, userData::writeInt)
        } else {
            value ?: throw IonException("Cannot write null integer when tagless ${container.expectedChildTypeName()} is expected")
            userData.writeTaglessInt(container.childType, value)
            container.prepareNextChildType()
        }
    }

    override fun writeFloat(value: Double) {
        val container = containers.peek()
        if (container.childIsTaggedValue()) {
            writeTaggedValueFieldNameAndAnnotations()
            userData.writeFloat(value)
        } else {
            userData.writeTaglessFloat(container.childType, value)
            container.prepareNextChildType()
        }
    }

    override fun writeDecimal(value: BigDecimal?) {
        val container = containers.peek()
        if (container.childIsTaggedValue()) {
            writeTaggedValueFieldNameAndAnnotations()
            value.writeMaybeNull(IonType.DECIMAL, userData::writeDecimal)
        } else {
            TODO("Tagless decimals")
        }
    }

    override fun writeTimestamp(value: Timestamp?) {
        val container = containers.peek()
        if (container.childIsTaggedValue()) {
            writeTaggedValueFieldNameAndAnnotations()
            value.writeMaybeNull(IonType.TIMESTAMP, userData::writeTimestamp)
        } else {
            TODO("Tagless timestamps")
        }
    }

    override fun writeString(value: String?) {
        writeTaggedValueFieldNameAndAnnotations()
        value.writeMaybeNull(IonType.STRING, userData::writeString)
    }

    override fun writeClob(value: ByteArray?) {
        writeTaggedValueFieldNameAndAnnotations()
        value.writeMaybeNull(IonType.CLOB, userData::writeClob)
    }

    override fun writeClob(value: ByteArray?, start: Int, len: Int) {
        writeTaggedValueFieldNameAndAnnotations()
        value.writeMaybeNull(IonType.CLOB) { userData.writeClob(it, start, len) }
    }

    override fun writeBlob(value: ByteArray?) {
        writeTaggedValueFieldNameAndAnnotations()
        value.writeMaybeNull(IonType.BLOB, userData::writeBlob)
    }

    override fun writeBlob(value: ByteArray?, start: Int, len: Int) {
        writeTaggedValueFieldNameAndAnnotations()
        value.writeMaybeNull(IonType.BLOB) { userData.writeBlob(it, start, len) }
    }

    override fun isFieldNameSet(): Boolean {
        return fieldNameId >= 0 || fieldNameText != null
    }

    override fun getDepth(): Int {
        return userData.depth()
    }

    override fun writeIonVersionMarker() {
        if (depth == 0) {
            // Make sure we write out any symbol tables and buffered values before the IVM
            finish()
        } else {
            throw IonException("A version marker may only be written at the top level of the data stream.")
        }
    }

    @Deprecated("Use IonValue.writeTo(IonWriter) instead.")
    override fun writeValue(value: IonValue) = value.writeTo(this)

    @Deprecated("Use writeTimestamp instead.")
    override fun writeTimestampUTC(value: Date?) {
        TODO()
    }

    override fun isStreamCopyOptimized(): Boolean = false

    override fun writeValues(reader: IonReader) {
        TODO()
    }

    override fun writeValue(reader: IonReader) {
        TODO()
    }

    // Stream termination

    override fun close() {
        flush()
        systemData.close()
        userData.close()
        onClose.run()
    }

    override fun flush() {
        if (depth != 0) throw IllegalStateException("Cannot call flush() while stepped in any value.")
        if (needsIVM) {
            systemData.writeIVM()
            needsIVM = false
        }
        encodingContextManager.writeEncodingDirective(systemData)
        systemData.flush()
        userData.flush()
    }

    override fun finish() {
        if (depth != 0) throw IllegalStateException("Cannot call finish() while stepped in any value.")
        flush()
        encodingContextManager.reset()
        needsIVM = true
    }

    override fun startMacro(macro: Macro) {
        require(macro is MacroImpl) { "Provided macro is not an Ion 1.1 macro." }
        val address = encodingContextManager.getOrAssignMacroAddress(macro, null)
        startMacro(encodingContextManager.getMacroNameForId(address), address, macro)
    }

    override fun startMacro(name: String, macro: Macro) {
        require(macro is MacroImpl) { "Provided macro is not an Ion 1.1 macro." }
        val address = encodingContextManager.getOrAssignMacroAddress(macro, name.takeIf { options.useMacroNames })
        startMacro(name, address, macro)
    }

    private fun startMacro(name: String?, address: Int, definition: MacroImpl) {
        val container = containers.peek()
        if (container.childIsTaggedValue()) {
            writeTaggedValueFieldNameAndAnnotations()
            if (options.useMacroNames && name != null) {
                userData.stepInEExp(name)
            } else {
                val includeLengthPrefix = if (definition.signature.isEmpty()) {
                    false
                } else {
                    options.writeLengthPrefix(ContainerType.EEXP, depth + 1)
                }
                userData.stepInEExp(address, includeLengthPrefix)
            }
        } else if (container.childIsTaglessMacroShaped()) {
            // TODO: Look up names, etc. to improve the message, if the macro is incorrect.
            if (address != container.childMacroId) throw IonException("Incorrect tagless macro.")
            container.prepareNextChildType() // So it's ready when we step out of the tagless EExp.
            userData.stepInTaglessEExp()
        } else {
            throw IonException("Cannot write a macro invocation when tagless ${container.expectedChildTypeName()} is expected")
        }
        containers.push { it.resetForMacroInvocation(definition.signature) }
    }

    override fun endMacro() {
        val currentContainer = containers.peek()
        if (currentContainer.isMacroInvocation()) {
            val numArgsProvided = currentContainer.i
            // If there are any missing parameters, attempt to write absent argument for them.
            while (currentContainer.prepareNextChildType()) {
                if (currentContainer.childIsTaggedValue()) {
                    absentArgument()
                } else {
                    throw IonException("Expected ${currentContainer.signature.size} arguments, but only $numArgsProvided were given.")
                }
            }
        }
        userData.stepOut()
        containers.pop()
    }

    override fun absentArgument() {
        val container = containers.peek()
        if (container.childType != ContainerInfo.TAGGED_VALUE) throw IonException("The argument corresponding to a tagless placeholder cannot be absent.")
        userData.writeAbsentArgument()
        container.prepareNextChildType()
    }

    /** Visible for testing */
    internal fun getOrAssignMacroAddress(macro: MacroImpl): Int = encodingContextManager.getOrAssignMacroAddress(macro, null)
}
