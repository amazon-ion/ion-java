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

    // Stores info about the types of the child elements.
    private class ContainerInfo(
        @JvmField var signature: Array<MacroImpl.Parameter> = EMPTY_ARRAY,
        @JvmField var type: Int = -1,
        /**
         // If i>=-1, type is an opcode
         // If i==-2, there is no type
         // If i==-3, type is a macro id
         */
        @JvmField var i: Int = 0,
    ) {

        companion object {
            @JvmStatic
            private val EMPTY_ARRAY = emptyArray<MacroImpl.Parameter>()
        }

        val macroId: Int get() = -1 - type

        fun reset(signature: Array<MacroImpl.Parameter>) {
            this.signature = signature
            i = 0
            prepareNextType()
        }

        fun resetWithMacroShape(macroId: Int) {
            this.signature = EMPTY_ARRAY
            this.type = -1 - macroId
            i = -3
        }

        fun reset(opcode: Int = 0) {
            this.signature = EMPTY_ARRAY
            this.type = opcode
            i = if (opcode == 0) -2 else -1
        }

        fun prepareNextType() {
            if (i >= 0) {
                if (i < signature.size) {
                    type = signature[i++].opcode
                } else {
                    type = OpCode.DELIMITED_CONTAINER_END
                }
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

    private fun _private_hasFirstAnnotation(sid: Int, text: String?): Boolean {
        if (numAnnotations == 0) return false
        if (sid >= 0 && annotationsIdBuffer[0] == sid) {
            return true
        }
        if (text != null && annotationsTextBuffer[0] == text) {
            return true
        }
        return false
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
            IonType.LIST -> prepareFieldNameAndAnnotations { userData.stepInList(options.writeLengthPrefix(ContainerType.LIST, newDepth)) }
            IonType.SEXP -> prepareFieldNameAndAnnotations { userData.stepInSExp(options.writeLengthPrefix(ContainerType.SEXP, newDepth)) }
            IonType.STRUCT -> {
                if (depth == 0 && _private_hasFirstAnnotation(SystemSymbols.ION_SYMBOL_TABLE_SID, SystemSymbols.ION_SYMBOL_TABLE)) {
                    // TODO: Should we throw here, or turn this into a no-op?
                    throw IonException("User-defined symbol tables not permitted by the Ion 1.1 managed writer.")
                }
                prepareFieldNameAndAnnotations {
                    userData.stepInStruct(options.writeLengthPrefix(ContainerType.STRUCT, newDepth))
                }
            }
            else -> throw IllegalArgumentException("Not a container type: $containerType")
        }
        containers.push { it.reset() }
    }

    override fun stepInTaglessElementList(name: String?, macro: Macro) = prepareFieldNameAndAnnotations {
        require(macro is MacroImpl) { "Provided macro is not an Ion 1.1 macro." }
        val macroId = if (name == null) encodingContextManager.getOrAssignMacroAddress(macro) else encodingContextManager.getOrAssignMacroAddressAndName(name, macro)
        userData.stepInTaglessElementList(macroId, name, lengthPrefixed = options.writeLengthPrefix(ContainerType.EEXP, macroId))
        containers.push { it.resetWithMacroShape(macroId) }
    }

    override fun stepInTaglessElementList(scalar: TaglessScalarType) = prepareFieldNameAndAnnotations {
        userData.stepInTaglessElementList(scalar.getOpcode())
        containers.push { it.reset(scalar.getOpcode()) }
    }

    override fun stepInTaglessElementSExp(name: String?, macro: Macro) = prepareFieldNameAndAnnotations {
        require(macro is MacroImpl) { "Provided macro is not an Ion 1.1 macro." }
        val macroId = if (name == null) encodingContextManager.getOrAssignMacroAddress(macro) else encodingContextManager.getOrAssignMacroAddressAndName(name, macro)
        userData.stepInTaglessElementSExp(macroId, name, lengthPrefixed = options.writeLengthPrefix(ContainerType.EEXP, macroId))
        containers.push { it.resetWithMacroShape(macroId) }
    }
    override fun stepInTaglessElementSExp(scalar: TaglessScalarType) = prepareFieldNameAndAnnotations {
        userData.stepInTaglessElementSExp(scalar.getOpcode())
        containers.push { it.reset(scalar.getOpcode()) }
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

    private inline fun prepareFieldNameAndAnnotations(valueWriterExpression: () -> Unit) {
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
        valueWriterExpression()
    }

    override fun writeSymbol(content: String?) = prepareFieldNameAndAnnotations {
        val container = containers.peek()
        if (container.type != 0) {
            content ?: throw IonException("Cannot write null.symbol for tagless symbol")
            if (container.type != OpCode.TE_SYMBOL_FS) {
                val expectedType = TaglessScalarType.getTaglessScalarTypeForOpcode(container.type) ?: "e-expression"
                throw IllegalArgumentException("Cannot write a symbol when a tagless $expectedType is expected.")
            }
            if (options.shouldWriteInline(SymbolKind.VALUE, content)) {
                userData.writeTaglessSymbol(container.type, content)
            } else {
                userData.writeTaglessSymbol(container.type, encodingContextManager.intern(content))
            }
            container.prepareNextType()
        } else if (content == null) {
            userData.writeNull(IonType.SYMBOL)
        } else {
            if (content == SystemSymbols.ION_1_0 && depth == 0) throw IonException("Can't write a top-level symbol that is the same as the IVM.")
            handleSymbolToken(UNKNOWN_SYMBOL_ID, content, SymbolKind.VALUE, userData)
        }
    }

    override fun writeSymbolToken(content: SymbolToken?) = prepareFieldNameAndAnnotations {
        val container = containers.peek()
        if (content?.text != null) return writeSymbol(content.text)

        if (container.type != 0) {
            content ?: throw IonException("Cannot write null.symbol for tagless symbol")
            if (container.type != OpCode.TE_SYMBOL_FS) {
                val expectedType = TaglessScalarType.getTaglessScalarTypeForOpcode(container.type) ?: "e-expression"
                throw IllegalArgumentException("Cannot write a symbol when a tagless $expectedType is expected.")
            }
            // content.text is already known to be null
            userData.writeTaglessSymbol(container.type, 0)
            container.prepareNextType()
        } else if (content == null) {
            userData.writeNull(IonType.SYMBOL)
        } else {
            // TODO: Check to see if the SID refers to a user symbol with text that looks like an IVM
            handleSymbolToken(content.sid, content.text, SymbolKind.VALUE, userData)
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

    override fun writeNull() = prepareFieldNameAndAnnotations { userData.writeNull() }
    override fun writeNull(type: IonType?) = prepareFieldNameAndAnnotations { userData.writeNull(type ?: IonType.NULL) }
    override fun writeBool(value: Boolean) = prepareFieldNameAndAnnotations { userData.writeBool(value) }
    override fun writeInt(value: Long) = prepareFieldNameAndAnnotations {
        val container = containers.peek()
        if (container.type != 0) {
            userData.writeTaglessInt(container.type, value)
            container.prepareNextType()
        } else {
            userData.writeInt(value)
        }
    }

    // TODO: Tagless int support for BigIntegers
    override fun writeInt(value: BigInteger?) = prepareFieldNameAndAnnotations { value.writeMaybeNull(IonType.INT, userData::writeInt) }

    override fun writeFloat(value: Double) = prepareFieldNameAndAnnotations {
        val container = containers.peek()
        if (container.type != 0) {
            userData.writeTaglessFloat(container.type, value)
            container.prepareNextType()
        } else {
            userData.writeFloat(value)
        }
    }
    override fun writeDecimal(value: BigDecimal?) = prepareFieldNameAndAnnotations { value.writeMaybeNull(IonType.DECIMAL, userData::writeDecimal) }
    override fun writeTimestamp(value: Timestamp?) = prepareFieldNameAndAnnotations { value.writeMaybeNull(IonType.TIMESTAMP, userData::writeTimestamp) }
    override fun writeString(value: String?) = prepareFieldNameAndAnnotations { value.writeMaybeNull(IonType.STRING, userData::writeString) }

    override fun writeClob(value: ByteArray?) = prepareFieldNameAndAnnotations { value.writeMaybeNull(IonType.CLOB, userData::writeClob) }
    override fun writeClob(value: ByteArray?, start: Int, len: Int) = prepareFieldNameAndAnnotations { value.writeMaybeNull(IonType.CLOB) { userData.writeClob(it, start, len) } }

    override fun writeBlob(value: ByteArray?) = prepareFieldNameAndAnnotations { value.writeMaybeNull(IonType.BLOB, userData::writeBlob) }
    override fun writeBlob(value: ByteArray?, start: Int, len: Int) = prepareFieldNameAndAnnotations { value.writeMaybeNull(IonType.BLOB) { userData.writeBlob(it, start, len) } }

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
            writeSymbol("\$ion_1_1")
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
        val address = encodingContextManager.getOrAssignMacroAddress(macro)
        startMacro(encodingContextManager.getMacroNameForId(address), address, macro)
    }

    override fun startMacro(name: String, macro: Macro) {
        require(macro is MacroImpl) { "Provided macro is not an Ion 1.1 macro." }
        val address = encodingContextManager.getOrAssignMacroAddressAndName(name, macro)
        startMacro(name, address, macro)
    }

    private fun startMacro(name: String?, address: Int, definition: MacroImpl) = prepareFieldNameAndAnnotations {
        val container = containers.peek()
        val prescribedMacroType = container.macroId
        if (prescribedMacroType < 0) {
            val useNames =
                options.eExpressionIdentifierStrategy == ManagedWriterOptions_1_1.EExpressionIdentifierStrategy.BY_NAME
            if (useNames && name != null) {
                userData.stepInEExp(name)
            } else {
                val includeLengthPrefix = if (definition.signature.isEmpty()) false else options.writeLengthPrefix(ContainerType.EEXP, depth + 1)
                userData.stepInEExp(address, includeLengthPrefix)
            }
        } else {
            userData.stepInTaglessEExp()
            container.prepareNextType()
        }
        containers.push { it.reset(definition.signature) }
    }

    override fun endMacro() {
        // TODO: See if there are unwritten parameters, and attempt to write `absent arg` for all of them.
        userData.stepOut()
        containers.pop()
    }

    override fun absentArgument() {
        val container = containers.peek()
        if (container.type != 0) throw IonException("The argument corresponding to a tagless placeholder cannot be absent.")
        userData.writeAbsentArgument()
        container.prepareNextType()
    }

    /** Visible for testing */
    internal fun getOrAssignMacroAddress(macro: MacroImpl): Int = encodingContextManager.getOrAssignMacroAddress(macro)
}
