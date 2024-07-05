// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.*
import com.amazon.ion.impl.*
import com.amazon.ion.impl.bin.IonEncoder_1_1.*
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1.ContainerType.*
import com.amazon.ion.impl.bin.Ion_1_1_Constants.*
import com.amazon.ion.impl.bin.utf8.*
import com.amazon.ion.impl.macro.*
import com.amazon.ion.util.*
import java.io.OutputStream
import java.lang.Double.doubleToRawLongBits
import java.lang.Float.floatToIntBits
import java.math.BigDecimal
import java.math.BigInteger

class IonRawBinaryWriter_1_1 internal constructor(
    private val out: OutputStream,
    private val buffer: WriteBuffer,
    private val lengthPrefixPreallocation: Int,
) : IonRawWriter_1_1 {

    /**
     * Types of encoding containers.
     */
    enum class ContainerType {
        LIST,
        SEXP,
        STRUCT,
        EEXP,
        EXPR_GROUP,
        /**
         * Represents the top level stream. The [containerStack] always has [ContainerInfo] for [TOP] at the bottom
         * of the stack so that we never have to check if [currentContainer] is null.
         *
         * TODO: Test if performance is better if we just check currentContainer for nullness.
         */
        TOP,
        /**
         * Represents a group of annotations. May only contain FlexSyms or FlexUInt symbol IDs.
         */
        ANNOTATIONS,
    }

    private class ContainerInfo(
        var type: ContainerType? = null,
        var isDelimited: Boolean = false,
        var usesFlexSym: Boolean = false,
        var position: Long = -1,
        /**
         * Where should metadata such as the length prefix and/or the presence bitmap be written,
         * relative to the start of this container.
         */
        var metadataOffset: Int = 1,
        /**
         * The number of bytes for everything following the length-prefix (if applicable) in this container.
         */
        var length: Long = 0,
        // TODO: Test if performance is better with an Object Reference or an index into the PatchPoint queue.
        var patchPoint: PatchPoint? = null,
        /**
         * The number of elements in the expression group or arguments to the macro.
         * This is updated when _finishing_ writing a value or expression group.
         */
        var numChildren: Int = 0,
        /**
         * The primitive type to use if this is a tagless expression group.
         */
        var primitiveType: PrimitiveType? = null
    ) {
        /**
         * Clears this [ContainerInfo] of old data and initializes it with the given new data.
         */
        fun reset(type: ContainerType, position: Long, isDelimited: Boolean = false, metadataOffset: Int = 1) {
            this.type = type
            this.isDelimited = isDelimited
            this.position = position
            this.metadataOffset = metadataOffset
            usesFlexSym = false
            length = 0
            patchPoint = null
            numChildren = 0
        }
    }

    companion object {
        /** Flag to indicate that annotations need to be written using FlexSyms */
        private const val FLEX_SYMS_REQUIRED = -1

        /**
         * Annotations container always requires at least one length prefix byte. In practice, it's almost certain to
         * never require more than one byte for SID annotations. We assume that it will infrequently require more than
         * one byte for FlexSym annotations.
         */
        private const val ANNOTATIONS_LENGTH_PREFIX_ALLOCATION_SIZE = 1
    }

    private val utf8StringEncoder = Utf8StringEncoderPool.getInstance().getOrCreate()

    private var annotationsTextBuffer = arrayOfNulls<CharSequence>(8)
    private var annotationsIdBuffer = IntArray(8)
    private var numAnnotations = 0
    /**
     * Flag indicating whether to use FlexSyms to write the annotations. When FlexSyms are required, the flag should be
     * set to `-1` so that we can `xor` it with [numAnnotations] to get a distinct integer that represents the number
     * and type of annotations required.
     */
    private var annotationFlexSymFlag = 0

    private var hasFieldName = false

    private var closed = false

    private val patchPoints = _Private_RecyclingQueue(512) { PatchPoint() }
    private val containerStack = _Private_RecyclingStack(8) { ContainerInfo() }
    private val presenceBitmapStack = _Private_RecyclingStack(8) { PresenceBitmap() }

    private var currentContainer: ContainerInfo = containerStack.push { it.reset(TOP, 0L) }

    override fun finish() {
        if (closed) return
        confirm(depth() == 0) { "Cannot call finish() while in a container" }
        confirm(numAnnotations == 0) { "Cannot call finish with dangling annotations" }

        if (patchPoints.isEmpty) {
            // nothing to patch--write 'em out!
            buffer.writeTo(out)
        } else {
            var bufferPosition: Long = 0

            // Patch length values are long, so they always fit in 10 bytes or fewer.
            val flexUIntScratch = ByteArray(10)

            val iterator = patchPoints.iterate()

            while (iterator.hasNext()) {
                val patch = iterator.next()
                if (patch.length < 0) {
                    continue
                }
                // write up to the thing to be patched
                val bufferLength = patch.oldPosition - bufferPosition
                buffer.writeTo(out, bufferPosition, bufferLength)

                // write out the patch
                // TODO: See if there's a measurable performance benefit if we write directly to the output stream vs using the flexUIntScratch
                val numBytes = FlexInt.flexUIntLength(patch.length)
                FlexInt.writeFlexIntOrUIntInto(flexUIntScratch, 0, patch.length, numBytes)
                out.write(flexUIntScratch, 0, numBytes)

                // skip over the preallocated field
                bufferPosition = patch.oldPosition
                bufferPosition += patch.oldLength.toLong()
            }
            buffer.writeTo(out, bufferPosition, buffer.position() - bufferPosition)
        }

        buffer.reset()
        patchPoints.clear()

        // TODO: Stream flush mode
    }

    override fun close() {
        if (closed) return
        finish()
        buffer.close()
        closed = true
    }

    override fun depth(): Int = containerStack.size() - 1 // "Top" doesn't count when counting depth.

    override fun isInStruct(): Boolean = currentContainer.type == STRUCT

    override fun writeIVM() {
        confirm(currentContainer.type == TOP) { "IVM can only be written at the top level of an Ion stream." }
        confirm(numAnnotations == 0) { "Cannot write an IVM with annotations" }
        buffer.writeBytes(_Private_IonConstants.BINARY_VERSION_MARKER_1_1)
    }

    /**
     * Ensures that there is enough space in the annotation buffers for [n] annotations.
     * If more space is needed, it over-allocates by 8 to ensure that we're not continually allocating when annotations
     * are being added one by one.
     */
    private /*inline*/ fun ensureAnnotationSpace(n: Int) {
        if (annotationsIdBuffer.size < n || annotationsTextBuffer.size < n) {
            val oldIds = annotationsIdBuffer
            annotationsIdBuffer = IntArray(n + 8)
            oldIds.copyInto(annotationsIdBuffer)
            val oldText = annotationsTextBuffer
            annotationsTextBuffer = arrayOfNulls(n + 8)
            oldText.copyInto(annotationsTextBuffer)
        }
    }

    override fun writeAnnotations(annotation0: Int) {
        confirm(annotation0 >= 0) { "Invalid SID: $annotation0" }
        ensureAnnotationSpace(numAnnotations + 1)
        annotationsIdBuffer[numAnnotations++] = annotation0
    }

    override fun writeAnnotations(annotation0: Int, annotation1: Int) {
        confirm(annotation0 >= 0 && annotation1 >= 0) { "One or more invalid SIDs: $annotation0, $annotation1" }
        ensureAnnotationSpace(numAnnotations + 2)
        annotationsIdBuffer[numAnnotations++] = annotation0
        annotationsIdBuffer[numAnnotations++] = annotation1
    }

    override fun writeAnnotations(annotations: IntArray) {
        confirm(annotations.all { it >= 0 }) { "One or more invalid SIDs: ${annotations.filter { it < 0 }.joinToString()}" }
        ensureAnnotationSpace(numAnnotations + annotations.size)
        annotations.copyInto(annotationsIdBuffer, numAnnotations)
        numAnnotations += annotations.size
    }

    override fun writeAnnotations(annotation0: CharSequence) {
        ensureAnnotationSpace(numAnnotations + 1)
        annotationsTextBuffer[numAnnotations++] = annotation0
        annotationFlexSymFlag = FLEX_SYMS_REQUIRED
    }

    override fun writeAnnotations(annotation0: CharSequence, annotation1: CharSequence) {
        ensureAnnotationSpace(numAnnotations + 2)
        annotationsTextBuffer[numAnnotations++] = annotation0
        annotationsTextBuffer[numAnnotations++] = annotation1
        annotationFlexSymFlag = FLEX_SYMS_REQUIRED
    }

    override fun writeAnnotations(annotations: Array<CharSequence>) {
        if (annotations.isEmpty()) return
        ensureAnnotationSpace(numAnnotations + annotations.size)
        annotations.copyInto(annotationsTextBuffer, numAnnotations)
        numAnnotations += annotations.size
        annotationFlexSymFlag = FLEX_SYMS_REQUIRED
    }

    override fun _private_clearAnnotations() {
        numAnnotations = 0
        annotationFlexSymFlag = 0
        // erase the first entries to ensure old values don't leak into `_private_hasFirstAnnotation()`
        annotationsIdBuffer[0] = -1
        annotationsTextBuffer[0] = null
    }

    override fun _private_hasFirstAnnotation(sid: Int, text: String?): Boolean {
        if (numAnnotations == 0) return false
        if (sid >= 0 && annotationsIdBuffer[0] == sid) {
            return true
        }
        if (text != null && annotationsTextBuffer[0] == text) {
            return true
        }
        return false
    }

    /**
     * Helper function for handling annotations and field names when starting a value.
     */
    private /*inline*/ fun openValue(valueWriterExpression: () -> Unit) {

        if (isInStruct()) {
            confirm(hasFieldName) { "Values in a struct must have a field name." }
        } else if (currentContainer.type == EEXP) {
            presenceBitmapStack.peek()[currentContainer.numChildren] = PresenceBitmap.EXPRESSION
        }

        // Start at 1, assuming there's an annotations OpCode byte.
        // We'll clear this if there are no annotations.
        var annotationsTotalLength = 1L

        // Effect of the xor: if annotationsFlexSymFlag is -1, then we're matching `-1 * numAnnotations - 1`
        when (numAnnotations xor annotationFlexSymFlag) {
            0, -1 -> annotationsTotalLength = 0
            1 -> {
                buffer.writeByte(OpCodes.ANNOTATIONS_1_SYMBOL_ADDRESS)
                annotationsTotalLength += buffer.writeFlexUInt(annotationsIdBuffer[0])
            }
            2 -> {
                buffer.writeByte(OpCodes.ANNOTATIONS_2_SYMBOL_ADDRESS)
                annotationsTotalLength += buffer.writeFlexUInt(annotationsIdBuffer[0])
                annotationsTotalLength += buffer.writeFlexUInt(annotationsIdBuffer[1])
            }
            -2 -> {
                // If there's only one annotation, and we know that at least one has text, we don't need to check
                // whether this is SID.
                buffer.writeByte(OpCodes.ANNOTATIONS_1_FLEX_SYM)
                annotationsTotalLength += buffer.writeFlexSym(utf8StringEncoder.encode(annotationsTextBuffer[0].toString()))
                annotationsTextBuffer[0] = null
            }
            -3 -> {
                buffer.writeByte(OpCodes.ANNOTATIONS_2_FLEX_SYM)
                annotationsTotalLength += writeFlexSymFromAnnotationsBuffer(0)
                annotationsTotalLength += writeFlexSymFromAnnotationsBuffer(1)
            }
            else -> annotationsTotalLength += writeManyAnnotations()
        }
        currentContainer.length += annotationsTotalLength

        numAnnotations = 0
        annotationFlexSymFlag = 0
        hasFieldName = false
        valueWriterExpression()
    }

    /**
     * Writes a FlexSym annotation for the specified position in the annotations buffers.
     */
    private fun writeFlexSymFromAnnotationsBuffer(i: Int): Int {
        val annotationText = annotationsTextBuffer[i]
        return if (annotationText != null) {
            annotationsTextBuffer[i] = null
            buffer.writeFlexSym(utf8StringEncoder.encode(annotationText.toString()))
        } else {
            buffer.writeFlexSym(annotationsIdBuffer[i])
        }
    }

    /**
     * Writes 3 or more annotations for SIDs or FlexSyms
     */
    private fun writeManyAnnotations(): Long {
        currentContainer = containerStack.push { it.reset(ANNOTATIONS, position = buffer.position()) }
        if (annotationFlexSymFlag == FLEX_SYMS_REQUIRED) {
            buffer.writeByte(OpCodes.ANNOTATIONS_MANY_FLEX_SYM)
            buffer.reserve(ANNOTATIONS_LENGTH_PREFIX_ALLOCATION_SIZE)
            for (i in 0 until numAnnotations) {
                currentContainer.length += writeFlexSymFromAnnotationsBuffer(i)
            }
        } else {
            buffer.writeByte(OpCodes.ANNOTATIONS_MANY_SYMBOL_ADDRESS)
            buffer.reserve(ANNOTATIONS_LENGTH_PREFIX_ALLOCATION_SIZE)
            for (i in 0 until numAnnotations) {
                currentContainer.length += buffer.writeFlexUInt(annotationsIdBuffer[i])
            }
        }

        val numAnnotationsBytes = currentContainer.length
        val numLengthPrefixBytes = writeCurrentContainerLength(ANNOTATIONS_LENGTH_PREFIX_ALLOCATION_SIZE)

        // Set the new current container
        containerStack.pop()
        currentContainer = containerStack.peek()

        return numLengthPrefixBytes + numAnnotationsBytes
    }

    /**
     * Helper function for writing scalar values that builds on [openValue] and also includes updating
     * the length of the current container.
     *
     * @param valueWriterExpression should be a function that writes the scalar value to the buffer, and
     *                              returns the number of bytes that were written.
     */
    private /*inline*/ fun writeScalar(valueWriterExpression: () -> Int) = openValue {
        val numBytesWritten = valueWriterExpression()
        currentContainer.length += numBytesWritten
        currentContainer.numChildren++
    }

    /**
     * Helper function for writing scalar values that could be tagless.
     *
     * @param ifTagged should be a function that writes the scalar value to the buffer, and returns the number of bytes that were written.
     * @param ifTagless should be a function that writes the scalar value to the buffer _without an opcode_ and returns the number of bytes that were written.
     */
    private /*inline*/ fun writeTaggedOrTaglessScalar(
        ifTagged: () -> Int,
        ifTagless: (PrimitiveType) -> Int,
    ) {
        val primitiveType = when (currentContainer.type) {
            EEXP -> presenceBitmapStack.peek().signature[currentContainer.numChildren].type.primitiveType
            EXPR_GROUP -> currentContainer.primitiveType
            else -> null
        }
        if (primitiveType != null) {
            confirm(numAnnotations == 0) { "Tagless values cannot be annotated" }
            if (currentContainer.type == EEXP) {
                presenceBitmapStack.peek()[currentContainer.numChildren] = PresenceBitmap.EXPRESSION
            }
            val numBytesWritten = ifTagless(primitiveType)
            currentContainer.length += numBytesWritten
            currentContainer.numChildren++
        } else {
            writeScalar { ifTagged() }
        }
    }

    override fun writeFieldName(sid: Int) {
        confirm(currentContainer.type == STRUCT) { "Can only write a field name inside of a struct." }
        if (sid == 0 && !currentContainer.usesFlexSym) switchCurrentStructToFlexSym()

        currentContainer.length += if (currentContainer.usesFlexSym) {
            buffer.writeFlexSym(sid)
        } else {
            buffer.writeFlexUInt(sid)
        }
        hasFieldName = true
    }

    override fun writeFieldName(text: CharSequence) {
        confirm(currentContainer.type == STRUCT) { "Can only write a field name inside of a struct." }
        if (!currentContainer.usesFlexSym) switchCurrentStructToFlexSym()

        currentContainer.length += buffer.writeFlexSym(utf8StringEncoder.encode(text.toString()))
        hasFieldName = true
    }

    override fun _private_hasFieldName(): Boolean = hasFieldName

    private fun switchCurrentStructToFlexSym() {
        // To switch, we need to insert the sid-to-flexsym switch marker.
        buffer.writeByte(SID_TO_FLEX_SYM_SWITCH_MARKER)
        currentContainer.length += 1
        currentContainer.usesFlexSym = true
    }

    override fun writeNull() = writeScalar { writeNullValue(buffer, IonType.NULL) }

    override fun writeNull(type: IonType) = writeScalar { writeNullValue(buffer, type) }

    override fun writeBool(value: Boolean) = writeScalar { writeBoolValue(buffer, value) }

    override fun writeInt(value: Long) = writeTaggedOrTaglessScalar(
        ifTagged = { writeIntValue(buffer, value) },
        ifTagless = { primitiveType ->
            when (primitiveType) {
                // TODO: Do we need to check bounds?
                PrimitiveType.UINT8 -> buffer.writeFixedIntOrUInt(value, 1)
                PrimitiveType.UINT16 -> buffer.writeFixedIntOrUInt(value, 2)
                PrimitiveType.UINT32 -> buffer.writeFixedIntOrUInt(value, 4)
                PrimitiveType.UINT64 -> buffer.writeFixedIntOrUInt(value, 8)
                PrimitiveType.FLEX_UINT -> buffer.writeFlexUInt(value)
                PrimitiveType.INT8 -> buffer.writeFixedIntOrUInt(value, 1)
                PrimitiveType.INT16 -> buffer.writeFixedIntOrUInt(value, 2)
                PrimitiveType.INT32 -> buffer.writeFixedIntOrUInt(value, 4)
                PrimitiveType.INT64 -> buffer.writeFixedIntOrUInt(value, 8)
                PrimitiveType.FLEX_INT -> buffer.writeFlexInt(value)
                else -> throw IonException("Cannot write an int when the macro signature requires $primitiveType.")
            }
        }
    )

    override fun writeInt(value: BigInteger) = writeTaggedOrTaglessScalar(
        ifTagged = { writeIntValue(buffer, value) },
        ifTagless = { primitiveType ->
            when (primitiveType) {
                // TODO: Do we need to check bounds?
                PrimitiveType.UINT8 -> buffer.writeFixedIntOrUInt(value.toLong(), 1)
                PrimitiveType.UINT16 -> buffer.writeFixedIntOrUInt(value.toLong(), 2)
                PrimitiveType.UINT32 -> buffer.writeFixedIntOrUInt(value.toLong(), 4)
                PrimitiveType.UINT64 -> buffer.writeFixedIntOrUInt(value.toLong(), 8)
                PrimitiveType.FLEX_UINT -> buffer.writeFlexUInt(value)
                PrimitiveType.INT8 -> buffer.writeFixedIntOrUInt(value.toLong(), 1)
                PrimitiveType.INT16 -> buffer.writeFixedIntOrUInt(value.toLong(), 2)
                PrimitiveType.INT32 -> buffer.writeFixedIntOrUInt(value.toLong(), 4)
                PrimitiveType.INT64 -> buffer.writeFixedIntOrUInt(value.toLong(), 8)
                PrimitiveType.FLEX_INT -> buffer.writeFlexInt(value)
                else -> throw IonException("Cannot write an int when the macro signature requires $primitiveType.")
            }
        }
    )

    override fun writeFloat(value: Float) = writeTaggedOrTaglessScalar(
        ifTagged = { writeFloatValue(buffer, value) },
        ifTagless = { primitiveType ->
            when (primitiveType) {
                PrimitiveType.FLOAT16 -> TODO("Writing FLOAT16 not supported yet")
                PrimitiveType.FLOAT32 -> buffer.writeFixedIntOrUInt(floatToIntBits(value).toLong(), 4)
                PrimitiveType.FLOAT64 -> buffer.writeFixedIntOrUInt(doubleToRawLongBits(value.toDouble()), 8)
                else -> throw IonException("Cannot write a float when the macro signature requires $primitiveType.")
            }
        }
    )

    override fun writeFloat(value: Double) = writeTaggedOrTaglessScalar(
        ifTagged = { writeFloatValue(buffer, value) },
        ifTagless = { primitiveType ->
            when (primitiveType) {
                PrimitiveType.FLOAT16 -> TODO("Writing FLOAT16 not supported yet")
                PrimitiveType.FLOAT32 -> buffer.writeFixedIntOrUInt(floatToIntBits(value.toFloat()).toLong(), 4)
                PrimitiveType.FLOAT64 -> buffer.writeFixedIntOrUInt(doubleToRawLongBits(value), 8)
                else -> throw IonException("Cannot write a float when the macro signature requires $primitiveType.")
            }
        }
    )

    override fun writeDecimal(value: BigDecimal) = writeScalar { writeDecimalValue(buffer, value) }

    override fun writeTimestamp(value: Timestamp) = writeScalar { writeTimestampValue(buffer, value) }

    override fun writeSymbol(id: Int) {
        confirm(id >= 0) { "Invalid SID: $id" }
        writeTaggedOrTaglessScalar(
            ifTagged = { writeSymbolValue(buffer, id) },
            ifTagless = { primitiveType ->
                when (primitiveType) {
                    PrimitiveType.COMPACT_SYMBOL -> buffer.writeFlexSym(id)
                    else -> throw IonException("Cannot write a symbol when the macro signature requires $primitiveType.")
                }
            }
        )
    }

    override fun writeSymbol(text: CharSequence) = writeTaggedOrTaglessScalar(
        ifTagged = { writeSymbolValue(buffer, utf8StringEncoder.encode(text.toString())) },
        ifTagless = { primitiveType ->
            when (primitiveType) {
                PrimitiveType.COMPACT_SYMBOL -> buffer.writeFlexSym(utf8StringEncoder.encode(text.toString()))
                else -> throw IonException("Cannot write a symbol when the macro signature requires $primitiveType.")
            }
        }
    )

    override fun writeString(value: CharSequence) = writeScalar { writeStringValue(buffer, utf8StringEncoder.encode(value.toString())) }

    override fun writeBlob(value: ByteArray, start: Int, length: Int) = writeScalar { writeBlobValue(buffer, value, start, length) }

    override fun writeClob(value: ByteArray, start: Int, length: Int) = writeScalar { writeClobValue(buffer, value, start, length) }

    override fun stepInList(delimited: Boolean) {
        openValue {
            currentContainer = containerStack.push { it.reset(LIST, buffer.position(), delimited) }
            if (delimited) {
                buffer.writeByte(OpCodes.DELIMITED_LIST)
            } else {
                buffer.writeByte(OpCodes.VARIABLE_LENGTH_LIST)
                buffer.reserve(lengthPrefixPreallocation)
            }
        }
    }

    override fun stepInSExp(delimited: Boolean) {
        openValue {
            currentContainer = containerStack.push { it.reset(SEXP, buffer.position(), delimited) }
            if (delimited) {
                buffer.writeByte(OpCodes.DELIMITED_SEXP)
            } else {
                buffer.writeByte(OpCodes.VARIABLE_LENGTH_SEXP)
                buffer.reserve(lengthPrefixPreallocation)
            }
        }
    }

    override fun stepInStruct(delimited: Boolean) {
        openValue {
            currentContainer = containerStack.push { it.reset(STRUCT, buffer.position(), delimited) }
            if (delimited) {
                buffer.writeByte(OpCodes.DELIMITED_STRUCT)
                currentContainer.usesFlexSym = true
            } else {
                buffer.writeByte(OpCodes.VARIABLE_LENGTH_STRUCT_WITH_SIDS)
                buffer.reserve(lengthPrefixPreallocation)
            }
        }
    }

    override fun stepInEExp(name: CharSequence) {
        throw UnsupportedOperationException("Binary writer requires macros to be invoked by their ID.")
    }

    // The managed writer should write all arguments, even if they are void.
    // Void can be written as an empty expression group.
    override fun stepInEExp(id: Int, lengthPrefixed: Boolean, macro: Macro) {
        // Length-prefixed e-expression format:
        //     F5 <flexuint-address> <flexuint-length> <presence-bitmap> <args...>
        // Non-length-prefixed e-expression format:
        //     <address/opcode> <presence-bitmap> <args...>
        confirm(numAnnotations == 0) { "Cannot annotate an E-Expression" }

        if (currentContainer.type == STRUCT && !hasFieldName) {
            if (!currentContainer.usesFlexSym) switchCurrentStructToFlexSym()
            buffer.writeByte(FlexInt.ZERO)
            currentContainer.length++
        }

        currentContainer = containerStack.push { it.reset(EEXP, buffer.position(), !lengthPrefixed) }
        println("LengthPrefixOffset = ${currentContainer.metadataOffset}")

        // We use `currentContainer.lengthPrefixOffset` to also keep track of where to write the presence bitmap.
        // It will be written at `currentContainer.lengthPrefixOffset + lengthPrefixPreallocation`

        if (lengthPrefixed) {
            buffer.writeByte(OpCodes.LENGTH_PREFIXED_MACRO_INVOCATION)
            currentContainer.metadataOffset += buffer.writeFlexUInt(id)
            buffer.reserve(lengthPrefixPreallocation)
        } else {
            if (id < 64) {
                buffer.writeByte(id.toByte())
            } else if (id < 4160) {
                val biasedId = id - 64
                val lowNibble = biasedId / 256
                val adjustedId = biasedId % 256L
                buffer.writeByte((OpCodes.BIASED_E_EXPRESSION_ONE_BYTE_FIXED_INT + lowNibble).toByte())
                currentContainer.metadataOffset += buffer.writeFixedUInt(adjustedId)
            } else if (id < 1_052_736) {
                val biasedId = id - 4160
                val lowNibble = biasedId / (256 * 256)
                val adjustedId = biasedId % (256 * 256L)
                buffer.writeByte((OpCodes.BIASED_E_EXPRESSION_TWO_BYTE_FIXED_INT + lowNibble).toByte())
                currentContainer.metadataOffset += buffer.writeFixedIntOrUInt(adjustedId, 2)
            } else {
                buffer.writeByte(OpCodes.E_EXPRESSION_FLEX_UINT)
                currentContainer.metadataOffset += buffer.writeFlexUInt(id)
            }
        }
        println("LengthPrefixOffset = ${currentContainer.metadataOffset}")

        val presenceBits = presenceBitmapStack.push { it.initialize(macro.signature) }
        if (presenceBits.byteSize > 0) {
            // Reserve for presence bits
            buffer.reserve(presenceBits.byteSize)
            currentContainer.length += presenceBits.byteSize
        }

        // No need to clear any of the annotation fields because we already asserted that there are no annotations
        hasFieldName = false
    }

    override fun stepInExpressionGroup(delimited: Boolean) {
        confirm(numAnnotations == 0) { "Cannot annotate an expression group" }
        confirm(currentContainer.type == EEXP) { "Can only create an expression group in a macro invocation" }

        val encoding = presenceBitmapStack.peek().signature[currentContainer.numChildren].type

        currentContainer = containerStack.push { it.reset(EXPR_GROUP, buffer.position(), delimited, metadataOffset = 0) }
        currentContainer.primitiveType = encoding.primitiveType

        if (!delimited || encoding.primitiveType != null) {
            // Delimited tagless groups also need a length, but it's the number of values rather than the number of bytes.
            buffer.reserve(maxOf(1, lengthPrefixPreallocation))
        } else {
            // At the start of a tagged expression group, signals that it is delimited.
            currentContainer.length += buffer.writeFlexUInt(0)
        }
        // No need to clear any of the annotation fields because we already asserted that there are no annotations
    }

    /**
     * Continues the current expression group. In most cases, this is a no-op. When in a tagless, delimited
     * expression group, this finished the current "segment" of the expression group and starts a new segment.
     * If the current segment is empty, this does nothing.
     *
     * TODO: Determine whether this should be called by the managed writer, or if some continuation strategy
     *       should be configured in this class.
     */
    fun continueExpressionGroup() {
        confirm(currentContainer.type == EXPR_GROUP) { "Can only call this method when directly in an expression group." }
        val primitiveType = currentContainer.primitiveType
        if (currentContainer.isDelimited && primitiveType != null && currentContainer.numChildren > 0) {
            var thisContainerTotalLength = currentContainer.length
            thisContainerTotalLength += writeCurrentContainerLength(
                lengthPrefixPreallocation,
                relativePosition = 0,
                lengthToWrite = currentContainer.numChildren.toLong(),
            )
            currentContainer.reset(EXPR_GROUP, buffer.position(), isDelimited = true, metadataOffset = 0)
            currentContainer.primitiveType = primitiveType
            // Carry over the length into the next segment (but not numChildren)
            currentContainer.length = thisContainerTotalLength
            // Reserve for the next pre-allocation
            buffer.reserve(1)
        }
    }

    override fun stepOut() {
        confirm(!hasFieldName) { "Cannot step out with dangling field name." }
        confirm(numAnnotations == 0) { "Cannot step out with dangling annotations." }

        // The length of the current container. By the end of this method, the total must include
        // any opcodes, length prefixes, or other data that is not counted in ContainerInfo.length
        var thisContainerTotalLength: Long = currentContainer.length

        // currentContainer.type is non-null for any initialized ContainerInfo
        when (currentContainer.type.assumeNotNull()) {
            LIST, SEXP, STRUCT -> {
                // Add one byte to account for the op code
                thisContainerTotalLength++
                // Write closing delimiter if we're in a delimited container.
                // Update length prefix if we're in a prefixed container.
                if (currentContainer.isDelimited) {
                    if (isInStruct()) {
                        // Need a 0 FlexInt before the end delimiter
                        buffer.writeByte(FlexInt.ZERO)
                        thisContainerTotalLength += 1
                    }
                    thisContainerTotalLength += 1 // For the end marker
                    buffer.writeByte(OpCodes.DELIMITED_END_MARKER)
                } else {
                    val contentLength = currentContainer.length
                    if (contentLength <= 0xF && !currentContainer.usesFlexSym) {
                        // TODO: Right now, this is skipped if we switch to FlexSym after starting a struct
                        //       because we have no way to differentiate a struct that started as FlexSym
                        //       from a struct that switched to FlexSym.
                        // Clean up any unused space that was pre-allocated.
                        buffer.shiftBytesLeft(currentContainer.length.toInt(), lengthPrefixPreallocation)
                        val zeroLengthOpCode = when (currentContainer.type) {
                            LIST -> OpCodes.LIST_ZERO_LENGTH
                            SEXP -> OpCodes.SEXP_ZERO_LENGTH
                            STRUCT -> OpCodes.STRUCT_SID_ZERO_LENGTH
                            else -> TODO("Unreachable")
                        }
                        buffer.writeByteAt(currentContainer.position, zeroLengthOpCode + contentLength)
                    } else {
                        thisContainerTotalLength += writeCurrentContainerLength(lengthPrefixPreallocation)
                    }
                }
            }
            EEXP -> {
                // Add to account for the opcode and/or address
                thisContainerTotalLength += currentContainer.metadataOffset

                val presenceBitmap = presenceBitmapStack.pop()
                val requiresWritingPresenceBits = presenceBitmap.byteSize > 0
                val presenceBitmapPosition = if (!currentContainer.isDelimited) {
                    // TODO: If the length is 0, see if we can go back and rewrite this as a non-length-prefixed e-exp
                    thisContainerTotalLength += writeCurrentContainerLength(lengthPrefixPreallocation)
                    currentContainer.position + currentContainer.metadataOffset + lengthPrefixPreallocation
                } else {
                    currentContainer.position + currentContainer.metadataOffset
                }

                if (requiresWritingPresenceBits) {
                    presenceBitmap.writeTo(buffer, presenceBitmapPosition)
                }
            }
            EXPR_GROUP -> {
                // Elide empty containers if we're going to be writing a presence bitmap
                // TODO: Consider whether we can rewrite groups that have only one expression as a single expression
                if (currentContainer.length == 0L && presenceBitmapStack.peek().byteSize > 0) {
                    // TODO: This can break if `continueExpressionGroup()` is called.

                    // It is not always safe to truncate like this without clearing the patch points for the
                    // truncated part of the buffer. However, it is safe to do so here because we can only get to
                    // this particular branch if this expression group is empty, ergo it contains no patch points.
                    buffer.truncate(currentContainer.position)
                    thisContainerTotalLength = 0
                } else if (currentContainer.numChildren == 0 && currentContainer.primitiveType != null) {
                    // If we've called `continueExpressionGroup` and the `stepOut` without adding any more items.
                    buffer.truncate(currentContainer.position)
                    thisContainerTotalLength += buffer.writeFlexUInt(0)
                } else {
                    // Cases:
                    //   - Delimited, tagged -- start with `01` end with `F0`
                    //   - Length-prefixed tagged -- write the number of bytes
                    //   - Tagless -- write the number of expressions, end with FlexUInt 0
                    val isTagless = currentContainer.primitiveType != null
                    if (isTagless) {
                        thisContainerTotalLength += writeCurrentContainerLength(
                            lengthPrefixPreallocation,
                            relativePosition = 0,
                            lengthToWrite = currentContainer.numChildren.toLong(),
                        )
                        buffer.writeFlexUInt(0)
                        thisContainerTotalLength++
                    } else if (currentContainer.isDelimited) {
                        buffer.writeByte(OpCodes.DELIMITED_END_MARKER)
                        thisContainerTotalLength++
                    } else {
                        thisContainerTotalLength += writeCurrentContainerLength(lengthPrefixPreallocation)
                    }
                }
            }
            ANNOTATIONS -> TODO("Unreachable.")
            TOP -> throw IonException("Nothing to step out of.")
        }

        // Set the new current container
        val justExitedContainer = containerStack.pop()
        currentContainer = containerStack.peek()

        if (currentContainer.type == EEXP) {
            presenceBitmapStack.peek()[currentContainer.numChildren] = when (justExitedContainer.type) {
                LIST, SEXP, STRUCT, EEXP -> PresenceBitmap.EXPRESSION
                EXPR_GROUP -> if (thisContainerTotalLength == 0L) PresenceBitmap.VOID else PresenceBitmap.GROUP
                else -> TODO("Unreachable")
            }
        }

        // Update the length of the new current container to include the length of the container that we just stepped out of.
        currentContainer.length += thisContainerTotalLength
        currentContainer.numChildren++
    }

    /**
     * Writes the length of the current container and returns the number of bytes needed to do so.
     * Transparently handles PatchPoints as necessary.
     *
     * @param numPreAllocatedLengthPrefixBytes the number of bytes that were pre-allocated for the length prefix of the
     *                                         current container.
     * @param relativePosition the position to write the length relative to the start of the current container (which
     *                         includes the opcode, if any).
     */
    private fun writeCurrentContainerLength(numPreAllocatedLengthPrefixBytes: Int, relativePosition: Int = currentContainer.metadataOffset, lengthToWrite: Long = currentContainer.length): Int {
        val e = Exception().apply { fillInStackTrace() }.stackTrace.first { "writeCurrentContainerLength" !in it.toString() }

        val lengthPosition = currentContainer.position + relativePosition
        println("Writing length $lengthToWrite at position $lengthPosition\n    called from $e")
        val lengthPrefixBytesRequired = FlexInt.flexUIntLength(lengthToWrite)
        if (lengthPrefixBytesRequired == numPreAllocatedLengthPrefixBytes) {
            // We have enough space, so write in the correct length.
            buffer.writeFlexIntOrUIntAt(lengthPosition, lengthToWrite, lengthPrefixBytesRequired)
        } else {
            addPatchPointsToStack()
            // All ContainerInfos are in the stack, so we know that its patchPoint is non-null.
            currentContainer.patchPoint.assumeNotNull().apply {
                oldPosition = lengthPosition
                oldLength = numPreAllocatedLengthPrefixBytes
                length = lengthToWrite
            }
        }
        return lengthPrefixBytesRequired
    }

    private fun addPatchPointsToStack() {
        // TODO: We may be able to improve this by skipping patch points on ancestors that are delimited containers,
        //       since the patch points for delimited containers will go unused anyway. However, the additional branching
        //       may negate the effect of any reduction in allocations.

        // If we're adding a patch point we first need to ensure that all of our ancestors (containing values) already
        // have a patch point. No container can be smaller than the contents, so all outer layers also require patches.
        // Instead of allocating iterator, we share one iterator instance within the scope of the container stack and
        // reset the cursor every time we track back to the ancestors.
        val stackIterator: ListIterator<ContainerInfo> = containerStack.iterator()
        // Walk down the stack until we find an ancestor which already has a patch point
        while (stackIterator.hasNext() && stackIterator.next().patchPoint == null);

        // The iterator cursor is now positioned on an ancestor container that has a patch point
        // Ascend back up the stack, fixing the ancestors which need a patch point assigned before us
        while (stackIterator.hasPrevious()) {
            val ancestor = stackIterator.previous()
            if (ancestor.patchPoint == null) {
                ancestor.patchPoint = patchPoints.pushAndGet { it.clear() }
            }
        }
    }
}
