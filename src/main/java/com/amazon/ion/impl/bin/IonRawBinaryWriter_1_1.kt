// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.*
import com.amazon.ion.eexp.*
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
import java.lang.IllegalArgumentException
import java.math.BigDecimal
import java.math.BigInteger

class IonRawBinaryWriter_1_1 internal constructor(
    private val out: OutputStream,
    private val buffer: WriteBuffer,
    private val lengthPrefixPreallocation: Int,
) : IonRawWriter_1_1, PrivateIonRawWriter_1_1 {

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
        var isLengthPrefixed: Boolean = true,
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
         * The kind of tagless encoding to use if this is a tagless expression group.
         */
        var taglessEncodingKind: TaglessEncoding? = null
    ) {
        /**
         * Clears this [ContainerInfo] of old data and initializes it with the given new data.
         */
        fun reset(type: ContainerType, position: Long, isLengthPrefixed: Boolean = true, metadataOffset: Int = 1) {
            this.type = type
            this.isLengthPrefixed = isLengthPrefixed
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

        /**
         * Create a new instance for the given OutputStream with the given block size and length preallocation.
         */
        @JvmStatic
        fun from(out: OutputStream, blockSize: Int, preallocation: Int): IonRawBinaryWriter_1_1 {
            return IonRawBinaryWriter_1_1(out, WriteBuffer(BlockAllocatorProviders.basicProvider().vendAllocator(blockSize)) {}, preallocation)
        }
    }

    private val utf8StringEncoder = Utf8StringEncoderPool.getInstance().getOrCreate()

    private var annotationsFlexSymBuffer = arrayOfNulls<Any>(8)
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

    override fun flush() {
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
        flush()
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
    private inline fun ensureAnnotationSpace(n: Int) {
        if (annotationsIdBuffer.size < n || annotationsFlexSymBuffer.size < n) {
            val oldIds = annotationsIdBuffer
            annotationsIdBuffer = IntArray(n + 8)
            oldIds.copyInto(annotationsIdBuffer)
            val oldText = annotationsFlexSymBuffer
            annotationsFlexSymBuffer = arrayOfNulls(n + 8)
            oldText.copyInto(annotationsFlexSymBuffer)
        }
    }

    override fun writeAnnotations(annotation0: SystemSymbols_1_1) {
        ensureAnnotationSpace(numAnnotations + 1)
        annotationsFlexSymBuffer[numAnnotations++] = annotation0
        annotationFlexSymFlag = FLEX_SYMS_REQUIRED
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
        annotationsFlexSymBuffer[numAnnotations++] = annotation0
        annotationFlexSymFlag = FLEX_SYMS_REQUIRED
    }

    override fun writeAnnotations(annotation0: CharSequence, annotation1: CharSequence) {
        ensureAnnotationSpace(numAnnotations + 2)
        annotationsFlexSymBuffer[numAnnotations++] = annotation0
        annotationsFlexSymBuffer[numAnnotations++] = annotation1
        annotationFlexSymFlag = FLEX_SYMS_REQUIRED
    }

    override fun writeAnnotations(annotations: Array<CharSequence>) {
        if (annotations.isEmpty()) return
        ensureAnnotationSpace(numAnnotations + annotations.size)
        annotations.copyInto(annotationsFlexSymBuffer, numAnnotations)
        numAnnotations += annotations.size
        annotationFlexSymFlag = FLEX_SYMS_REQUIRED
    }

    override fun _private_clearAnnotations() {
        numAnnotations = 0
        annotationFlexSymFlag = 0
        // erase the first entries to ensure old values don't leak into `_private_hasFirstAnnotation()`
        annotationsIdBuffer[0] = -1
        annotationsFlexSymBuffer[0] = null
    }

    override fun _private_hasFirstAnnotation(sid: Int, text: String?): Boolean {
        if (numAnnotations == 0) return false
        if (sid >= 0 && annotationsIdBuffer[0] == sid) {
            return true
        }
        if (text != null && annotationsFlexSymBuffer[0] == text) {
            return true
        }
        return false
    }

    /**
     * Helper function for handling annotations and field names when starting a value.
     */
    private inline fun openValue(valueWriterExpression: () -> Unit) {

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
                annotationsTotalLength += writeFlexSymFromAnnotationsBuffer(0)
                annotationsFlexSymBuffer[0] = null
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
        val annotationText = annotationsFlexSymBuffer[i]
        return if (annotationText != null) {
            annotationsFlexSymBuffer[i] = null
            if (annotationText is SystemSymbols_1_1) {
                buffer.writeFlexSym(annotationText)
            } else {
                buffer.writeFlexSym(utf8StringEncoder.encode(annotationText.toString()))
            }
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
    private inline fun writeScalar(valueWriterExpression: () -> Int) = openValue {
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
    private inline fun writeTaggedOrTaglessScalar(
        taggedEncoder: () -> Int,
        taglessEncoder: (TaglessEncoding) -> Int,
    ) {
        val primitiveType = when (currentContainer.type) {
            EEXP -> {
                val signature = presenceBitmapStack.peek().signature
                if (currentContainer.numChildren >= signature.size) throw IllegalArgumentException("Too many arguments for macro with signature $signature")
                signature[currentContainer.numChildren].type.taglessEncodingKind
            }
            EXPR_GROUP -> currentContainer.taglessEncodingKind
            else -> null
        }
        if (primitiveType != null) {
            confirm(numAnnotations == 0) { "Tagless values cannot be annotated" }
            if (currentContainer.type == EEXP) {
                presenceBitmapStack.peek()[currentContainer.numChildren] = PresenceBitmap.EXPRESSION
            }
            val numBytesWritten = taglessEncoder(primitiveType)
            currentContainer.length += numBytesWritten
            currentContainer.numChildren++
        } else {
            writeScalar { taggedEncoder() }
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

    override fun writeFieldName(symbol: SystemSymbols_1_1) {
        confirm(currentContainer.type == STRUCT) { "Can only write a field name inside of a struct." }
        if (!currentContainer.usesFlexSym) switchCurrentStructToFlexSym()
        currentContainer.length += buffer.writeFlexSym(symbol)
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
        taggedEncoder = { writeIntValue(buffer, value) },
        taglessEncoder = { primitiveType ->
            when (primitiveType) {
                TaglessEncoding.UINT8 -> {
                    confirm((value and 0xFF) == value) { "value $value is not a valid uint8" }
                    buffer.writeFixedIntOrUInt(value, 1)
                }
                TaglessEncoding.UINT16 -> {
                    confirm((value and 0xFFFF) == value) { "value $value is not a valid uint16" }
                    buffer.writeFixedIntOrUInt(value, 2)
                }
                TaglessEncoding.UINT32 -> {
                    confirm((value and 0xFFFFFFFF) == value) { "value $value is not a valid uint32" }
                    buffer.writeFixedIntOrUInt(value, 4)
                }
                TaglessEncoding.UINT64 -> {
                    confirm(value >= 0) { "value $value is not a valid uint64" }
                    buffer.writeFixedIntOrUInt(value, 8)
                }
                TaglessEncoding.FLEX_UINT -> {
                    confirm(value >= 0) { "value $value is not a valid flex_uint" }
                    buffer.writeFlexUInt(value)
                }
                TaglessEncoding.INT8 -> {
                    confirm(value.toByte().toLong() == value) { "value $value is not a value int8" }
                    buffer.writeFixedIntOrUInt(value, 1)
                }
                TaglessEncoding.INT16 -> {
                    confirm(value.toShort().toLong() == value) { "value $value is not a value int16" }
                    buffer.writeFixedIntOrUInt(value, 2)
                }
                TaglessEncoding.INT32 -> {
                    confirm(value.toInt().toLong() == value) { "value $value is not a value int32" }
                    buffer.writeFixedIntOrUInt(value, 4)
                }
                TaglessEncoding.INT64 -> buffer.writeFixedIntOrUInt(value, 8)
                TaglessEncoding.FLEX_INT -> buffer.writeFlexInt(value)
                else -> throw IonException("Cannot write an int when the macro signature requires $primitiveType.")
            }
        }
    )

    override fun writeInt(value: BigInteger) = writeTaggedOrTaglessScalar(
        taggedEncoder = { writeIntValue(buffer, value) },
        taglessEncoder = { primitiveType ->
            when (primitiveType) {
                TaglessEncoding.UINT8 -> {
                    confirm(value.signum() >= 0 && value.bitLength() <= 8) { "value $value is not a value uint8" }
                    buffer.writeFixedIntOrUInt(value.toLong(), 1)
                }
                TaglessEncoding.UINT16 -> {
                    confirm(value.signum() >= 0 && value.bitLength() <= 16) { "value $value is not a value uint16" }
                    buffer.writeFixedIntOrUInt(value.toLong(), 2)
                }
                TaglessEncoding.UINT32 -> {
                    confirm(value.signum() >= 0 && value.bitLength() <= 32) { "value $value is not a value uint32" }
                    buffer.writeFixedIntOrUInt(value.toLong(), 4)
                }
                TaglessEncoding.UINT64 -> {
                    confirm(value.signum() >= 0 && value.bitLength() <= 64) { "value $value is not a value uint64" }
                    buffer.writeFixedIntOrUInt(value.toLong(), 8)
                }
                TaglessEncoding.FLEX_UINT -> {
                    confirm(value.signum() >= 0) { "value $value is not a value flex_uint" }
                    buffer.writeFlexUInt(value)
                }
                TaglessEncoding.INT8 -> {
                    confirm(value.bitLength() < 8) { "value $value is not a value int8" }
                    buffer.writeFixedIntOrUInt(value.toLong(), 1)
                }
                TaglessEncoding.INT16 -> {
                    confirm(value.bitLength() < 16) { "value $value is not a value int16" }
                    buffer.writeFixedIntOrUInt(value.toLong(), 2)
                }
                TaglessEncoding.INT32 -> {
                    confirm(value.bitLength() < 32) { "value $value is not a value int32" }
                    buffer.writeFixedIntOrUInt(value.toLong(), 4)
                }
                TaglessEncoding.INT64 -> {
                    confirm(value.bitLength() < 64) { "value $value is not a value int64" }
                    buffer.writeFixedIntOrUInt(value.toLong(), 8)
                }
                TaglessEncoding.FLEX_INT -> buffer.writeFlexInt(value)
                else -> throw IonException("Cannot write an int when the macro signature requires $primitiveType.")
            }
        }
    )

    override fun writeFloat(value: Float) = writeTaggedOrTaglessScalar(
        taggedEncoder = { writeFloatValue(buffer, value) },
        taglessEncoder = { primitiveType ->
            when (primitiveType) {
                TaglessEncoding.FLOAT16 -> TODO("Writing FLOAT16 not supported yet")
                TaglessEncoding.FLOAT32 -> buffer.writeFixedIntOrUInt(floatToIntBits(value).toLong(), 4)
                TaglessEncoding.FLOAT64 -> buffer.writeFixedIntOrUInt(doubleToRawLongBits(value.toDouble()), 8)
                else -> throw IonException("Cannot write a float when the macro signature requires $primitiveType.")
            }
        }
    )

    override fun writeFloat(value: Double) = writeTaggedOrTaglessScalar(
        taggedEncoder = { writeFloatValue(buffer, value) },
        taglessEncoder = { primitiveType ->
            when (primitiveType) {
                TaglessEncoding.FLOAT16 -> TODO("Writing FLOAT16 not supported yet")
                // Bounds check for Double->Float would be surprising to some users since floating point numbers
                // normally just accept loss of precision for amy operations instead of throwing and Exception.
                TaglessEncoding.FLOAT32 -> buffer.writeFixedIntOrUInt(floatToIntBits(value.toFloat()).toLong(), 4)
                TaglessEncoding.FLOAT64 -> buffer.writeFixedIntOrUInt(doubleToRawLongBits(value), 8)
                else -> throw IonException("Cannot write a float when the macro signature requires $primitiveType.")
            }
        }
    )

    override fun writeDecimal(value: BigDecimal) = writeScalar { writeDecimalValue(buffer, value) }

    override fun writeTimestamp(value: Timestamp) = writeScalar { writeTimestampValue(buffer, value) }

    override fun writeSymbol(id: Int) {
        confirm(id >= 0) { "Invalid SID: $id" }
        writeTaggedOrTaglessScalar(
            taggedEncoder = { writeSymbolValue(buffer, id) },
            taglessEncoder = { primitiveType ->
                when (primitiveType) {
                    TaglessEncoding.FLEX_SYM -> buffer.writeFlexSym(id)
                    else -> throw IonException("Cannot write a symbol when the macro signature requires $primitiveType.")
                }
            }
        )
    }

    override fun writeSymbol(text: CharSequence) = writeTaggedOrTaglessScalar(
        taggedEncoder = { writeSymbolValue(buffer, utf8StringEncoder.encode(text.toString())) },
        taglessEncoder = { primitiveType ->
            when (primitiveType) {
                TaglessEncoding.FLEX_SYM -> buffer.writeFlexSym(utf8StringEncoder.encode(text.toString()))
                else -> throw IonException("Cannot write a symbol when the macro signature requires $primitiveType.")
            }
        }
    )

    override fun writeSymbol(symbol: SystemSymbols_1_1) = writeScalar {
        buffer.writeByte(OpCodes.SYSTEM_SYMBOL)
        buffer.writeByte(symbol.id.toByte())
        2
    }

    override fun writeString(value: CharSequence) = writeScalar { writeStringValue(buffer, utf8StringEncoder.encode(value.toString())) }

    override fun writeBlob(value: ByteArray, start: Int, length: Int) = writeScalar { writeBlobValue(buffer, value, start, length) }

    override fun writeClob(value: ByteArray, start: Int, length: Int) = writeScalar { writeClobValue(buffer, value, start, length) }

    fun writeTaglessArgumentBytes(action: WriteBuffer.() -> Int) {
        val bytesWritten = buffer.action()
        currentContainer.length += bytesWritten
        currentContainer.numChildren++
    }

    override fun stepInList(usingLengthPrefix: Boolean) {
        openValue {
            currentContainer = containerStack.push { it.reset(LIST, buffer.position(), usingLengthPrefix) }
            if (usingLengthPrefix) {
                buffer.writeByte(OpCodes.VARIABLE_LENGTH_LIST)
                buffer.reserve(lengthPrefixPreallocation)
            } else {
                buffer.writeByte(OpCodes.DELIMITED_LIST)
            }
        }
    }

    override fun stepInSExp(usingLengthPrefix: Boolean) {
        openValue {
            currentContainer = containerStack.push { it.reset(SEXP, buffer.position(), usingLengthPrefix) }
            if (usingLengthPrefix) {
                buffer.writeByte(OpCodes.VARIABLE_LENGTH_SEXP)
                buffer.reserve(lengthPrefixPreallocation)
            } else {
                buffer.writeByte(OpCodes.DELIMITED_SEXP)
            }
        }
    }

    override fun stepInStruct(usingLengthPrefix: Boolean) {
        openValue {
            currentContainer = containerStack.push { it.reset(STRUCT, buffer.position(), usingLengthPrefix) }
            if (usingLengthPrefix) {
                buffer.writeByte(OpCodes.VARIABLE_LENGTH_STRUCT_WITH_SIDS)
                buffer.reserve(lengthPrefixPreallocation)
            } else {
                buffer.writeByte(OpCodes.DELIMITED_STRUCT)
                currentContainer.usesFlexSym = true
            }
        }
    }

    override fun stepInEExp(name: CharSequence) {
        throw UnsupportedOperationException("Binary writer requires macros to be invoked by their ID.")
    }

    // Void can be written as an empty expression group.
    override fun stepInEExp(id: Int, usingLengthPrefix: Boolean, macro: Macro) {
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

        currentContainer = containerStack.push { it.reset(EEXP, buffer.position(), usingLengthPrefix) }

        if (usingLengthPrefix) {
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
                buffer.writeByte(OpCodes.E_EXPRESSION_WITH_FLEX_UINT_ADDRESS)
                currentContainer.metadataOffset += buffer.writeFlexUInt(id)
            }
        }

        val presenceBits = presenceBitmapStack.push { it.initialize(macro.signature) }
        if (presenceBits.byteSize > 0) {
            // Reserve for presence bits
            buffer.reserve(presenceBits.byteSize)
            currentContainer.length += presenceBits.byteSize
        }

        // No need to clear any of the annotation fields because we already asserted that there are no annotations
        hasFieldName = false
    }

    override fun stepInEExp(systemMacro: SystemMacro) {
        confirm(numAnnotations == 0) { "Cannot annotate an E-Expression" }

        if (currentContainer.type == STRUCT && !hasFieldName) {
            // This allows the e-expression to be written in field-name position.
            // TODO: Confirm that this is still in the spec.
            if (!currentContainer.usesFlexSym) switchCurrentStructToFlexSym()
            buffer.writeByte(FlexInt.ZERO)
            currentContainer.length++
        }

        currentContainer = containerStack.push { it.reset(EEXP, buffer.position(), isLengthPrefixed = false) }

        buffer.writeByte(OpCodes.SYSTEM_MACRO_INVOCATION)
        buffer.writeByte(systemMacro.id)
        currentContainer.metadataOffset += 1 // to account for the macro ID.

        val presenceBits = presenceBitmapStack.push { it.initialize(systemMacro.signature) }
        if (presenceBits.byteSize > 0) {
            // Reserve for presence bits
            buffer.reserve(presenceBits.byteSize)
            currentContainer.length += presenceBits.byteSize
        }

        // No need to clear any of the annotation fields because we already asserted that there are no annotations
        hasFieldName = false
    }

    override fun stepInExpressionGroup(usingLengthPrefix: Boolean) {
        confirm(numAnnotations == 0) { "Cannot annotate an expression group" }
        confirm(currentContainer.type == EEXP) { "Can only create an expression group in a macro invocation" }

        val encoding = presenceBitmapStack.peek().signature[currentContainer.numChildren].type

        currentContainer = containerStack.push { it.reset(EXPR_GROUP, buffer.position(), usingLengthPrefix, metadataOffset = 0) }
        currentContainer.taglessEncodingKind = encoding.taglessEncodingKind

        if (encoding.taglessEncodingKind != null) {
            // Tagless groups always need a length (although it is actually the count of expressions in the group)
            buffer.reserve(maxOf(1, lengthPrefixPreallocation))
        } else if (usingLengthPrefix) {
            // Reserve length prefix for a tagged expression group
            buffer.reserve(maxOf(1, lengthPrefixPreallocation))
        } else {
            // At the start of a tagged expression group, signals that it is delimited.
            buffer.writeByte(FlexInt.ZERO)
            currentContainer.length++
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
        val primitiveType = currentContainer.taglessEncodingKind
        if (!currentContainer.isLengthPrefixed && primitiveType != null && currentContainer.length > 0) {
            var thisContainerTotalLength = currentContainer.length
            val thisContainerNumChildren = currentContainer.numChildren
            thisContainerTotalLength += writeCurrentContainerLength(lengthPrefixPreallocation)
            containerStack.pop()
            containerStack.peek().length += thisContainerTotalLength
            currentContainer = containerStack.push { it.reset(EXPR_GROUP, buffer.position(), isLengthPrefixed = false, metadataOffset = 0) }
            currentContainer.taglessEncodingKind = primitiveType
            // Carry over numChildren into the next segment (but not length)
            currentContainer.numChildren = thisContainerNumChildren
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
                if (currentContainer.isLengthPrefixed) {
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
                } else {
                    if (isInStruct()) {
                        // Need a 0 FlexInt before the end delimiter
                        buffer.writeByte(FlexInt.ZERO)
                        thisContainerTotalLength += 1
                    }
                    thisContainerTotalLength += 1 // For the end marker
                    buffer.writeByte(OpCodes.DELIMITED_END_MARKER)
                }
            }
            EEXP -> {
                // Add to account for the opcode and/or address
                thisContainerTotalLength += currentContainer.metadataOffset

                val presenceBitmap = presenceBitmapStack.pop()
                val requiresWritingPresenceBits = presenceBitmap.byteSize > 0
                val presenceBitmapPosition = if (currentContainer.isLengthPrefixed) {
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
                val isTagless = currentContainer.taglessEncodingKind != null
                // TODO: Consider whether we can rewrite groups that have only one expression as a single expression

                // Elide empty containers if we're going to be writing a presence bitmap
                if (currentContainer.numChildren == 0 && presenceBitmapStack.peek().byteSize > 0) {
                    // NOTE: This check is safe after calling `continueExpressionGroup` because that function
                    //       only resets the container length, not the number of children.

                    // It is not always safe to truncate like this without clearing the patch points for the
                    // truncated part of the buffer. However, it is safe to do so here because we can only get to
                    // this particular branch if this expression group is empty, ergo it contains no patch points.
                    buffer.truncate(currentContainer.position)
                    thisContainerTotalLength = 0
                } else if (isTagless && currentContainer.length == 0L) {
                    // If we've called `continueExpressionGroup` and then `stepOut` without adding any more items...
                    buffer.truncate(currentContainer.position)
                    buffer.writeByte(FlexInt.ZERO)
                    thisContainerTotalLength++
                } else if (isTagless) {
                    // End tagless group -- write the number of expressions, end with FlexUInt 0
                    thisContainerTotalLength += writeCurrentContainerLength(maxOf(1, lengthPrefixPreallocation))
                    buffer.writeByte(FlexInt.ZERO)
                    thisContainerTotalLength++
                } else if (currentContainer.isLengthPrefixed) {
                    // Length-prefixed, tagged -- write the number of bytes
                    thisContainerTotalLength += writeCurrentContainerLength(maxOf(1, lengthPrefixPreallocation))
                } else {
                    // Delimited, tagged -- start with `01` end with `F0`
                    buffer.writeByte(OpCodes.DELIMITED_END_MARKER)
                    thisContainerTotalLength++
                }
            }
            ANNOTATIONS -> TODO("Unreachable.")
            TOP -> throw IonException("Nothing to step out of.")
        }

        // Set the new current container
        val justExitedContainer = containerStack.pop()
        currentContainer = containerStack.peek()

        if (currentContainer.type == EEXP) {
            val signature = presenceBitmapStack.peek().signature
            if (currentContainer.numChildren >= signature.size) throw IllegalArgumentException("Too many arguments for macro with signature $signature")
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
     */
    private fun writeCurrentContainerLength(numPreAllocatedLengthPrefixBytes: Int): Int {
        val lengthToWrite = currentContainer.length
        val lengthPosition = currentContainer.position + currentContainer.metadataOffset
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

    override fun writeMacroParameterCardinality(cardinality: Macro.ParameterCardinality) {
        // TODO: Write as a system symbol
        writeSymbol(cardinality.sigil.toString())
    }

    override fun stepInTdlMacroInvocation(macroRef: Int) {
        stepInSExp(usingLengthPrefix = false)
        writeSymbol(".")
        writeInt(macroRef.toLong())
    }

    override fun stepInTdlMacroInvocation(macroRef: String) {
        stepInSExp(usingLengthPrefix = false)
        writeSymbol(".")
        writeSymbol(macroRef)
    }

    override fun stepInTdlSystemMacroInvocation(systemSymbol: SystemSymbols_1_1) {
        stepInSExp(usingLengthPrefix = false)
        writeSymbol(".")
        writeAnnotations(SystemSymbols_1_1.ION)
        writeSymbol(systemSymbol)
    }

    override fun writeTdlVariableExpansion(variableName: String) {
        stepInSExp(usingLengthPrefix = false)
        writeSymbol("%")
        writeSymbol(variableName)
        stepOut()
    }

    override fun stepInTdlExpressionGroup() {
        stepInSExp(usingLengthPrefix = false)
        // TODO: Write as a system symbol
        writeSymbol("..")
    }
}
