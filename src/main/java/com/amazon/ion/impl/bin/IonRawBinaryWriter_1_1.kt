// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.*
import com.amazon.ion.impl.*
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1.ContainerType.*
import com.amazon.ion.impl.bin.utf8.*
import com.amazon.ion.util.*
import java.io.ByteArrayOutputStream
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant

class IonRawBinaryWriter_1_1 internal constructor(
    private val out: ByteArrayOutputStream,
    private val buffer: WriteBuffer,
    private val lengthPrefixPreallocation: Int,
) : IonWriter_1_1 {

    /**
     * Types of encoding containers.
     */
    enum class ContainerType {
        List,
        SExp,
        Struct,
        Macro,
        Stream,
        /**
         * Represents the top level stream. The [containerStack] always has [ContainerInfo] for [Top] at the bottom
         * of the stack so that we never have to check if [currentContainer] is null.
         *
         * TODO: Test if performance is better if we just check currentContainer for nullness.
         */
        Top,
        /**
         * Represents a group of annotations. May only contain FlexSyms or FlexUInt symbol IDs.
         */
        Annotations,
    }

    private data class ContainerInfo(
        var type: ContainerType? = null,
        var isDelimited: Boolean = false,
        var position: Long = -1,
        var length: Long = 0,
        // TODO: Test if performance is better with an Object Reference or an index into the PatchPoint queue.
        var patchPoint: PatchPoint? = null,
    ) {
        fun clear() {
            type = null
            isDelimited = false
            position = -1
            length = 0
            patchPoint = null
        }
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
    private val FLEX_SYMS_REQUIRED = -1

    private var hasFieldName = false

    private var closed = false

    private val patchPoints = _Private_RecyclingQueue(512) { PatchPoint() }
    private val containerStack = _Private_RecyclingStack(8) { ContainerInfo() }

    private var currentContainer: ContainerInfo = containerStack.push { it.type = Top }

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

    override fun isInStruct(): Boolean = currentContainer.type == Struct

    override fun writeIVM() {
        confirm(currentContainer.type == Top) { "IVM can only be written at the top level of an Ion stream." }
        confirm(numAnnotations == 0) { "Cannot write an IVM with annotations" }
        buffer.writeBytes(_Private_IonConstants.BINARY_VERSION_MARKER_1_1)
    }

    /**
     * Ensures that there is enough space in the annotation buffers for [n] annotations.
     * If more space is needed, it over-allocates by 8 to ensure that we're not continually allocating when annotations
     * are being added one by one.
     */
    private inline fun ensureAnnotationSpace(n: Int) {
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
        ensureAnnotationSpace(numAnnotations + 1)
        annotationsIdBuffer[numAnnotations++] = annotation0
    }

    override fun writeAnnotations(annotation0: Int, annotation1: Int) {
        ensureAnnotationSpace(numAnnotations + 2)
        annotationsIdBuffer[numAnnotations++] = annotation0
        annotationsIdBuffer[numAnnotations++] = annotation1
    }

    override fun writeAnnotations(annotations: IntArray) {
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

    /**
     * Helper function for handling annotations and field names when starting a value.
     */
    private inline fun openValue(valueWriterExpression: () -> Unit) {

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
        currentContainer = containerStack.push {
            it.clear()
            it.type = Annotations
            it.position = buffer.position()
        }
        if (annotationFlexSymFlag == FLEX_SYMS_REQUIRED) {
            buffer.writeByte(OpCodes.ANNOTATIONS_MANY_FLEX_SYM)
            buffer.reserve(lengthPrefixPreallocation)
            for (i in 0 until numAnnotations) {
                currentContainer.length += writeFlexSymFromAnnotationsBuffer(i)
            }
        } else {
            buffer.writeByte(OpCodes.ANNOTATIONS_MANY_SYMBOL_ADDRESS)
            buffer.reserve(lengthPrefixPreallocation)
            for (i in 0 until numAnnotations) {
                currentContainer.length += buffer.writeFlexUInt(annotationsIdBuffer[i])
            }
        }

        val numAnnotationsBytes = currentContainer.length
        val numLengthPrefixBytes = writeCurrentContainerLength()

        // Set the new current container
        containerStack.pop()
        currentContainer = containerStack.peek()

        return numLengthPrefixBytes + numAnnotationsBytes
    }

    /**
     * Helper function for finishing scalar values. Similar concerns for containers are handled in [stepOut].
     */
    private inline fun closeScalar(valueWriterExpression: () -> Int) {
        val numBytesWritten = valueWriterExpression()
        currentContainer.length += numBytesWritten
    }

    /**
     * Helper function for writing scalar values that composes both [openValue] and [closeScalar].
     */
    private inline fun writeScalar(valueWriterExpression: () -> Int) {
        openValue { closeScalar(valueWriterExpression) }
    }

    override fun writeFieldName(sid: Int) {
        TODO("Not implemented yet.")
    }

    override fun writeFieldName(text: CharSequence) {
        TODO("Not implemented yet.")
    }

    override fun writeNull() = writeScalar { IonEncoder_1_1.writeNullValue(buffer, IonType.NULL) }

    override fun writeNull(type: IonType) = writeScalar { IonEncoder_1_1.writeNullValue(buffer, type) }

    override fun writeBool(value: Boolean) = writeScalar { IonEncoder_1_1.writeBoolValue(buffer, value) }

    override fun writeInt(value: Byte) {
        TODO("Not yet implemented")
    }

    override fun writeInt(value: Int) {
        TODO("Not yet implemented")
    }

    override fun writeInt(value: Long) {
        TODO("Not yet implemented")
    }

    override fun writeInt(value: BigInteger) {
        TODO("Not yet implemented")
    }

    override fun writeFloat(value: Float) {
        TODO("Not yet implemented")
    }

    override fun writeFloat(value: Double) {
        TODO("Not yet implemented")
    }

    override fun writeDecimal(value: BigDecimal) {
        TODO("Not yet implemented")
    }

    override fun writeDecimal(value: Decimal) {
        TODO("Not yet implemented")
    }

    override fun writeTimestamp(value: Timestamp) {
        TODO("Not yet implemented")
    }

    override fun writeTimestamp(value: Instant) {
        TODO("Not yet implemented")
    }

    override fun writeSymbol(id: Int) {
        TODO("Not yet implemented")
    }

    override fun writeSymbol(text: CharSequence) {
        TODO("Not yet implemented")
    }

    override fun writeString(value: CharSequence) {
        TODO("Not yet implemented")
    }

    override fun writeBlob(value: ByteArray, start: Int, length: Int) {
        TODO("Not yet implemented")
    }

    override fun writeClob(value: ByteArray, start: Int, length: Int) {
        TODO("Not yet implemented")
    }

    override fun stepInList(delimited: Boolean) {
        openValue {
            currentContainer = containerStack.push {
                it.clear()
                it.type = List
                it.position = buffer.position()
                it.isDelimited = delimited
            }
            if (delimited) {
                buffer.writeByte(OpCodes.DELIMITED_LIST)
            } else {
                buffer.writeByte(OpCodes.VARIABLE_LENGTH_LIST)
                buffer.reserve(lengthPrefixPreallocation)
            }
        }
    }

    override fun stepInSExp(delimited: Boolean) {
        TODO("Not yet implemented")
    }

    override fun stepInStruct(delimited: Boolean, useFlexSym: Boolean) {
        TODO("Not yet implemented")
    }

    override fun stepInEExp(name: CharSequence) {
        TODO("Not supported by the raw binary writer.")
    }

    override fun stepInEExp(id: Int) {
        TODO("Not yet implemented")
    }

    override fun stepInStream() {
        TODO("Not yet implemented")
    }

    override fun stepOut() {
        confirm(!hasFieldName) { "Cannot step out with dangling field name." }
        confirm(numAnnotations == 0) { "Cannot step out with dangling annotations." }

        // The length of the current container, including any opcodes and length prefixes
        var thisContainerTotalLength: Long = 1 + currentContainer.length

        // Write closing delimiter if we're in a delimited container.
        // Update length prefix if we're in a prefixed container.
        if (currentContainer.isDelimited) {
            thisContainerTotalLength += 1 // For the end marker
            buffer.writeByte(OpCodes.DELIMITED_END_MARKER)
        } else {
            // currentContainer.type is non-null for any initialized ContainerInfo
            when (currentContainer.type.assumeNotNull()) {
                List -> {
                    // TODO: Possibly extract this so it can be reused for the other length-prefixed container types
                    val contentLength = currentContainer.length
                    if (contentLength <= 0xF) {
                        // Clean up any unused space that was pre-allocated.
                        buffer.shiftBytesLeft(currentContainer.length.toInt(), lengthPrefixPreallocation)
                        buffer.writeUInt8At(currentContainer.position, OpCodes.LIST_ZERO_LENGTH + contentLength)
                    } else {
                        thisContainerTotalLength += writeCurrentContainerLength()
                    }
                }
                SExp -> TODO()
                Struct -> TODO()
                Macro -> TODO()
                Stream -> TODO()
                Annotations -> TODO("Unreachable.")
                Top -> throw IonException("Nothing to step out of.")
            }
        }

        // Set the new current container
        containerStack.pop()
        currentContainer = containerStack.peek()
        // Update the length of the new current container to include the length of the container that we just stepped out of.
        currentContainer.length += thisContainerTotalLength
    }

    /**
     * Writes the length of the current container and returns the number of bytes needed to do so.
     * Transparently handles PatchPoints as necessary.
     */
    private fun writeCurrentContainerLength(): Int {
        val lengthPrefixBytesRequired = FlexInt.flexUIntLength(currentContainer.length)
        if (lengthPrefixBytesRequired == lengthPrefixPreallocation) {
            // We have enough space, so write in the correct length.
            buffer.writeFlexIntOrUIntAt(currentContainer.position + 1, currentContainer.length, lengthPrefixBytesRequired)
        } else {
            addPatchPointsToStack()
            // All ContainerInfos are in the stack, so we know that its patchPoint is non-null.
            currentContainer.patchPoint.assumeNotNull().apply {
                oldPosition = currentContainer.position + 1
                oldLength = lengthPrefixPreallocation
                length = currentContainer.length
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
        // Instead of allocating iterator, we share one iterator instance within the scope of the container stack and reset the cursor every time we track back to the ancestors.
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
