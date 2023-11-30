// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.*
import com.amazon.ion.impl.*
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1.ContainerType.*
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
    }

    private data class ContainerInfo(
        var type: ContainerType? = null,
        var isDelimited: Boolean = false,
        var position: Long = -1,
        var length: Long = -1,
        // TODO: Test if performance is better with an Object Reference or an index into the PatchPoint queue.
        var patchPoint: PatchPoint? = null,
    )

    private var numAnnotations = 0
    private var hasFieldName = false

    private var closed = false

    private val patchPoints = _Private_RecyclingQueue(512) { PatchPoint() }
    private val containerStack = _Private_RecyclingStack(8) { ContainerInfo() }

    private var currentContainer: ContainerInfo = containerStack.push { it.type = Top }

    override fun finish() {
        if (closed) return
        confirm(depth() == 0) { "Cannot call finish() while in a container" }

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
        buffer.writeBytes(Ion_1_1_Constants.IVM)
    }

    override fun writeAnnotations(annotation0: Int) {
        TODO("Not yet implemented")
    }

    override fun writeAnnotations(annotation0: Int, annotation1: Int) {
        TODO("Not yet implemented")
    }

    override fun writeAnnotations(annotation0: Int, annotation1: Int, vararg annotations: Int) {
        TODO("Not yet implemented")
    }

    override fun writeAnnotations(annotation0: CharSequence) {
        TODO("Not yet implemented")
    }

    override fun writeAnnotations(annotation0: CharSequence, annotation1: CharSequence) {
        TODO("Not yet implemented")
    }

    override fun writeAnnotations(annotation0: CharSequence, annotation1: CharSequence, vararg annotations: CharSequence) {
        TODO("Not yet implemented")
    }

    /**
     * Helper function for handling annotations and field names when starting a value.
     */
    private inline fun openValue(valueWriterExpression: () -> Unit) {
        if (numAnnotations > 0) {
            TODO("Actually write out the annotations.")
        }
        numAnnotations = 0
        hasFieldName = false
        valueWriterExpression()
    }

    /**
     * Helper function for finishing scalar values. Similar concerns for containers are handled in [stepOut].
     */
    private inline fun closeScalar(valueWriterExpression: () -> Int) {
        val numBytesWritten = valueWriterExpression()

        // Update the container length (unless it's Top)
        if (currentContainer.type != Top) currentContainer.length += numBytesWritten
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
                it.type = List
                it.position = buffer.position()
                it.isDelimited = delimited
                it.length = 0
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
                        buffer.writeUInt8At(currentContainer.position, 0xA0L or contentLength)
                    } else {
                        val lengthPrefixBytesRequired = FlexInt.flexUIntLength(contentLength)
                        thisContainerTotalLength += lengthPrefixBytesRequired

                        if (lengthPrefixBytesRequired == lengthPrefixPreallocation) {
                            // We have enough space, so write in the correct length.
                            buffer.writeFlexIntOrUIntAt(currentContainer.position + 1, currentContainer.length, lengthPrefixBytesRequired)
                        } else {
                            addPatchPointsToStack()
                            // currentContainer is in containerStack, so we know that its patchPoint is non-null.
                            currentContainer.patchPoint.assumeNotNull().apply {
                                oldPosition = currentContainer.position + 1
                                oldLength = lengthPrefixPreallocation
                                length = currentContainer.length
                            }
                        }
                    }
                }
                SExp -> TODO()
                Struct -> TODO()
                Macro -> TODO()
                Stream -> TODO()
                Top -> throw IonException("Nothing to step out of.")
            }
        }

        // Set the new current container
        containerStack.pop()
        currentContainer = containerStack.peek()
        // Update the length of the new current container to include the length of the container that we just stepped out of.
        if (currentContainer.type != Top) currentContainer.length += thisContainerTotalLength
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
