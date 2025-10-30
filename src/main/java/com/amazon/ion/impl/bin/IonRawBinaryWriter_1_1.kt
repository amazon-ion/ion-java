// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.Decimal
import com.amazon.ion.IonException
import com.amazon.ion.IonType
import com.amazon.ion.Timestamp
import com.amazon.ion.bytecode.bin11.OpCode
import com.amazon.ion.impl._Private_RecyclingQueue
import com.amazon.ion.impl._Private_RecyclingStack
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1.ContainerType.DELIMITED_LIST
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1.ContainerType.DELIMITED_SEXP
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1.ContainerType.DELIMITED_STRUCT_FS
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1.ContainerType.DELIMITED_STRUCT_SID
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1.ContainerType.DELIMITED_STRUCT_SID_TO_FS
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1.ContainerType.DIRECTIVE
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1.ContainerType.EEXP
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1.ContainerType.PREFIXED_EEXP
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1.ContainerType.PREFIXED_LIST
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1.ContainerType.PREFIXED_SEXP
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1.ContainerType.PREFIXED_STRUCT_FS
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1.ContainerType.PREFIXED_STRUCT_SID
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1.ContainerType.PREFIXED_STRUCT_SID_TO_FS
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1.ContainerType.PREFIXED_TAGLESS_EEXP
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1.ContainerType.TAGLESS_EEXP
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1.ContainerType.TE_LIST
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1.ContainerType.TE_LIST_W_LENGTH_PREFIXED_MACRO
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1.ContainerType.TE_SEXP
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1.ContainerType.TE_SEXP_W_LENGTH_PREFIXED_MACRO
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1.ContainerType.TOP
import com.amazon.ion.impl.bin.WriteBuffer.fixedIntLength
import com.amazon.ion.impl.bin.WriteBuffer.flexIntLength
import com.amazon.ion.impl.bin.WriteBuffer.flexUIntLength
import com.amazon.ion.impl.bin.utf8.Utf8StringEncoderPool
import com.amazon.ion.ion_1_1.IonRawWriter_1_1
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import java.io.OutputStream
import java.lang.Double.doubleToRawLongBits
import java.lang.Float.floatToIntBits
import java.math.BigDecimal
import java.math.BigInteger
import java.util.function.Consumer

class IonRawBinaryWriter_1_1 internal constructor(
    @SuppressFBWarnings("EI_EXPOSE_REP2", justification = "We're intentionally storing a reference to a mutable object because we need to write to it.")
    private val out: OutputStream,
    @SuppressFBWarnings("EI_EXPOSE_REP2", justification = "We're intentionally storing a reference to a mutable object because we need to write to it.")
    private val buffer: WriteBuffer,
    private val lengthPrefixPreallocation: Int,
) : IonRawWriter_1_1 {

    private inline fun confirm(condition: Boolean, lazyMessage: () -> String) {
        if (!condition) {
            throw IonException(lazyMessage())
        }
    }

    private fun WriteBuffer.writeByte(byte: Int): Int {
        writeByte(byte.toByte())
        return 1
    }

    /**
     * Types of encoding containers.
     */
    private object ContainerType {
        const val TOP = -1
        const val DIRECTIVE = 0
        const val EEXP = 1
        const val PREFIXED_EEXP = 2
        const val TAGLESS_EEXP = 3
        const val PREFIXED_TAGLESS_EEXP = 4

        // NOTE: All data model containers are deliberately grouped together to make it easier to check for zero-length containers.

        const val TE_LIST = 5
        const val TE_SEXP = 6
        const val TE_LIST_W_LENGTH_PREFIXED_MACRO = 7
        const val TE_SEXP_W_LENGTH_PREFIXED_MACRO = 8
        const val PREFIXED_LIST = 9
        const val PREFIXED_SEXP = 10
        const val DELIMITED_LIST = 11
        const val DELIMITED_SEXP = 12

        // NOTE: All struct encodings are deliberately at the end so that we can check if it's a struct by just seeing if
        // currentContainer.type >= DELIMITED_STRUCT_SID

        const val DELIMITED_STRUCT_SID = 13
        /** Represents a struct that started out in SID mode and has switched to FlexSym */
        const val DELIMITED_STRUCT_SID_TO_FS = 14
        const val DELIMITED_STRUCT_FS = 15

        const val PREFIXED_STRUCT_SID = 16
        /** Represents a struct that started out in SID mode and has switched to FlexSym */
        const val PREFIXED_STRUCT_SID_TO_FS = 17
        const val PREFIXED_STRUCT_FS = 18
    }

    private class ContainerInfo(
        /** The type of container, represented by one of the constants in [ContainerType]. */
        @JvmField var type: Int = -1,
        /** The position, in the output, of the _opcode_ of this container. */
        @JvmField var position: Long = -1,
        /** Where the length prefix should be written, relative to the start of this container. */
        @JvmField var metadataOffset: Int = 1,
        /** The number of bytes for everything following the length-prefix (if applicable) in this container. */
        @JvmField var length: Long = 0,
        // TODO: Test if performance is better with an Object Reference or an index into the PatchPoint queue.
        @JvmField var patchPoint: PatchPoint? = null,
        /**
         * The number of elements in the expression group or arguments to the macro.
         * This is updated when _finishing_ writing a value or expression group.
         */
        @JvmField var numChildren: Int = 0,
    ) {
        /**
         * Clears this [ContainerInfo] of old data and initializes it with the given new data.
         */
        fun reset(type: Int, position: Long, metadataOffset: Int = 1) {
            this.type = type
            this.position = position
            this.metadataOffset = metadataOffset
            length = 0
            patchPoint = null
            numChildren = 0
        }
    }

    companion object {
        /**
         * Create a new instance for the given OutputStream with the given block size and length preallocation.
         */
        @JvmStatic
        fun from(out: OutputStream, blockSize: Int, preallocation: Int): IonRawBinaryWriter_1_1 {
            return IonRawBinaryWriter_1_1(out, WriteBuffer(BlockAllocatorProviders.basicProvider().vendAllocator(blockSize)) {}, preallocation)
        }

        private val IVM_BYTES = byteArrayOf(0xE0.toByte(), 1, 1, 0xEA.toByte())
    }

    private val utf8StringEncoder = Utf8StringEncoderPool.getInstance().getOrCreate()

    private var closed = false

    private val patchPoints = _Private_RecyclingQueue(512) { PatchPoint() }
    private val containerStack = _Private_RecyclingStack(8) { ContainerInfo() }

    private var currentContainer: ContainerInfo = containerStack.push { it.reset(-1, 0L) }

    override fun flush() {
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
                val numBytes = flexUIntLength(patch.length)
                PrimitiveEncoder.writeFlexIntOrUIntInto(flexUIntScratch, 0, patch.length, numBytes)
                out.write(flexUIntScratch, 0, numBytes)

                // skip over the pre-allocated field
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

    override fun isInStruct(): Boolean = currentContainer.type >= DELIMITED_STRUCT_SID

    override fun writeIVM() {
        confirm(currentContainer.type == TOP) { "IVM can only be written at the top level of an Ion stream." }
        buffer.writeBytes(IVM_BYTES)
    }

    override fun writeAnnotations(annotation0: Int) {
        val currentContainer = currentContainer
        buffer.writeByte(OpCode.ANNOTATION_SID)
        currentContainer.length += 1 + buffer.writeFlexUInt(annotation0)
    }

    override fun writeAnnotations(annotation0: Int, annotation1: Int) {
        TODO("Remove this method from the interface, since it is no longer useful.")
    }

    override fun writeAnnotations(annotations: IntArray) {
        val buffer = buffer
        val numAnnotations = annotations.size
        var numAnnotationBytes = numAnnotations
        for (i in 0 until numAnnotations) {
            buffer.writeByte(OpCode.ANNOTATION_SID)
            numAnnotationBytes += buffer.writeFlexUInt(annotations[i])
        }
        currentContainer.length += numAnnotationBytes
    }

    override fun writeAnnotations(annotation0: CharSequence) {
        val buffer = buffer
        buffer.writeByte(OpCode.ANNOTATION_TEXT)
        val text = utf8StringEncoder.encode(annotation0.toString())
        val textLength = text.encodedLength
        val numLengthBytes: Int = buffer.writeFlexUInt(textLength.toLong())
        buffer.writeBytes(text.buffer, 0, textLength)
        currentContainer.length += 1 + numLengthBytes + textLength
    }

    override fun writeAnnotations(annotation0: CharSequence, annotation1: CharSequence) {
        TODO("Remove this method from the interface, since it is no longer useful.")
    }

    override fun writeAnnotations(annotations: Array<CharSequence>) {
        val buffer = buffer
        val numAnnotations = annotations.size
        var numAnnotationBytes = numAnnotations
        for (i in 0 until numAnnotations) {
            buffer.writeByte(OpCode.ANNOTATION_TEXT)
            val text = utf8StringEncoder.encode(annotations[i].toString())
            val textLength = text.encodedLength
            val numLengthBytes: Int = buffer.writeFlexUInt(textLength.toLong())
            buffer.writeBytes(text.buffer, 0, textLength)
            numAnnotationBytes += textLength + numLengthBytes
        }
        currentContainer.length += numAnnotationBytes
    }

    /**
     * Helper function for writing scalar values that updates the length of the current container.
     *
     * @param valueWriterExpression should be a function that writes the scalar value to the buffer, and
     *                              returns the number of bytes that were written.
     */
    private inline fun writeScalar(valueWriterExpression: () -> Int) {
        val numBytesWritten = valueWriterExpression()
        currentContainer.length += numBytesWritten
        currentContainer.numChildren++
    }

    override fun writeFieldName(sid: Int) {
        val currentContainer = currentContainer
        when (currentContainer.type) {
            PREFIXED_STRUCT_SID,
            DELIMITED_STRUCT_SID -> currentContainer.length += buffer.writeFlexUInt(sid.toLong())
            PREFIXED_STRUCT_FS,
            DELIMITED_STRUCT_FS,
            PREFIXED_STRUCT_SID_TO_FS,
            DELIMITED_STRUCT_SID_TO_FS -> currentContainer.length += buffer.writeFlexInt(sid.toLong())
            // Raw writer is not required to check this, but since we're already checking the container type, we can do it here anyway.
            else -> throw IonException("Can only write a field name inside of a struct.")
        }
    }

    override fun writeFieldName(text: CharSequence) {
        val currentContainer = currentContainer
        when (currentContainer.type) {
            PREFIXED_STRUCT_SID,
            DELIMITED_STRUCT_SID -> {
                buffer.writeByte(PrimitiveEncoder.FLEX_ZERO) // field name $0
                buffer.writeByte(OpCode.STRUCT_SWITCH_MODES)
                // NOTE: This has the effect of turning a SID struct into a FlexSym struct because the latter's IDs are one above the former.
                currentContainer.type++
                currentContainer.length += 2 + buffer.writeFlexSymText(text)
            }
            PREFIXED_STRUCT_FS,
            DELIMITED_STRUCT_FS,
            PREFIXED_STRUCT_SID_TO_FS,
            DELIMITED_STRUCT_SID_TO_FS -> currentContainer.length += buffer.writeFlexSymText(text)
            // Raw writer is not required to check this, but since we're already checking the container type, we can do it here anyway.
            else -> throw IonException("Can only write a field name inside of a struct.")
        }
    }

    override fun writeNull() = writeScalar {
        buffer.writeByte(OpCode.NULL_NULL.toByte())
        1
    }

    override fun writeNull(type: IonType) = writeScalar {
        if (type == IonType.NULL) {
            buffer.writeByte(OpCode.NULL_NULL)
            1
        } else {
            buffer.writeByte(OpCode.TYPED_NULL)
            val typeByte = type.ordinal
            buffer.writeByte(typeByte)
            2
        }
    }

    override fun writeBool(value: Boolean) = writeScalar {
        val data = if (value) OpCode.BOOL_TRUE else OpCode.BOOL_FALSE
        buffer.writeByte(data.toByte())
        1
    }

    override fun writeInt(value: Long) = writeScalar {
        if (value == 0L) {
            buffer.writeByte(OpCode.INT_0)
            1
        } else {
            val length = fixedIntLength(value)
            buffer.writeByte(OpCode.INT_0 + length)
            buffer.writeFixedIntOrUInt(value, length)
            1 + length
        }
    }

    override fun writeInt(value: BigInteger) {
        if (value.bitLength() < Long.SIZE_BITS) {
            writeInt(value.longValueExact())
        } else {
            writeScalar {
                buffer.writeByte(OpCode.VARIABLE_LENGTH_INTEGER)
                val intBytes = value.toByteArray()
                val totalBytes = 1 + intBytes.size + buffer.writeFlexUInt(intBytes.size)
                for (i in intBytes.size downTo 1) {
                    buffer.writeByte(intBytes[i - 1])
                }
                totalBytes
            }
        }
    }

    override fun writeFloat(value: Double) {
        // TODO: Optimization to write a 16 bit float for non-finite and possibly other values
        //       We could check the number of significand bits and the value of the exponent
        //       to determine if it can be represented in a smaller format without having a
        //       complete representation of half-precision floating point numbers.
        if (!value.isFinite() || value == value.toFloat().toDouble()) {
            writeFloat(value.toFloat())
        } else {
            writeScalar {
                buffer.writeByte(OpCode.FLOAT_64)
                buffer.writeFixedIntOrUInt(doubleToRawLongBits(value), 8)
                9
            }
        }
    }

    override fun writeFloat(value: Float) = writeScalar {
        // TODO: Consider adding a check for some half-precision values that we can use.
        if (value == 0.0f) {
            buffer.writeByte(OpCode.FLOAT_0)
            1
        } else {
            buffer.writeByte(OpCode.FLOAT_32)
            buffer.writeFixedIntOrUInt(floatToIntBits(value).toLong(), 4)
            5
        }
    }

    override fun writeDecimal(value: BigDecimal) = writeScalar {

        val exponent = -value.scale()
        val numExponentBytes = flexIntLength(exponent.toLong())

        var coefficientBytes: ByteArray? = null
        val numCoefficientBytes: Int
        if (BigDecimal.ZERO.compareTo(value) == 0) {
            numCoefficientBytes = if (Decimal.isNegativeZero(value)) {
                1
            } else if (exponent == 0) {
                buffer.writeByte(OpCode.DECIMAL_0)
                return@writeScalar 1
            } else {
                0
            }
        } else {
            coefficientBytes = value.unscaledValue().toByteArray()
            numCoefficientBytes = coefficientBytes.size
        }

        var opCodeAndLengthBytes = 1
        if (numExponentBytes + numCoefficientBytes < 16) {
            val opCode = OpCode.DECIMAL_0 + numExponentBytes + numCoefficientBytes
            buffer.writeByte(opCode.toByte())
        } else {
            // Decimal values that require more than 15 bytes can be encoded using the variable-length decimal opcode: 0xF6.
            buffer.writeByte(OpCode.VARIABLE_LENGTH_DECIMAL)
            opCodeAndLengthBytes += buffer.writeFlexUInt(numExponentBytes + numCoefficientBytes)
        }

        buffer.writeFlexInt(exponent.toLong())
        if (numCoefficientBytes > 0) {
            if (coefficientBytes != null) {
                buffer.writeFixedIntOrUInt(coefficientBytes)
            } else {
                buffer.writeByte(0.toByte())
            }
        }
        opCodeAndLengthBytes + numCoefficientBytes + numExponentBytes
    }

    override fun writeTimestamp(value: Timestamp) = writeScalar { TimestampEncoder_1_1.writeTimestampValue(buffer, value) }

    override fun writeSymbol(id: Int) = writeScalar {
        confirm(id >= 0) { "Invalid SID: $id" }
        val opcode = OpCode.SYMBOL_SID_FLEX_0 or (id and 0x7)
        buffer.writeByte(opcode.toByte())
        1 + buffer.writeFlexUInt(id ushr 3)
    }

    override fun writeSymbol(text: CharSequence) = writeScalar {
        val encodedText = utf8StringEncoder.encode(text.toString())
        val encodedTextLength = encodedText.encodedLength
        if (encodedTextLength < 16) {
            buffer.writeByte((OpCode.SYMBOL_LENGTH_0 + encodedTextLength).toByte())
            buffer.writeBytes(encodedText.buffer, 0, encodedTextLength)
            encodedTextLength + 1
        } else {
            buffer.writeByte(OpCode.VARIABLE_LENGTH_SYMBOL.toByte())
            val lengthOfLength = buffer.writeFlexUInt(encodedTextLength)
            buffer.writeBytes(encodedText.buffer, 0, encodedTextLength)
            1 + lengthOfLength + encodedTextLength
        }
    }

    override fun writeString(value: CharSequence) = writeScalar {
        val encodedText = utf8StringEncoder.encode(value.toString())
        val encodedTextLength = encodedText.encodedLength
        if (encodedTextLength < 16) {
            buffer.writeByte((OpCode.STRING_LENGTH_0 + encodedTextLength).toByte())
            buffer.writeBytes(encodedText.buffer, 0, encodedTextLength)
            encodedTextLength + 1
        } else {
            buffer.writeByte(OpCode.VARIABLE_LENGTH_STRING.toByte())
            val lengthOfLength = buffer.writeFlexUInt(encodedTextLength)
            buffer.writeBytes(encodedText.buffer, 0, encodedTextLength)
            1 + lengthOfLength + encodedTextLength
        }
    }

    override fun writeBlob(value: ByteArray, start: Int, length: Int) = writeScalar {
        buffer.writeByte(OpCode.VARIABLE_LENGTH_BLOB)
        val numLengthBytes = buffer.writeFlexUInt(value.size)
        buffer.writeBytes(value)
        1 + numLengthBytes + value.size
    }

    override fun writeClob(value: ByteArray, start: Int, length: Int) = writeScalar {
        buffer.writeByte(OpCode.VARIABLE_LENGTH_CLOB)
        val numLengthBytes = buffer.writeFlexUInt(value.size)
        buffer.writeBytes(value)
        1 + numLengthBytes + value.size
    }

    override fun stepInList(usingLengthPrefix: Boolean) {
        if (usingLengthPrefix) {
            currentContainer = containerStack.push { it.reset(PREFIXED_LIST, buffer.position()) }
            buffer.writeByte(OpCode.VARIABLE_LENGTH_LIST)
            buffer.reserve(lengthPrefixPreallocation)
        } else {
            currentContainer = containerStack.push { it.reset(DELIMITED_LIST, buffer.position()) }
            buffer.writeByte(OpCode.DELIMITED_LIST)
        }
    }

    override fun stepInSExp(usingLengthPrefix: Boolean) {
        if (usingLengthPrefix) {
            currentContainer = containerStack.push { it.reset(PREFIXED_SEXP, buffer.position()) }
            buffer.writeByte(OpCode.VARIABLE_LENGTH_SEXP)
            buffer.reserve(lengthPrefixPreallocation)
        } else {
            currentContainer = containerStack.push { it.reset(DELIMITED_SEXP, buffer.position()) }
            buffer.writeByte(OpCode.DELIMITED_SEXP)
        }
    }

    override fun stepInStruct(usingLengthPrefix: Boolean) {
        // TODO: Check the symbol-inlining options and use FlexSym mode if appropriate.
        if (usingLengthPrefix) {
            currentContainer = containerStack.push { it.reset(PREFIXED_STRUCT_SID, buffer.position()) }
            buffer.writeByte(OpCode.VARIABLE_LENGTH_STRUCT_SID_MODE)
            buffer.reserve(lengthPrefixPreallocation)
        } else {
            currentContainer = containerStack.push { it.reset(DELIMITED_STRUCT_SID, buffer.position()) }
            buffer.writeByte(OpCode.DELIMITED_STRUCT_SID_MODE)
        }
    }

    override fun stepInEExp(name: CharSequence) {
        throw UnsupportedOperationException("Binary writer requires macros to be invoked by their ID.")
    }

    override fun stepInEExp(id: Int, usingLengthPrefix: Boolean) {
        // Length-prefixed e-expression format:
        //     F4 <flexuint-address> <flexuint-length> <args...>
        // Non-length-prefixed e-expression format:
        //     <address/opcode> <args...>

        if (usingLengthPrefix) {
            currentContainer = containerStack.push { it.reset(PREFIXED_EEXP, buffer.position()) }
            buffer.writeByte(OpCode.LENGTH_PREFIXED_MACRO_INVOCATION)
            currentContainer.metadataOffset += buffer.writeFlexUInt(id)
            buffer.reserve(lengthPrefixPreallocation)
        } else {
            currentContainer = containerStack.push { it.reset(EEXP, buffer.position()) }
            currentContainer.metadataOffset = writeEExpMacroIdWithoutLengthPrefix(id)
        }
    }

    private fun writeEExpMacroIdWithoutLengthPrefix(id: Int): Int {
        return if (id < OpCode.EXTENSIBLE_MACRO_ADDRESS_0) {
            buffer.writeByte(id.toByte())
            1
        } else {
            val biasedId = id - OpCode.EXTENSIBLE_MACRO_ADDRESS_0
            val opcode = OpCode.EXTENSIBLE_MACRO_ADDRESS_0 or (biasedId and 0x7)
            buffer.writeByte(opcode.toByte())
            1 + buffer.writeFlexUInt(biasedId ushr 3)
        }
    }

    override fun writeAbsentArgument() {
        buffer.writeByte(OpCode.NO_ARGUMENT)
        currentContainer.length++
    }

    override fun stepOut() {
        val currentContainer = currentContainer
        // The length of the current container. By the end of this method, the total must include
        // any opcodes, length prefixes, or other data that is not counted in ContainerInfo.length
        var thisContainerTotalLength: Long = currentContainer.length

        // If we have a data-model container with no child values, we can replace it with the prefixed, zero-length opcode.
        if (currentContainer.numChildren == 0 && currentContainer.type >= TE_LIST) {
            val zeroLengthOpcode = when (currentContainer.type) {
                TE_LIST, PREFIXED_LIST, DELIMITED_LIST -> OpCode.LIST_LENGTH_0
                TE_SEXP, PREFIXED_SEXP, DELIMITED_SEXP -> OpCode.SEXP_LENGTH_0
                else -> OpCode.STRUCT_LENGTH_0
            }
            thisContainerTotalLength++ // For the opcode
            buffer.truncate(currentContainer.position + 1)
            buffer.writeUInt8At(currentContainer.position, zeroLengthOpcode.toLong())
        } else when (currentContainer.type) {
            TE_LIST,
            TE_SEXP,
            TE_LIST_W_LENGTH_PREFIXED_MACRO,
            TE_SEXP_W_LENGTH_PREFIXED_MACRO -> {
                // Add one byte to account for the op code
                thisContainerTotalLength += currentContainer.metadataOffset + writeCurrentContainerLength(lengthPrefixPreallocation, currentContainer.numChildren.toLong())
            }
            PREFIXED_LIST,
            PREFIXED_SEXP -> {
                thisContainerTotalLength++
                val contentLength = currentContainer.length
                if (contentLength <= 0xF) {
                    // Clean up any unused space that was pre-allocated.
                    buffer.shiftBytesLeft(currentContainer.length.toInt(), lengthPrefixPreallocation)
                    val zeroLengthOpCode = if (currentContainer.type == PREFIXED_LIST) OpCode.LIST_LENGTH_0 else OpCode.SEXP_LENGTH_0
                    buffer.writeUInt8At(currentContainer.position, zeroLengthOpCode + contentLength)
                } else {
                    thisContainerTotalLength += writeCurrentContainerLength(lengthPrefixPreallocation)
                }
            }
            DELIMITED_LIST,
            DELIMITED_SEXP -> {
                thisContainerTotalLength += 2 // For the start and end delimiters
                buffer.writeByte(OpCode.DELIMITED_CONTAINER_END)
            }
            DELIMITED_STRUCT_SID_TO_FS,
            DELIMITED_STRUCT_SID,
            DELIMITED_STRUCT_FS -> {
                // Need a sacrificial field name before the closing delimiter. We'll use $0.
                // This works regardless of whether we're in FlexSym or SID mode.
                buffer.writeByte(PrimitiveEncoder.FLEX_ZERO)
                thisContainerTotalLength += 3 // For the start opcode, throwaway field name, and end marker
                buffer.writeByte(OpCode.DELIMITED_CONTAINER_END)
            }
            PREFIXED_STRUCT_SID,
            PREFIXED_STRUCT_SID_TO_FS -> {
                // Add one byte to account for the op code
                thisContainerTotalLength++
                val contentLength = currentContainer.length
                if (contentLength <= 0xF) {
                    // Clean up any unused space that was pre-allocated.
                    buffer.shiftBytesLeft(currentContainer.length.toInt(), lengthPrefixPreallocation)
                    val zeroLengthOpCode = OpCode.STRUCT_LENGTH_0
                    buffer.writeUInt8At(currentContainer.position, zeroLengthOpCode + contentLength)
                } else {
                    thisContainerTotalLength += writeCurrentContainerLength(lengthPrefixPreallocation)
                }
            }
            PREFIXED_STRUCT_FS -> {
                thisContainerTotalLength += 1 + writeCurrentContainerLength(lengthPrefixPreallocation)
            }
            TAGLESS_EEXP -> {
                // Nothing to do here because there's no opcode, length, or end delimiter.
            }
            EEXP -> {
                // Add this to account for the opcode/address
                thisContainerTotalLength += currentContainer.metadataOffset
            }
            PREFIXED_EEXP,
            PREFIXED_TAGLESS_EEXP -> {
                // NOTE: For the (non-tagless) prefixed case, we could check if the length is 0 to see if we can go back
                // and rewrite this as a non-length-prefixed e-exp, but we won't because that's easier done in the managed
                // writer, which already has knowledge of the macro signature.

                // Add to account for the opcode, address, and length prefix
                thisContainerTotalLength += currentContainer.metadataOffset + writeCurrentContainerLength(lengthPrefixPreallocation)
            }
            DIRECTIVE -> {
                thisContainerTotalLength += 2 // For the start and end delimiters
                buffer.writeByte(OpCode.DELIMITED_CONTAINER_END)
            }
            else -> throw IonException("Nothing to step out of.")
        }

        containerStack.pop() // This is the container we just exited. We don't need to do anything more with it.
        val newCurrentContainer = containerStack.peek()
        // Update the length of the new current container to include the length of the container that we just stepped out of.
        newCurrentContainer.length += thisContainerTotalLength
        newCurrentContainer.numChildren++
        this.currentContainer = newCurrentContainer
    }

    /**
     * Writes the length of the current container and returns the number of bytes needed to do so.
     * Transparently handles PatchPoints as necessary.
     *
     * @param numPreAllocatedLengthPrefixBytes the number of bytes that were pre-allocated for the length prefix of the
     *                                         current container.
     */
    private fun writeCurrentContainerLength(numPreAllocatedLengthPrefixBytes: Int, lengthToWrite: Long = currentContainer.length): Int {
        val lengthPosition = currentContainer.position + currentContainer.metadataOffset
        val lengthPrefixBytesRequired = flexUIntLength(lengthToWrite)
        // TODO(perf): Patch Points are required when there is less space pre-allocated than is required. However, this
        //    also uses patch points even when there is _more_ space allocated that required. Check whether it's faster
        //    to shift bytes around in order to close/cover the excess space that was pre-allocated for the length.
        if (lengthPrefixBytesRequired == numPreAllocatedLengthPrefixBytes) {
            // We have enough space, so write in the correct length.
            buffer.writeFlexIntOrUIntAt(lengthPosition, lengthToWrite, lengthPrefixBytesRequired)
        } else {
            addPatchPointsToStack()
            // All ContainerInfos are in the stack, so we know that its patchPoint is non-null.
            currentContainer.patchPoint!!.apply {
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
        while (stackIterator.hasNext() && stackIterator.next().patchPoint == null) {
            // Logic happens in the loop condition.
        }

        // The iterator cursor is now positioned on an ancestor container that has a patch point
        // Ascend back up the stack, fixing the ancestors which need a patch point assigned before us
        while (stackIterator.hasPrevious()) {
            val ancestor = stackIterator.previous()
            if (ancestor.patchPoint == null) {
                ancestor.patchPoint = patchPoints.pushAndGet { it.clear() }
            }
        }
    }

    override fun writeTaggedPlaceholder() {
        writeScalar {
            buffer.writeByte(OpCode.TAGGED_PLACEHOLDER)
            1
        }
    }

    override fun writeTaggedPlaceholderWithDefault(default: Consumer<IonRawWriter_1_1>) {
        buffer.writeByte(OpCode.TAGGED_PLACEHOLDER_WITH_DEFAULT)
        currentContainer.length++
        default.accept(this)
    }

    override fun writeTaglessPlaceholder(taglessEncodingOpcode: Int) = writeScalar {
        buffer.write2Bytes(OpCode.TAGLESS_PLACEHOLDER.toByte(), taglessEncodingOpcode.toByte())
        2
    }

    override fun stepInDirective(directiveOpcode: Int) {
        currentContainer = containerStack.push { it.reset(DIRECTIVE, buffer.position()) }
        buffer.writeByte(directiveOpcode)
    }

    override fun stepInTaglessElementList(taglessEncodingOpcode: Int) {
        currentContainer = containerStack.push { it.reset(TE_LIST, buffer.position()) }
        buffer.write2Bytes(OpCode.TAGLESS_ELEMENT_LIST.toByte(), taglessEncodingOpcode.toByte())
        currentContainer.metadataOffset++
        buffer.reserve(lengthPrefixPreallocation)
    }

    override fun stepInTaglessElementList(macroId: Int, macroName: String?, lengthPrefixed: Boolean) {
        val start = buffer.position()
        buffer.writeByte(OpCode.TAGLESS_ELEMENT_LIST)
        if (lengthPrefixed) {
            currentContainer = containerStack.push { it.reset(TE_LIST_W_LENGTH_PREFIXED_MACRO, start) }
            buffer.writeByte(OpCode.LENGTH_PREFIXED_MACRO_INVOCATION)
            currentContainer.metadataOffset += 1 + buffer.writeFlexUInt(macroId)
        } else {
            currentContainer = containerStack.push { it.reset(TE_LIST, start) }
            currentContainer.metadataOffset += writeEExpMacroIdWithoutLengthPrefix(macroId)
        }
        buffer.reserve(lengthPrefixPreallocation)
    }

    override fun stepInTaglessElementSExp(taglessEncodingOpcode: Int) {
        currentContainer = containerStack.push { it.reset(TE_SEXP, buffer.position()) }
        buffer.write2Bytes(OpCode.TAGLESS_ELEMENT_SEXP.toByte(), taglessEncodingOpcode.toByte())
        currentContainer.metadataOffset++
        buffer.reserve(lengthPrefixPreallocation)
    }

    override fun stepInTaglessElementSExp(macroId: Int, macroName: String?, lengthPrefixed: Boolean) {
        val start = buffer.position()
        buffer.writeByte(OpCode.TAGLESS_ELEMENT_SEXP)
        if (lengthPrefixed) {
            currentContainer = containerStack.push { it.reset(TE_SEXP_W_LENGTH_PREFIXED_MACRO, start) }
            buffer.writeByte(OpCode.LENGTH_PREFIXED_MACRO_INVOCATION)
            currentContainer.metadataOffset += 1 + buffer.writeFlexUInt(macroId)
        } else {
            currentContainer = containerStack.push { it.reset(TE_SEXP, start) }
            currentContainer.metadataOffset += writeEExpMacroIdWithoutLengthPrefix(macroId)
        }
        buffer.reserve(lengthPrefixPreallocation)
    }

    override fun stepInTaglessEExp() {
        when (currentContainer.type) {
            TE_LIST_W_LENGTH_PREFIXED_MACRO,
            TE_SEXP_W_LENGTH_PREFIXED_MACRO -> {
                currentContainer = containerStack.push { it.reset(PREFIXED_TAGLESS_EEXP, buffer.position()) }
                buffer.reserve(lengthPrefixPreallocation)
                currentContainer.metadataOffset = 0
            }
            TE_LIST,
            TE_SEXP -> currentContainer = containerStack.push { it.reset(TAGLESS_EEXP, buffer.position()) }
            else -> throw IonException("Cannot step into a tagless e-expression here unless in a tagless-element sequence.")
        }
    }

    override fun writeTaglessInt(implicitOpcode: Int, value: Long) {
        val currentContainer = currentContainer
        when (implicitOpcode) {
            OpCode.INT_8,
            OpCode.INT_16,
            OpCode.INT_32,
            OpCode.INT_64 -> {
                val length = implicitOpcode - OpCode.INT_0
                buffer.writeFixedIntOrUInt(value, length)
                currentContainer.length += length
            }
            OpCode.TE_UINT_8,
            OpCode.TE_UINT_16,
            OpCode.TE_UINT_32,
            OpCode.TE_UINT_64 -> {
                val length = implicitOpcode - 0xE0
                buffer.writeFixedIntOrUInt(value, length)
                currentContainer.length += length
            }
            OpCode.TE_FLEX_INT -> currentContainer.length += buffer.writeFlexInt(value)
            OpCode.TE_FLEX_UINT -> currentContainer.length += buffer.writeFlexUInt(value)
            else -> throw IonException("Not a valid tagless int opcode: $implicitOpcode")
        }
        currentContainer.numChildren++
    }

    override fun writeTaglessInt(implicitOpcode: Int, value: BigInteger) {
        val currentContainer = currentContainer
        when (implicitOpcode) {
            OpCode.INT_8,
            OpCode.INT_16,
            OpCode.INT_32,
            OpCode.INT_64 -> {
                val length = implicitOpcode - OpCode.INT_0
                buffer.writeFixedIntOrUInt(value.toLong(), length)
                currentContainer.length += length
            }
            OpCode.TE_UINT_8,
            OpCode.TE_UINT_16,
            OpCode.TE_UINT_32,
            OpCode.TE_UINT_64 -> {
                val length = implicitOpcode - 0xE0
                buffer.writeFixedIntOrUInt(value.toLong(), length)
                currentContainer.length += length
            }
            OpCode.TE_FLEX_INT -> currentContainer.length += buffer.writeFlexInt(value)
            OpCode.TE_FLEX_UINT -> currentContainer.length += buffer.writeFlexUInt(value)
            else -> throw IonException("Not a valid tagless int opcode: $implicitOpcode")
        }
        currentContainer.numChildren++
    }

    override fun writeTaglessDecimal(implicitOpcode: Int, value: BigDecimal) = writeScalar {
        val coefficientSize = buffer.writeFlexInt(value.unscaledValue())
        val exponent = -value.scale()
        buffer.writeByte(exponent)
        coefficientSize + 1
    }

    override fun writeTaglessTimestamp(implicitOpcode: Int, value: Timestamp) = writeScalar {
        TimestampEncoder_1_1.writeTaglessTimestampValue(buffer, implicitOpcode, value)
    }

    override fun writeTaglessFloat(implicitOpcode: Int, value: Float) {
        val currentContainer = currentContainer
        when (implicitOpcode) {
            OpCode.FLOAT_16 -> TODO()
            OpCode.FLOAT_32 -> {
                buffer.writeFixedIntOrUInt(floatToIntBits(value).toLong(), 4)
                currentContainer.length += 4
            }
            OpCode.FLOAT_64 -> {
                buffer.writeFixedIntOrUInt(doubleToRawLongBits(value.toDouble()), 8)
                currentContainer.length += 8
            }
            else -> throw IonException("Not a valid tagless float opcode: $implicitOpcode")
        }
        currentContainer.numChildren++
    }

    override fun writeTaglessFloat(implicitOpcode: Int, value: Double) {
        val bytesWritten = when (implicitOpcode) {
            OpCode.FLOAT_16 -> TODO()
            OpCode.FLOAT_32 -> {
                buffer.writeFixedIntOrUInt(floatToIntBits(value.toFloat()).toLong(), 4)
                4
            }
            OpCode.FLOAT_64 -> {
                buffer.writeFixedIntOrUInt(doubleToRawLongBits(value), 8)
                8
            }
            else -> throw IonException("Not a valid tagless float opcode: $implicitOpcode")
        }
        val currentContainer = currentContainer
        currentContainer.length += bytesWritten
        currentContainer.numChildren++
    }

    override fun writeTaglessSymbol(implicitOpcode: Int, id: Int) {
        val bytesWritten = when (implicitOpcode) {
            OpCode.TE_SYMBOL_FS -> buffer.writeFlexInt(id.toLong())
            else -> throw IonException("Not a valid tagless symbol id opcode: $implicitOpcode")
        }
        val currentContainer = currentContainer
        currentContainer.length += bytesWritten
        currentContainer.numChildren++
    }

    @OptIn(ExperimentalStdlibApi::class)
    override fun writeTaglessSymbol(implicitOpcode: Int, text: CharSequence) {
        val bytesWritten = when (implicitOpcode) {
            OpCode.TE_SYMBOL_FS -> buffer.writeFlexSymText(text)
            else -> throw IonException("Not a valid tagless symbol text opcode: ${implicitOpcode.toByte().toHexString()}")
        }
        val currentContainer = currentContainer
        currentContainer.length += bytesWritten
        currentContainer.numChildren++
    }

    private fun WriteBuffer.writeFlexSymText(text: CharSequence): Int {
        val encodedText = utf8StringEncoder.encode(text.toString())
        val encodedTextLength = encodedText.encodedLength
        val lengthOfLength = writeFlexInt(-1 - encodedTextLength.toLong())
        writeBytes(encodedText.buffer, 0, encodedTextLength)
        return lengthOfLength + encodedTextLength
    }
}
