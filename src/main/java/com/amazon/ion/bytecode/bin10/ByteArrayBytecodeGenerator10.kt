// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin10

import com.amazon.ion.IonException
import com.amazon.ion.SystemSymbols
import com.amazon.ion.bytecode.BytecodeEmitter
import com.amazon.ion.bytecode.BytecodeGenerator
import com.amazon.ion.bytecode.ir.Instructions
import com.amazon.ion.bytecode.ir.Instructions.I_BLOB_REF
import com.amazon.ion.bytecode.ir.Instructions.I_CLOB_REF
import com.amazon.ion.bytecode.ir.Instructions.I_DECIMAL_REF
import com.amazon.ion.bytecode.ir.Instructions.I_END_OF_INPUT
import com.amazon.ion.bytecode.ir.Instructions.I_STRING_REF
import com.amazon.ion.bytecode.ir.Instructions.I_TIMESTAMP_REF
import com.amazon.ion.bytecode.ir.Instructions.packInstructionData
import com.amazon.ion.bytecode.ir.OperationKind
import com.amazon.ion.bytecode.util.AppendableConstantPoolView
import com.amazon.ion.bytecode.util.ByteSlice
import com.amazon.ion.bytecode.util.BytecodeBuffer
import com.amazon.ion.bytecode.util.unsignedToInt
import com.amazon.ion.impl.bin.utf8.Utf8StringDecoderPool
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import java.lang.IllegalStateException
import java.math.BigInteger
import java.nio.Buffer
import java.nio.ByteBuffer
import java.util.Arrays
import kotlin.math.min

/**
 * A bytecode generator for Ion 1.0 binary encoding.
 *
 * #### Note on integer values
 *
 * Because there are separate positive/negative opcodes for integers in Ion 1.0, the data referenced in an INT_REF
 * instruction only includes the magnitude and not the sign. To avoid this ambiguity, this implementation eagerly
 * materializes all big integers and places them in the constant pool.
 */
@SuppressFBWarnings("EI_EXPOSE_REP2", justification = "constructor does not make a defensive copy of source as a performance optimization")
internal class ByteArrayBytecodeGenerator10(
    private val source: ByteArray,
    private var i: Int,
) : BytecodeGenerator {

    private val decoder = Utf8StringDecoderPool.getInstance().getOrCreate()
    private val scratchBuffer = ByteBuffer.wrap(source)
    private var scratchArray = ByteArray(32)
    private val symbolTableHelper = SymbolTableHelper

    private fun getScratchArray(minCapacity: Int): ByteArray {
        if (scratchArray.size < minCapacity) {
            scratchArray = ByteArray(minCapacity)
        }
        return scratchArray
    }

    override fun refill(
        destination: BytecodeBuffer,
        constantPool: AppendableConstantPoolView,
        macroSrc: IntArray,
        macroIndices: IntArray,
        symTab: Array<String?>
    ) {
        val source = source
        i += compileTopLevelValues(source, i, destination, constantPool, source.size)

        if (i == source.size) {
            destination.add(I_END_OF_INPUT)
        }
    }

    override fun readBigIntegerReference(position: Int, length: Int): BigInteger {
        throw IllegalStateException("Should be unreachable. All BigIntegers are eagerly parsed.")
    }

    override fun readDecimalReference(position: Int, length: Int) = readDecimalReference(source, position, position + length)

    override fun readShortTimestampReference(position: Int, opcode: Int) = throw IllegalStateException("Should be unreachable. Not supported for Ion 1.0")

    override fun readTimestampReference(position: Int, length: Int) = readTimestampReference(source, position, length)

    override fun readTextReference(position: Int, length: Int): String {
        val b = scratchBuffer
        // We have to cast to `Buffer` here because JDK 17 added an override that returns `ByteBuffer`.
        // The compiler seems to prefer that version, rather than the base method (which returns `Buffer`), and so
        // running the tests with JDK 8 fails without this cast.
        (b as Buffer).limit(position + length)
        (b as Buffer).position(position)
        return decoder.decode(b, length)
    }

    override fun readBytesReference(position: Int, length: Int): ByteSlice = ByteSlice(source, position, position + length)

    override fun ionMinorVersion(): Int = 0

    override fun getGeneratorForMinorVersion(minorVersion: Int): BytecodeGenerator {
        return when (minorVersion) {
            0 -> this
            // TODO:
            //  1 -> ByteArrayBytecodeGenerator11(source, i)
            else -> throw IonException("Unknown Ion version: 1.$minorVersion")
        }
    }

    private fun compileTopLevelValues(
        src: ByteArray,
        pos: Int,
        dest: BytecodeBuffer,
        cp: AppendableConstantPoolView,
        limit: Int,
    ): Int {
        var p = pos
        val end = min(pos + limit, src.size)

        // Try to avoid causing the destination buffer to resize.
        // This is a good enough heuristic as long as the sizes of top-level values are fairly consistent.
        // TODO(perf): Benchmark with and without this for large and small streams.
        val instructionSoftLimit = dest.capacity() * 10 / 9

        var firstAnnotationIndex = -1
        var firstAnnotationSid = -1
        var typeId: Int

        while (p < end && dest.size() < instructionSoftLimit) {

            typeId = source[p++].unsignedToInt()

            // Length includes only the bytes after the typeId.
            val lengthAndValue = getLengthForTypeId(typeId, source, p)
            val length = lengthAndValue.shr(8).toInt()
            p += lengthAndValue.and(0xFF).toInt()

            if (TypeIdHelper.isNull(typeId)) {
                compileNullValue(typeId, dest)
            } else when (TypeIdHelper.operationKindForTypeId(typeId)) {
                OperationKind.UNSET -> if (firstAnnotationIndex >= 0) throw IonException("Invalid annotation wrapper: NOP pad may not occur inside an annotation wrapper.")
                OperationKind.IVM -> {
                    if (firstAnnotationIndex >= 0) throw IonException("Invalid annotation wrapper: IVM may not occur inside an annotation wrapper.")
                    val major = source[p++].unsignedToInt()
                    val minor = source[p++].unsignedToInt()
                    val lastByte = source[p++].unsignedToInt()
                    if (lastByte != 0xEA) throw IonException("Invalid IVM encountered. Ended with $lastByte instead of 0xEA.")
                    dest.add(Instructions.I_IVM.or(major.shl(8).or(minor)))
                    break
                }
                OperationKind.ANNOTATIONS -> {
                    if (firstAnnotationIndex >= 0) throw IonException("Invalid annotation wrapper: annotations may not occur inside an annotation wrapper.")
                    val varUIntValueAndLength = VarIntHelper.readVarUIntValueAndLength(source, p)
                    val annotationStart = p + varUIntValueAndLength.and(0xFF).toInt()
                    val valueStart = annotationStart + varUIntValueAndLength.shr(8).toInt()
                    firstAnnotationIndex = dest.size()
                    firstAnnotationSid = compileAnnotations(source, annotationStart, valueStart, dest)
                    p = valueStart
                    continue
                }
                OperationKind.BOOL -> compileBoolValue(typeId, dest)
                OperationKind.INT -> compileIntValue(typeId, source, p, length, dest, cp)
                OperationKind.FLOAT -> compileFloatValue(typeId, source, p, dest)
                OperationKind.DECIMAL -> dest.add2(I_DECIMAL_REF.or(length), p)
                OperationKind.TIMESTAMP -> dest.add2(I_TIMESTAMP_REF.or(length), p)
                OperationKind.SYMBOL -> compileSymbolValue(source, p, length, dest)
                OperationKind.STRING -> dest.add2(I_STRING_REF.or(length), p)
                OperationKind.CLOB -> dest.add2(I_CLOB_REF.or(length), p)
                OperationKind.BLOB -> dest.add2(I_BLOB_REF.or(length), p)
                OperationKind.LIST -> compileList(source, p, length, dest, cp)
                OperationKind.SEXP -> compileSExp(source, p, length, dest, cp)
                OperationKind.STRUCT -> {
                    if (firstAnnotationSid == SystemSymbols.ION_SYMBOL_TABLE_SID) {
                        dest.truncate(firstAnnotationIndex)
                        symbolTableHelper.compileSymbolTable(source, p, length, dest, cp)
                        p += length
                        break
                    } else {
                        compileStruct(source, p, length, dest, cp)
                    }
                }
                // Handled earlier.
                OperationKind.NULL -> throw IllegalArgumentException("Unreachable!")
                else -> throw IonException("Invalid Type Id: ${typeId.toString(16)}")
            }
            p += length
            firstAnnotationIndex = -1
            firstAnnotationSid = -1
        }
        return p - pos
    }

    /**
     * Returns the SID of the first annotation. (To enable easy checks for a local symbol table.)
     */
    private fun compileAnnotations(source: ByteArray, start: Int, end: Int, bytecode: BytecodeBuffer): Int {
        var p = start
        val firstSidValueAndLength = VarIntHelper.readVarUIntValueAndLength(source, p)
        val firstSid = (firstSidValueAndLength ushr 8).toInt()
        val firstSidLength = firstSidValueAndLength.toInt() and 0xFF
        p += firstSidLength
        bytecode.add(Instructions.I_ANNOTATION_SID.packInstructionData(firstSid))

        while (p < end) {
            val valueAndLength = VarIntHelper.readVarUIntValueAndLength(source, p)
            val sid = (valueAndLength ushr 8).toInt()
            val length = valueAndLength.toInt() and 0xFF
            p += length
            bytecode.add(Instructions.I_ANNOTATION_SID.packInstructionData(sid))
        }
        return firstSid
    }

    private fun compileNullValue(typeId: Int, dest: BytecodeBuffer) {
        // TODO: Make something that's a little less brittle for this line.
        val operationKind = TypeIdHelper.ionTypeForTypeId(typeId)!!.ordinal + 1
        dest.add(Instructions.typedNullFromOperationKind(operationKind))
    }

    private fun compileBoolValue(typeId: Int, dest: BytecodeBuffer) {
        dest.add(Instructions.I_BOOL.packInstructionData(typeId and 0xF))
    }

    private fun compileIntValue(typeId: Int, source: ByteArray, position: Int, length: Int, dest: BytecodeBuffer, cp: AppendableConstantPoolView) {
        val sign = signForIntTypeId(typeId)

        when (length) {
            0 -> {
                if (sign == -1) throw IonException("Int zero may not be negative")
                dest.add(Instructions.I_INT_I16)
            }
            1 -> {
                val value = source[position].toInt().and(0xFF).times(sign)
                if (value == 0 && sign == -1) throw IonException("Int zero may not be negative")
                dest.add(Instructions.I_INT_I16.packInstructionData(value))
            }
            2 -> {
                val msb = source[position].toInt().and(0xFF).shl(8)
                val lsb = source[position + 1].toInt() and 0xFF
                val value = (msb or lsb) * sign
                if (value == 0 && sign == -1) throw IonException("Int zero may not be negative")
                val numLeadingSignBits = Integer.numberOfLeadingZeros(value.shr(31).xor(value))
                if (numLeadingSignBits > 16) {
                    dest.add(Instructions.I_INT_I16.packInstructionData(value))
                } else {
                    dest.add2(Instructions.I_INT_I32, value)
                }
            }
            3 -> {
                var p = position
                var absoluteValue = 0
                absoluteValue = absoluteValue.shl(8) or source[p++].toInt().and(0xFF)
                absoluteValue = absoluteValue.shl(8) or source[p++].toInt().and(0xFF)
                absoluteValue = absoluteValue.shl(8) or source[p++].toInt().and(0xFF)
                val value = absoluteValue * sign
                if (value == 0 && sign == -1) throw IonException("Int zero may not be negative")
                val numLeadingSignBits = Integer.numberOfLeadingZeros(value.shr(31).xor(value))
                if (numLeadingSignBits > 16) {
                    dest.add(Instructions.I_INT_I16.packInstructionData(value))
                } else {
                    dest.add2(Instructions.I_INT_I32, value)
                }
            }
            4, 5, 6, 7 -> {
                val absoluteValue = readUInt(source, position, length)
                val value = absoluteValue * sign
                if (value == 0L && sign == -1) throw IonException("Int zero may not be negative")
                val minRequiredBits = Long.SIZE_BITS - java.lang.Long.numberOfLeadingZeros(value.shr(63).xor(value))
                if (minRequiredBits <= Short.SIZE_BITS) {
                    dest.add(Instructions.I_INT_I16.packInstructionData(value.toInt()))
                } else if (minRequiredBits <= Int.SIZE_BITS) {
                    dest.add2(Instructions.I_INT_I32, value.toInt())
                } else {
                    BytecodeEmitter.emitInt64Value(dest, value)
                }
            }
            else -> {
                val scratch = getScratchArray(length)
                val scratchSize = scratch.size
                val scratchPosition = scratchSize - length
                System.arraycopy(source, position, scratch, scratchPosition, length)
                val value = BigInteger(sign, scratch)
                Arrays.fill(scratch, scratchPosition, scratchSize, 0)

                if (value == BigInteger.ZERO && sign == -1) throw IonException("Int zero may not be negative")

                val minimumBytes = value.bitLength() / 8
                when (minimumBytes) {
                    0, 1 -> {
                        dest.add(Instructions.I_INT_I16.packInstructionData(value.toInt()))
                    }
                    2, 3 -> {
                        dest.add2(Instructions.I_INT_I32, value.toInt())
                    }
                    4, 5, 6, 7 -> {
                        BytecodeEmitter.emitInt64Value(dest, value.toLong())
                    }
                    else -> {
                        dest.add(Instructions.I_INT_CP.packInstructionData(cp.add(value)))
                    }
                }
            }
        }
    }

    private fun compileFloatValue(typeId: Int, source: ByteArray, position: Int, dest: BytecodeBuffer) {
        var p = position
        when (typeId and 0xF) {
            0 -> dest.add2(Instructions.I_FLOAT_F32, 0.0f.toRawBits())
            4 -> {
                // TODO(perf): See if there's any difference between this, a for loop, or manually unrolling the loop
                var bits = 0
                repeat(4) { bits = bits.shl(8) or source[p++].toInt().and(0xFF) }
                dest.add2(Instructions.I_FLOAT_F32, bits)
            }
            8 -> {
                // TODO(perf): See if there's any difference between this, a for loop, or manually unrolling the loop
                var bits = 0L
                repeat(8) { bits = bits.shl(8) or source[p++].toLong().and(0xFF) }
                BytecodeEmitter.emitDoubleValue(dest, Double.fromBits(bits))
            }
            else -> throw IonException("Encountered an illegal typeId; not a valid float length: $typeId")
        }
    }

    private fun compileSymbolValue(source: ByteArray, position: Int, length: Int, dest: BytecodeBuffer) {
        val sid = readUInt(source, position, length).toInt()
        dest.add(Instructions.I_SYMBOL_SID.packInstructionData(sid))
    }

    private fun compileList(source: ByteArray, position: Int, length: Int, dest: BytecodeBuffer, cp: AppendableConstantPoolView) {
        compileContainer(Instructions.I_LIST_START, dest) {
            var p = position
            val end = position + length
            while (p < end) p += compileChildValue(source, p, dest, cp)
        }
    }

    private fun compileSExp(source: ByteArray, position: Int, length: Int, dest: BytecodeBuffer, cp: AppendableConstantPoolView) {
        compileContainer(Instructions.I_SEXP_START, dest) {
            var p = position
            val end = position + length
            while (p < end) p += compileChildValue(source, p, dest, cp)
        }
    }

    private fun compileStruct(source: ByteArray, position: Int, length: Int, dest: BytecodeBuffer, cp: AppendableConstantPoolView) {
        compileContainer(Instructions.I_STRUCT_START, dest) {
            var p = position
            val end = position + length
            while (p < end) {
                val sidValueAndLength = VarIntHelper.readVarUIntValueAndLength(source, p)
                val sid = sidValueAndLength.ushr(8).toInt()
                p += sidValueAndLength.and(0xFF).toInt()
                dest.add(Instructions.I_FIELD_NAME_SID.or(sid))
                p += compileChildValue(source, p, dest, cp)
            }
        }
    }

    private inline fun compileContainer(instruction: Int, dest: BytecodeBuffer, content: () -> Unit) {
        val containerStartIndex = dest.reserve()
        val start = containerStartIndex + 1
        content()
        dest.add(Instructions.I_END_CONTAINER)
        val end = dest.size()
        dest[containerStartIndex] = instruction.packInstructionData(end - start)
    }

    private fun compileChildValue(source: ByteArray, position: Int, dest: BytecodeBuffer, cp: AppendableConstantPoolView, isAnnotated: Boolean = false): Int {
        var p = position

        val typeId = source[p++].unsignedToInt()

        // Length counts only the bytes after the typeId.
        val valueAndLength = getLengthForTypeId(typeId, source, p)
        p += valueAndLength.toByte()
        val length = valueAndLength.ushr(8).toInt()

        if (TypeIdHelper.isNull(typeId)) {
            compileNullValue(typeId, dest)
        } else when (TypeIdHelper.operationKindForTypeId(typeId)) {
            OperationKind.UNSET -> if (isAnnotated) throw IonException("Invalid annotation wrapper: NOP pad may not occur inside an annotation wrapper.")
            OperationKind.IVM -> throw IonException("Found IVM illegally nested in a container.")
            OperationKind.ANNOTATIONS -> {
                if (isAnnotated) throw IonException("Invalid annotation wrapper: annotations may not occur inside an annotation wrapper.")
                val varUIntValueAndLength = VarIntHelper.readVarUIntValueAndLength(source, p)
                val annotationStart = p + varUIntValueAndLength.and(0xFF).toInt()
                val valueStart = annotationStart + varUIntValueAndLength.shr(8).toInt()
                compileAnnotations(source, annotationStart, valueStart, dest)
                compileChildValue(source, valueStart, dest, cp, isAnnotated = true)
            }
            OperationKind.BOOL -> compileBoolValue(typeId, dest)
            OperationKind.INT -> compileIntValue(typeId, source, p, length, dest, cp)
            OperationKind.FLOAT -> compileFloatValue(typeId, source, p, dest)
            OperationKind.DECIMAL -> dest.add2(I_DECIMAL_REF.or(length), p)
            OperationKind.TIMESTAMP -> dest.add2(I_TIMESTAMP_REF.or(length), p)
            OperationKind.SYMBOL -> compileSymbolValue(source, p, length, dest)
            OperationKind.STRING -> dest.add2(I_STRING_REF.or(length), p)
            OperationKind.CLOB -> dest.add2(I_CLOB_REF.or(length), p)
            OperationKind.BLOB -> dest.add2(I_BLOB_REF.or(length), p)
            OperationKind.LIST -> compileList(source, p, length, dest, cp)
            OperationKind.SEXP -> compileSExp(source, p, length, dest, cp)
            OperationKind.STRUCT -> compileStruct(source, p, length, dest, cp)
            // Handled earlier.
            OperationKind.NULL -> throw IllegalStateException("Unreachable!")
            else -> throw IonException("Invalid Type Id: ${typeId.toString(16)}")
        }
        p += length
        return p - position
    }

    /**
     * Gets the length for the given TypeId, reading a VarUInt length if needed.
     * Returns 7 bytes with the length and 1 byte containing the number of bytes consumed to read the length.
     *
     * See [VarIntHelper.readVarUIntValueAndLength].
     *
     * @throws IonException if the typeId is not a legal typeId in Ion 1.0
     */
    private fun getLengthForTypeId(typeId: Int, source: ByteArray, position: Int): Long {
        return when (val l = TypeIdHelper.TYPE_LENGTHS[typeId]) {
            -1 -> VarIntHelper.readVarUIntValueAndLength(source, position)
            -2 -> throw IonException("Invalid Type ID: $typeId")
            else -> l.toLong() shl 8
        }
    }
}
