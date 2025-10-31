// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode

import com.amazon.ion.Decimal
import com.amazon.ion.IntegerSize
import com.amazon.ion.IonException
import com.amazon.ion.IonReader
import com.amazon.ion.IonType
import com.amazon.ion.SymbolTable
import com.amazon.ion.SymbolToken
import com.amazon.ion.Timestamp
import com.amazon.ion.bytecode.BytecodeIonReader.AnnotationHelper.EMPTY_ANNOTATIONS
import com.amazon.ion.bytecode.ir.Debugger
import com.amazon.ion.bytecode.ir.Instructions
import com.amazon.ion.bytecode.ir.Instructions.I_REFILL
import com.amazon.ion.bytecode.ir.Operation
import com.amazon.ion.bytecode.ir.OperationKind
import com.amazon.ion.bytecode.util.ByteSlice
import com.amazon.ion.bytecode.util.BytecodeBuffer
import com.amazon.ion.bytecode.util.ConstantPool
import com.amazon.ion.impl._Private_RecyclingStack
import com.amazon.ion.impl._Private_Utils
import java.math.BigDecimal
import java.math.BigInteger
import java.util.Date

/**
 * This class implements [IonReader] for the Bytecode IR.
 *
 * TODO: When should the value accessor functions return null?
 *   The existing text, tree, and binary readers are not consistent with each other.
 */
internal class BytecodeIonReader(private var generator: BytecodeGenerator) : IonReader {

    private var bytecodeI = 0

    internal var minorVersion: Byte = 0
        private set

    private val context = EncodingContextManager()
    private var symbolTable: Array<String?> = EncodingContextManager.SYSTEM_SYMBOLS

    private var bytecodeIntList = BytecodeBuffer().also { it.add(I_REFILL) }
    private var bytecode = bytecodeIntList.unsafeGetArray()

    private var constantPool = ConstantPool(256)
    private var firstLocalConstant = 0

    private var instruction = 0
    private var fieldNameIndex = -1
    private var annotationsIndex = -1
    private var annotationCount: Byte = 0

    private var labelIndex = -1

    private var isInStruct = false
    private var isNextAlreadyLoaded = false

    private val containerStack = _Private_RecyclingStack(10) { ContainerInfo() }

    // This has higher overhead than using standard library methods. Only use this when there's a chance that
    // the conversion in fallible and/or lossy.
    private val scalarConverter = ScalarConversionHelper()

    data class ContainerInfo(
        @JvmField var isStruct: Boolean = false,
        /** The index of the first instruction after this container. */
        @JvmField var bytecodeI: Int = -1,
    )

    companion object {
        const val INSTRUCTION_NOT_SET = 0
    }

    override fun close() {}

    private fun refillBytecode(): IntArray {
        val bytecodeIntList = bytecodeIntList
        bytecodeIntList.clear()
        val constantPool = constantPool
        constantPool.truncate(firstLocalConstant)
        val context = context
        generator.refill(
            bytecodeIntList,
            constantPool,
            context.getEffectiveMacroTableBytecode(),
            context.getEffectiveMacroTableOffsets(),
            symbolTable
        )
        bytecodeIntList.add(I_REFILL)
        val bytecodeArray = bytecodeIntList.unsafeGetArray()
        this.bytecode = bytecodeArray
        return bytecodeArray
    }

    override fun hasNext(): Boolean {
        val result = next() != null
        isNextAlreadyLoaded = true
        return result
    }

    override fun next(): IonType? {
        if (isNextAlreadyLoaded) {
            isNextAlreadyLoaded = false
            return type
        }

        var bytecode = bytecode
        var i = bytecodeI
        var instruction = instruction
        var operationKind: Int

        var annotationStart = 0 // Stores the start of the annotations, offset by 1 (for bit-twiddling reasons).
        var annotationFlag = -1 // -1 if not set, 0 if set. Used to avoid branches in the annotation case.
        var annotationCount = 0
        var labelIndex = labelIndex

        do {
            // Move `i` to point to the next instruction.
            val length = Instructions.getData(instruction)
            val operandCountBits = Instructions.getOperandCountBits(instruction)
            // equivalent to `i += if (operandsToSkip == 3) length else operandsToSkip`
            // `useOperandCount` is all zeros if `operandsToSkip` is 3, and all ones if `operandsToCount` is smaller than 3.
            val useOperandCount = ((operandCountBits - 3) shr 2)
            i += (operandCountBits and useOperandCount) or (length and useOperandCount.inv())

            // Load the next instruction
            instruction = bytecode[i++]
            operationKind = Operation.toOperationKind(Instructions.toOperation(instruction))

            when (operationKind) {
                OperationKind.NULL,
                OperationKind.BOOL,
                OperationKind.INT,
                OperationKind.FLOAT,
                OperationKind.DECIMAL,
                OperationKind.TIMESTAMP,
                OperationKind.STRING,
                OperationKind.SYMBOL,
                OperationKind.BLOB,
                OperationKind.CLOB,
                OperationKind.LIST,
                OperationKind.SEXP,
                OperationKind.STRUCT -> break
                OperationKind.END -> {
                    // TODO(perf): See if there is any benefit to splitting these into different operation kinds.
                    when (instruction) {
                        // return so that we don't update the current state so that if the user keeps calling `next()` they keep encountering this.
                        Instructions.I_END_CONTAINER -> return null
                        // break so that we do update the current state so that if the user calls `next()`, we can check the generator for more data.
                        Instructions.I_END_OF_INPUT -> break
                        else -> TODO("${Debugger.renderSingleInstruction(instruction)} at ${i - 1}")
                    }
                }
                OperationKind.FIELD_NAME -> fieldNameIndex = i - 1
                OperationKind.ANNOTATIONS -> {
                    annotationStart = (annotationStart or (i and annotationFlag))
                    annotationCount++
                    annotationFlag = 0
                }
                OperationKind.IVM -> handleIvm(instruction)
                OperationKind.REFILL -> {
                    bytecode = refillBytecode()
                    i = 0
                }
                OperationKind.DIRECTIVE -> {
                    i = handleSystemValue(instruction, i)
                }
                OperationKind.ARGUMENT -> {
                    // In other words, an absent argument.
                    annotationStart = 0
                    annotationFlag = -1
                    annotationCount = 0
                }
                OperationKind.METADATA -> {
                    labelIndex = i - 1
                    continue
                }
                else -> TODO("${OperationKind.nameOf(operationKind)} at ${i - 1}")
            }
        } while (true)

        if (labelIndex != -1) {
            this.labelIndex = labelIndex
        }

        this.bytecodeI = i
        this.instruction = instruction
        this.annotationsIndex = annotationStart - 1
        this.annotationCount = annotationCount.toByte()

        return OperationKind.ionTypeOf(operationKind)
    }

    private fun handleIvm(instruction: Int) {
        val ionVersionInt = Instructions.getData(instruction)
        minorVersion = ionVersionInt.toByte()
        generator = generator.getGeneratorForMinorVersion(ionVersionInt)
        symbolTable = EncodingContextManager.SYSTEM_SYMBOLS
        constantPool.clear()
        context.reset()
    }

    private fun handleSystemValue(instruction: Int, nextI: Int): Int {
        TODO("Implement directive handler")
    }

    override fun getType(): IonType? = OperationKind.ionTypeOf(Operation.toOperationKind(Instructions.toOperation(instruction)))

    override fun stepIn() {
        val instruction = this.instruction
        val op = Instructions.toOperation(instruction)
        val steppingIntoStruct = when (op) {
            Operation.OP_LIST_START,
            Operation.OP_SEXP_START -> false
            Operation.OP_STRUCT_START -> true
            else -> throw IonException("Not positioned on a container")
        }

        val length = Instructions.getData(instruction)
        containerStack.push {
            it.bytecodeI = bytecodeI + length
            it.isStruct = isInStruct
        }
        this.isInStruct = steppingIntoStruct
        this.instruction = INSTRUCTION_NOT_SET
        this.fieldNameIndex = -1
        this.annotationCount = 0
        this.annotationsIndex = -1
    }

    override fun stepOut() {
        val top = containerStack.pop() ?: throw IonException("Nothing to step out of.")
        this.bytecodeI = top.bytecodeI
        this.isInStruct = top.isStruct
        this.instruction = INSTRUCTION_NOT_SET
        this.fieldNameIndex = -1
        this.annotationCount = 0
        this.annotationsIndex = -1
    }

    override fun isInStruct(): Boolean = isInStruct

    override fun getDepth(): Int = containerStack.size()

    private object AnnotationHelper {
        @JvmStatic
        val EMPTY_ANNOTATIONS: Array<String?> = emptyArray()
        @JvmStatic
        val EMPTY_ITERATOR: Iterator<String?> = object : Iterator<String?> {
            override fun hasNext(): Boolean = false
            override fun next(): Nothing = throw NoSuchElementException()
        }
    }

    override fun getTypeAnnotations(): Array<String?> {
        val nAnnotations = annotationCount.toInt()
        if (nAnnotations == 0) {
            return EMPTY_ANNOTATIONS
        }
        val result = arrayOfNulls<String>(nAnnotations)
        var p = annotationsIndex
        val bytecode = this.bytecode
        for (i in 0 until nAnnotations) {
            val instruction = bytecode[p++]
            val data = Instructions.getData(instruction)
            result[i] = when (Instructions.toOperation(instruction)) {
                Operation.OP_ANNOTATION_CP -> constantPool[data] as String?
                Operation.OP_ANNOTATION_SID -> symbolTable[data]
                Operation.OP_ANNOTATION_REF -> {
                    val position = bytecode[p++]
                    generator.readTextReference(position, data)
                }
                else -> throw IllegalStateException("annotation $i does not point to an annotation; was ${Debugger.renderSingleInstruction(instruction)}")
            }
        }
        return result
    }

    private fun createSymbolToken(text: String?, sid: Int): SymbolToken = _Private_Utils.newSymbolToken(text, sid)

    override fun getTypeAnnotationSymbols(): Array<SymbolToken> {

        val nAnnotations = annotationCount.toInt()
        var p = annotationsIndex
        val bytecode = this.bytecode
        val result = Array(nAnnotations) { i ->

            val instruction = bytecode[p++]
            val data = Instructions.getData(instruction)
            when (Instructions.toOperation(instruction)) {
                Operation.OP_ANNOTATION_CP -> {
                    val text = constantPool[data] as String?
                    createSymbolToken(text, -1)
                }
                Operation.OP_ANNOTATION_SID -> {
                    val text = symbolTable[data]
                    createSymbolToken(text, data)
                }
                Operation.OP_ANNOTATION_REF -> {
                    val position = bytecode[p++]
                    val text = generator.readTextReference(position, data)
                    createSymbolToken(text, -1)
                }
                else -> throw IllegalStateException("annotation $i does not point to an annotation; was ${Debugger.renderSingleInstruction(instruction)}")
            }
        }
        return result
    }

    // TODO(perf): We could make this into lazy iterator.
    override fun iterateTypeAnnotations(): Iterator<String?> = if (annotationCount.toInt() == 0) AnnotationHelper.EMPTY_ITERATOR else typeAnnotations.iterator()

    override fun getFieldId(): Int {
        val fieldName = fieldNameIndex
        if (fieldName < 0) return -1
        val fieldInstruction = bytecode[fieldName]
        return when (Instructions.toOperation(fieldInstruction)) {
            Operation.OP_FIELD_NAME_CP -> -1
            Operation.OP_FIELD_NAME_SID -> Instructions.getData(fieldInstruction)
            Operation.OP_FIELD_NAME_REF -> -1
            else -> throw IllegalStateException("Field name index does not point to a field name; was ${Debugger.renderSingleInstruction(instruction)}")
        }
    }

    override fun getFieldName(): String? {
        val fieldName = fieldNameIndex
        if (fieldName < 0) return null
        val fieldInstruction = bytecode[fieldName]
        val data = Instructions.getData(fieldInstruction)
        return when (Instructions.toOperation(fieldInstruction)) {
            Operation.OP_FIELD_NAME_CP -> constantPool[data] as String?
            Operation.OP_FIELD_NAME_SID -> symbolTable[data]
            Operation.OP_FIELD_NAME_REF -> {
                val position = bytecode[fieldName + 1]
                generator.readTextReference(position, data)
            }
            else -> throw IllegalStateException("Field name index does not point to a field name; was ${Debugger.renderSingleInstruction(instruction)}")
        }
    }

    override fun getFieldNameSymbol(): SymbolToken? {
        val fieldName = fieldNameIndex
        if (fieldName < 0) return null
        val fieldInstruction = bytecode[fieldName]
        val data = Instructions.getData(fieldInstruction)
        return when (Instructions.toOperation(fieldInstruction)) {
            Operation.OP_FIELD_NAME_CP -> {
                val text = constantPool[data] as String?
                createSymbolToken(text, -1)
            }
            Operation.OP_FIELD_NAME_SID -> {
                val text = symbolTable[data]
                createSymbolToken(text, data)
            }
            Operation.OP_FIELD_NAME_REF -> {
                val position = bytecode[fieldName + 1]
                val text = generator.readTextReference(position, data)
                createSymbolToken(text, -1)
            }
            else -> throw IllegalStateException("Field name index does not point to a field name; was ${Debugger.renderSingleInstruction(instruction)}")
        }
    }

    override fun isNullValue(): Boolean {
        val op = Instructions.toOperation(instruction)
        return Operation.NULL_VARIANT == (op and Operation.NULL_VARIANT)
    }

    override fun booleanValue(): Boolean {
        val instruction = this.instruction
        if (Instructions.toOperation(instruction) != Operation.OP_BOOL) {
            throw IonException("Not positioned on a boolean")
        }
        val bool = (instruction and 1) == 1
        return bool
    }

    override fun getIntegerSize(): IntegerSize? {
        val instruction = instruction
        return when (Instructions.toOperation(instruction)) {
            Operation.OP_INT_I16 -> IntegerSize.INT
            Operation.OP_INT_I32 -> IntegerSize.INT
            Operation.OP_INT_I64 -> IntegerSize.LONG
            Operation.OP_INT_CP -> IntegerSize.BIG_INTEGER
            Operation.OP_INT_REF -> {
                val length = Instructions.getData(instruction)
                when (length) {
                    // TODO: Check the size of the materialized value to see how big it really is?
                    // TODO: This might not be accurate for Ion 1.0 since it has unsigned integer encodings.
                    1, 2, 3, 4 -> IntegerSize.INT
                    5, 6, 7, 8 -> IntegerSize.LONG
                    else -> IntegerSize.BIG_INTEGER
                }
            }
            else -> null
        }
    }

    override fun intValue(): Int {
        val instruction = this.instruction
        val op = Instructions.toOperation(instruction)
        val i = bytecodeI
        val int = when (op) {
            Operation.OP_INT_I16 -> Instructions.getData(instruction)
            Operation.OP_INT_I32 -> bytecode[i]
            Operation.OP_INT_I64 -> scalarConverter.from(longValue()).intoInt()
            Operation.OP_INT_CP,
            Operation.OP_INT_REF -> scalarConverter.from(bigIntegerValue()).intoInt()
            Operation.OP_DECIMAL_CP,
            Operation.OP_DECIMAL_REF -> scalarConverter.from(decimalValue()).intoInt()
            Operation.OP_FLOAT_F32,
            Operation.OP_FLOAT_F64 -> scalarConverter.from(doubleValue()).intoInt()
            // TODO: Other numeric value types
            else -> throw IonException("Not positioned on an Int value")
        }
        return int
    }

    override fun longValue(): Long {
        val instruction = this.instruction
        val op = Instructions.toOperation(instruction)
        var i = bytecodeI
        val long = when (op) {
            Operation.OP_INT_I16 -> Instructions.getData(instruction).toLong()
            Operation.OP_INT_I32 -> bytecode[i].toLong()
            Operation.OP_INT_I64 -> {
                val msb = bytecode[i++].toLong() and 0xFFFFFFFF
                val lsb = bytecode[i].toLong() and 0xFFFFFFFF
                (msb shl 32) or lsb
            }
            Operation.OP_INT_CP,
            Operation.OP_INT_REF -> scalarConverter.from(bigIntegerValue()).intoLong()
            Operation.OP_DECIMAL_CP,
            Operation.OP_DECIMAL_REF -> scalarConverter.from(decimalValue()).intoLong()
            Operation.OP_FLOAT_F32,
            Operation.OP_FLOAT_F64 -> scalarConverter.from(doubleValue()).intoLong()
            // TODO: Other numeric value types
            else -> throw IonException("Not positioned on an Int value")
        }
        return long
    }

    override fun bigIntegerValue(): BigInteger {
        val instruction = this.instruction
        val op = Instructions.toOperation(instruction)
        var i = bytecodeI
        val bigInt = when (op) {
            Operation.OP_INT_I16 -> Instructions.getData(instruction).toBigInteger()
            Operation.OP_INT_I32 -> bytecode[i].toLong().toBigInteger()
            Operation.OP_INT_I64 -> {
                val msb = bytecode[i++].toLong() and 0xFFFFFFFF
                val lsb = bytecode[i].toLong() and 0xFFFFFFFF
                ((msb shl 32) or lsb).toBigInteger()
            }
            Operation.OP_INT_CP -> {
                val cpIndex = Instructions.getData(instruction)
                constantPool[cpIndex] as BigInteger
            }
            Operation.OP_INT_REF -> {
                val length = Instructions.getData(instruction)
                val position = bytecode[bytecodeI]
                generator.readBigIntegerReference(position, length)
            }
            Operation.OP_DECIMAL_CP,
            Operation.OP_DECIMAL_REF -> scalarConverter.from(decimalValue()).intoBigInteger()
            Operation.OP_FLOAT_F32,
            Operation.OP_FLOAT_F64 -> scalarConverter.from(doubleValue()).intoBigInteger()
            else -> throw IonException("Not positioned on an Int value")
        }
        return bigInt
    }

    override fun doubleValue(): Double {
        var i = bytecodeI
        val op = Instructions.toOperation(instruction)
        val bytecode = bytecode
        val double = when (op) {
            Operation.OP_FLOAT_F64 -> {
                val msb = bytecode[i++].toLong() and 0xFFFFFFFF
                val lsb = bytecode[i].toLong() and 0xFFFFFFFF
                Double.fromBits((msb shl 32) or lsb)
            }
            Operation.OP_FLOAT_F32 -> Float.fromBits(bytecode[i]).toDouble()
            Operation.OP_INT_I16,
            Operation.OP_INT_I32 -> scalarConverter.from(intValue()).intoDouble()
            Operation.OP_INT_I64 -> scalarConverter.from(longValue()).intoDouble()
            Operation.OP_INT_CP,
            Operation.OP_INT_REF -> scalarConverter.from(bigIntegerValue()).intoDouble()
            Operation.OP_DECIMAL_CP,
            Operation.OP_DECIMAL_REF -> scalarConverter.from(decimalValue()).intoDouble()
            else -> throw IonException("Not positioned on an Float value")
        }
        return double
    }

    override fun bigDecimalValue(): BigDecimal? = decimalValue()

    override fun decimalValue(): Decimal? {
        val instruction = this.instruction
        val op = Instructions.toOperation(instruction)
        val data = Instructions.getData(instruction)
        return when (op) {
            Operation.OP_DECIMAL_REF -> Decimal.valueOf(generator.readDecimalReference(position = bytecode[bytecodeI], length = data))
            Operation.OP_DECIMAL_CP -> Decimal.valueOf(constantPool[data] as BigDecimal)
            Operation.OP_NULL_DECIMAL -> null
            // Other numeric types
            Operation.OP_INT_I16,
            Operation.OP_INT_I32,
            Operation.OP_INT_I64 -> Decimal.valueOf(longValue())
            Operation.OP_INT_CP,
            Operation.OP_INT_REF -> Decimal.valueOf(bigIntegerValue())
            Operation.OP_FLOAT_F32,
            Operation.OP_FLOAT_F64 -> scalarConverter.from(doubleValue()).intoDecimal()
            else -> throw IonException("Not positioned on a decimal value")
        }
    }

    override fun dateValue(): Date? = timestampValue()?.dateValue()

    override fun timestampValue(): Timestamp? {
        val i = bytecodeI
        val instruction = this.instruction
        val op = Instructions.toOperation(instruction)
        val data = Instructions.getData(instruction)
        return when (op) {
            Operation.OP_TIMESTAMP_CP -> constantPool[data] as Timestamp
            Operation.OP_SHORT_TIMESTAMP_REF -> generator.readShortTimestampReference(position = bytecode[i], opcode = data)
            Operation.OP_TIMESTAMP_REF -> generator.readTimestampReference(position = bytecode[i], length = data)
            Operation.OP_NULL_TIMESTAMP -> null
            else -> throw IonException("Not positioned on a timestamp value")
        }
    }

    override fun stringValue(): String? {
        val i = bytecodeI
        val data = Instructions.getData(instruction)
        return when (Instructions.toOperation(instruction)) {
            Operation.OP_NULL_STRING,
            Operation.OP_NULL_SYMBOL -> null
            Operation.OP_STRING_CP,
            Operation.OP_SYMBOL_CP -> constantPool[data] as String?
            Operation.OP_SYMBOL_REF,
            Operation.OP_STRING_REF -> generator.readTextReference(position = bytecode[i], length = data)
            Operation.OP_SYMBOL_CHAR -> data.toChar().toString()
            Operation.OP_SYMBOL_SID -> symbolTable[data]
            else -> throw IonException("Not positioned on a string or symbol value")
        }
    }

    override fun symbolValue(): SymbolToken? {
        val instruction = this.instruction
        val op = Instructions.toOperation(instruction)
        val data = Instructions.getData(instruction)
        val i = bytecodeI
        return when (op) {
            Operation.OP_NULL_SYMBOL -> null
            Operation.OP_SYMBOL_CP -> createSymbolToken(constantPool[data] as String, -1)
            Operation.OP_SYMBOL_REF -> createSymbolToken(generator.readTextReference(position = bytecode[i], length = data), -1)
            Operation.OP_SYMBOL_CHAR -> createSymbolToken(data.toChar().toString(), -1)
            Operation.OP_SYMBOL_SID -> createSymbolToken(symbolTable[data], data)
            else -> throw IonException("Not positioned on a symbol")
        }
    }

    // TODO: don't return null
    override fun getSymbolTable(): SymbolTable? = null

    override fun byteSize(): Int {
        val instruction = this.instruction
        val op = Instructions.toOperation(instruction)
        val data = Instructions.getData(instruction)
        return when (op) {
            Operation.OP_BLOB_CP,
            Operation.OP_CLOB_CP -> (constantPool[data] as ByteSlice).length
            Operation.OP_BLOB_REF,
            Operation.OP_CLOB_REF -> data
            else -> throw IonException("Not positioned on a lob value")
        }
    }

    override fun newBytes(): ByteArray {
        val instruction = this.instruction
        val op = Instructions.toOperation(instruction)
        val data = Instructions.getData(instruction)
        val i = bytecodeI
        return when (op) {
            Operation.OP_BLOB_CP,
            Operation.OP_CLOB_CP -> (constantPool[data] as ByteSlice).newByteArray()
            Operation.OP_BLOB_REF,
            Operation.OP_CLOB_REF -> generator.readBytesReference(position = bytecode[i], length = data).newByteArray()
            else -> throw IonException("Not positioned on a lob value")
        }
    }

    override fun getBytes(buffer: ByteArray, offset: Int, len: Int): Int {
        val instruction = this.instruction
        val op = Instructions.toOperation(instruction)
        val data = Instructions.getData(instruction)
        val i = bytecodeI
        val slice = when (op) {
            Operation.OP_BLOB_CP,
            Operation.OP_CLOB_CP -> (constantPool[data] as ByteSlice)
            Operation.OP_BLOB_REF,
            Operation.OP_CLOB_REF -> generator.readBytesReference(position = bytecode[i], length = data)
            else -> throw IonException("Not positioned on a lob value")
        }
        slice.bytes.copyInto(buffer, offset, slice.startInclusive, slice.endExclusive)
        return slice.length
    }

    override fun <T : Any?> asFacet(facetType: Class<T>?): T = TODO("Not yet implemented")
}
