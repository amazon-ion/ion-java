package com.amazon.ion.impl

import com.amazon.ion.*
import com.amazon.ion.impl.BytecodeOpcodes.opToInstruction
import com.amazon.ion.impl.macro.*
import java.math.BigDecimal
import java.math.BigInteger

class Bytecode(body: List<Expression>) {

    val bytecode: IntArray
    val constants: Array<Any?>
    val primitives: LongArray

    init {
        val constants = mutableListOf<Any?>()
        val primitives = mutableListOf<Long>()
        val bytecode = mutableListOf<Int>()

        //if (body != null) {
            toByteCode(body, bytecode, constants, primitives)
        //}

        this.constants = constants.toTypedArray()
        this.bytecode = bytecode.toIntArray()
        this.primitives = primitives.toLongArray()
    }

    fun getOpcode(index: Int): Int {
        return bytecode[index]
    }

    fun getLong(index: Int): Long {
        return primitives[index]
    }

    fun getDouble(index: Int): Double {
        return Double.fromBits(primitives[index])
    }

    fun getString(index: Int): String {
        return constants[index] as String
    }

    fun getBigInteger(index: Int): BigInteger {
        return constants[index] as BigInteger
    }

    fun getBigDecimal(index: Int): BigDecimal {
        return constants[index] as BigDecimal
    }

    fun getTimestamp(index: Int): Timestamp {
        return constants[index] as Timestamp
    }

    fun getMacro(index: Int): Macro {
        return constants[index] as Macro
    }

    fun size(): Int {
        return bytecode.size
    }

    // TODO getters for other types

    companion object {
        @JvmStatic
        fun toByteCode(
            expressions: List<Expression>,
            bytecode: MutableList<Int>,
            constants: MutableList<Any?>,
            primitives: MutableList<Long>
        ) {
            // TODO: Deduplicate the constant and primitive pools?
            toByteCode(expressions, 0, expressions.size, bytecode, constants, primitives)
        }

        @JvmStatic
        fun toByteCode(
            expressions: List<Expression>,
            startIndex: Int,
            endIndex: Int,
            bytecode: MutableList<Int>,
            constants: MutableList<Any?>,
            primitives: MutableList<Long>
        ) {
            var i = startIndex
            while (i < endIndex) {
                val expr = expressions[i++]
                if (expr is Expression.DataModelValue && expr.annotations.isNotEmpty()) {
                    val anns = expr.annotations
                    if (anns.size == 1) {
                        val ann = anns[0]
                        val cpIndex = constants.size
                        constants.add(ann)
                        bytecode.add(BytecodeOpcodes.OP_CP_ONE_ANNOTATION.opToInstruction(cpIndex))
                    } else {
                        bytecode.add(BytecodeOpcodes.OP_CP_N_ANNOTATIONS.opToInstruction(anns.size))
                        for (ann in anns) {
                            val cpIndex = constants.size
                            constants.add(ann)
                            bytecode.add(cpIndex)
                        }
                    }
                }
                when (expr) {
                    is Expression.NullValue -> when (expr.type) {
                        IonType.NULL -> bytecode.add(BytecodeOpcodes.OP_NULL_NULL.opToInstruction())
                        else -> bytecode.add(BytecodeOpcodes.OP_NULL_TYPED.opToInstruction(expr.type.ordinal))
                    }

                    is Expression.BoolValue -> bytecode.add(BytecodeOpcodes.OP_BOOL.opToInstruction(if (expr.value) 1 else 0))

                    is Expression.IntValue -> {
                        if (expr is Expression.BigIntValue) {
                            val cpIndex = constants.size
                            constants.add(expr.value)
                            bytecode.add(BytecodeOpcodes.OP_CP_BIG_INT.opToInstruction(cpIndex))
                        } else {
                            val longValue = expr.longValue
                            if (longValue and 0xFFFFFF == longValue) {
                                bytecode.add(BytecodeOpcodes.OP_SMALL_INT.opToInstruction(longValue.toInt()))
                            } else if (longValue.toInt().toLong() == longValue) {
                                bytecode.add(BytecodeOpcodes.OP_INLINE_INT.opToInstruction())
                                bytecode.add(longValue.toInt())
                            } else {
                                val cpIndex = primitives.size
                                primitives.add(longValue)
                                bytecode.add(BytecodeOpcodes.OP_CP_LONG.opToInstruction(cpIndex))
                            }
                        }
                    }

                    is Expression.FloatValue -> {
                        val cpIndex = constants.size
                        primitives.add(expr.value.toRawBits())
                        bytecode.add(BytecodeOpcodes.OP_CP_FLOAT.opToInstruction(cpIndex))
                    }

                    is Expression.DecimalValue -> {
                        // TODO: Special case for zero
                        val cpIndex = constants.size
                        constants.add(expr.value)
                        bytecode.add(BytecodeOpcodes.OP_CP_DECIMAL.opToInstruction(cpIndex))
                    }

                    is Expression.TimestampValue -> {
                        val cpIndex = constants.size
                        constants.add(expr.value)
                        bytecode.add(BytecodeOpcodes.OP_CP_TIMESTAMP.opToInstruction(cpIndex))
                    }

                    is Expression.StringValue -> {
                        val cpIndex = constants.size
                        constants.add(expr.value)
                        bytecode.add(BytecodeOpcodes.OP_CP_STRING.opToInstruction(cpIndex))
                    }

                    is Expression.SymbolValue -> {
                        val cpIndex = constants.size
                        constants.add(expr.stringValue)
                        bytecode.add(BytecodeOpcodes.OP_CP_SYMBOL.opToInstruction(cpIndex))
                    }

                    is Expression.BlobValue -> TODO()
                    is Expression.ClobValue -> TODO()
                    is Expression.ListValue -> {
                        // TODO: Handle empty case
                        // Delimited
                        bytecode.add(BytecodeOpcodes.OP_LIST_START.opToInstruction()) // TODO include number of ints in bytecode for quick skip
                        toByteCode(expressions, i, expr.endExclusive, bytecode, constants, primitives)
                        bytecode.add(BytecodeOpcodes.OP_LIST_END.opToInstruction())
                        i = expr.endExclusive
                        // Prefixed
//                        val listIndex = bytecode.size
//                        bytecode.add(TDLBytecode.OP_LIST_START.opToInstruction())
//                        val start = bytecode.size
//                        val content = expr.childExpressions
//                        toByteCode(content, bytecode, constants, primitives)
//                        val end = bytecode.size
//                        bytecode[listIndex] = TDLBytecode.OP_LIST_PREFIXED.opToInstruction(end - start)
                    }

                    is Expression.StructValue -> {
                        // TODO: Handle empty case
                        // Delimited
                        bytecode.add(BytecodeOpcodes.OP_STRUCT_START.opToInstruction()) // TODO include number of ints in bytecode for quick skip
                        toByteCode(expressions, i, expr.endExclusive, bytecode, constants, primitives)
                        bytecode.add(BytecodeOpcodes.OP_STRUCT_END.opToInstruction())
                        i = expr.endExclusive
                    }

                    is Expression.SExpValue -> {
                        // TODO: Handle empty case
                        // Delimited
                        bytecode.add(BytecodeOpcodes.OP_SEXP_START.opToInstruction()) // TODO include number of ints in bytecode for quick skip
                        toByteCode(expressions, i, expr.endExclusive, bytecode, constants, primitives)
                        bytecode.add(BytecodeOpcodes.OP_SEXP_END.opToInstruction())
                        i = expr.endExclusive
                    }

                    is Expression.FieldName -> {
                        val cpIndex = constants.size
                        constants.add(expr.value.assumeText())
                        bytecode.add(BytecodeOpcodes.OP_CP_FIELD_NAME.opToInstruction(cpIndex))
                    }

                    is Expression.ExpressionGroup -> {
                        val exprGroupIndex = bytecode.size
                        bytecode.add(BytecodeOpcodes.OP_ARGUMENT_VALUE.opToInstruction())
                        val start = bytecode.size
                        toByteCode(expressions, i, expr.endExclusive, bytecode, constants, primitives)
                        val end = bytecode.size
                        bytecode[exprGroupIndex] = BytecodeOpcodes.OP_ARGUMENT_VALUE.opToInstruction(end - start)
                    }

                    is Expression.VariableRef -> {
                        bytecode.add(BytecodeOpcodes.OP_ARGUMENT_REF_TYPE.opToInstruction(expr.signatureIndex))
                    }

                    is Expression.InvokableExpression -> {
                        var j = i
                        while (j < expr.endExclusive) {
                            val arg = expressions[j]
                            val argIndex = bytecode.size
                            bytecode.add(BytecodeOpcodes.OP_ARGUMENT_VALUE.opToInstruction())
                            val start = bytecode.size
                            if (arg is Expression.HasStartAndEnd) {
                                toByteCode(expressions, j + 1, arg.endExclusive, bytecode, constants, primitives)
                                j = arg.endExclusive
                            } else {
                                toByteCode(expressions, j, j + 1, bytecode, constants, primitives)
                                j++;
                            }
                            val end = bytecode.size
                            bytecode[argIndex] = (BytecodeOpcodes.OP_ARGUMENT_VALUE.opToInstruction(end - start))
                        }
                        val cpIndex = constants.size
                        constants.add(expr.macro)
                        bytecode.add(BytecodeOpcodes.OP_INVOKE_MACRO.opToInstruction(cpIndex))
                    }

                    //Kind.LITERAL -> TODO()
                    else -> throw IllegalStateException("Invalid Expression: $expr")
                }
            }
        }
    }
}
