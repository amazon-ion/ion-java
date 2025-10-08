// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.ion_1_1

import com.amazon.ion.IonException
import com.amazon.ion.IonType
import com.amazon.ion.Macro
import com.amazon.ion.Timestamp
import com.amazon.ion.bytecode.ir.OperationKind
import com.amazon.ion.bytecode.util.ByteSlice
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import java.math.BigDecimal
import java.math.BigInteger
import java.util.Arrays

/**
 * Implementation of a macro that represents a template with parameters and a body of expressions.
 * Macros are used to define reusable templates that can be invoked with arguments to produce Ion values.
 */
internal class MacroImpl private constructor(private val body: Array<TemplateExpression>) : Macro {

    constructor(body: List<TemplateExpression>) : this(body.toTypedArray())

    /**
     * Represents a parameter in a macro template signature.
     * Parameters can be either tagged (general Ion values) or tagless (specific scalar types).
     *
     * @param taglessEncoding The specific scalar type for tagless parameters, or null for tagged parameters
     */
    class Parameter internal constructor(val taglessEncoding: TaglessScalarType? = null) {
        /** The opcode associated with this parameter's encoding type */
        val opcode: Int = taglessEncoding?.getOpcode() ?: 0
    }

    @get:[SuppressFBWarnings("EI_EXPOSE_REP", justification = "internal use only")]
    val signature: List<Parameter> = mutableListOf<Parameter>().also { sig -> body.forEach { addPlaceholdersToSignature(it, sig) } }

    @get:[SuppressFBWarnings("EI_EXPOSE_REP", justification = "internal use only")]
    val bodyExpressions: List<TemplateExpression> = Arrays.asList(*body)

    /**
     * The type of Ion value that is produced by this template.
     */
    val expandedIonType: IonType = findExpandedIonType(body)

    /**
     * Writes this macro's template expressions to the specified Ion writer.
     *
     * @param writer The Ion writer to write the template expressions to
     */
    fun writeTo(writer: IonRawWriter_1_1) {
        writeTemplateExpressions(body, writer)
    }

    companion object {

        @JvmStatic
        private fun findExpandedIonType(body: Array<TemplateExpression>): IonType {
            var ionType: IonType? = null
            var numberOfValues = 0
            for (expr in body) {
                if (expr.expressionKind in OperationKind.NULL..OperationKind.STRUCT) {
                    numberOfValues++
                    // TODO: Create a proper mapping from Kind to IonType.
                    ionType = IonType.entries[expr.expressionKind - 1]
                }
            }
            if (numberOfValues != 1) {
                throw IonException("Template macros must produce exactly one value.")
            }
            return ionType!!
        }

        /**
         * Recursively traverses template expressions to identify placeholders and add them to the macro signature.
         * Tagless placeholders are identified by their opcode, while tagged placeholders are added as general parameters.
         *
         * @param expression The template expression to analyze for placeholders
         * @param signature The mutable list to add discovered parameters to
         */
        @JvmStatic
        private fun addPlaceholdersToSignature(expression: TemplateExpression, signature: MutableList<Parameter>) {
            if (expression.expressionKind == OperationKind.PLACEHOLDER) {
                if (expression.primitiveValue > 0) {
                    // Tagless placeholder - determine the TaglessScalarType from the opcode
                    val opcode = expression.primitiveValue.toInt()
                    val taglessType = TaglessScalarType.entries.find { it.getOpcode() == opcode }
                    signature.add(Parameter(taglessType))
                } else {
                    // Tagged placeholder (with or without default)
                    signature.add(Parameter())
                }
            } else {
                for (childExpression in expression.childValues) {
                    addPlaceholdersToSignature(childExpression, signature)
                }
            }
        }

        /**
         * Writes an array of template expressions to the Ion writer, handling all supported operation kinds.
         * This method recursively processes nested expressions for containers like lists, s-expressions, and structs.
         *
         * @param body The array of template expressions to write
         * @param writer The Ion writer to write the expressions to
         */
        @JvmStatic
        // TODO: Revisit this with performance in mind.
        private fun writeTemplateExpressions(body: Array<TemplateExpression>, writer: IonRawWriter_1_1) {
            for (expr in body) {
                when (expr.expressionKind) {
                    OperationKind.NULL -> writer.writeNull(expr.objectValue as IonType)
                    OperationKind.BOOL -> writer.writeBool(expr.primitiveValue == 1L)
                    OperationKind.INT -> {
                        if (expr.objectValue != null) {
                            writer.writeInt(expr.objectValue as BigInteger)
                        } else {
                            writer.writeInt(expr.primitiveValue)
                        }
                    }
                    OperationKind.FLOAT -> writer.writeFloat(Double.fromBits(expr.primitiveValue))
                    OperationKind.DECIMAL -> writer.writeDecimal(expr.objectValue as BigDecimal)
                    OperationKind.TIMESTAMP -> writer.writeTimestamp(expr.objectValue as Timestamp)
                    OperationKind.STRING -> writer.writeString(expr.objectValue as String)
                    OperationKind.SYMBOL -> (expr.objectValue as String?)?.let(writer::writeSymbol) ?: writer.writeSymbol(0)
                    OperationKind.CLOB -> {
                        val slice = expr.objectValue as ByteSlice
                        writer.writeClob(slice.bytes, slice.startInclusive, slice.length)
                    }
                    OperationKind.BLOB -> {
                        val slice = expr.objectValue as ByteSlice
                        writer.writeBlob(slice.bytes, slice.startInclusive, slice.length)
                    }
                    OperationKind.LIST -> {
                        writer.stepInList(false)
                        writeTemplateExpressions(expr.childValues, writer)
                        writer.stepOut()
                    }
                    OperationKind.SEXP -> {
                        writer.stepInSExp(false)
                        writeTemplateExpressions(expr.childValues, writer)
                        writer.stepOut()
                    }
                    OperationKind.STRUCT -> {
                        writer.stepInStruct(false)
                        writeTemplateExpressions(expr.childValues, writer)
                        writer.stepOut()
                    }
                    OperationKind.FIELD_NAME -> (expr.objectValue as String?)?.let(writer::writeFieldName) ?: writer.writeFieldName(0)
                    OperationKind.ANNOTATIONS -> (expr.objectValue as Array<*>).forEach { (it as String?)?.let(writer::writeAnnotations) ?: writer.writeAnnotations(0) }
                    OperationKind.PLACEHOLDER -> {
                        if (expr.primitiveValue > 0) {
                            writer.writeTaglessPlaceholder(expr.primitiveValue.toInt())
                        } else if (expr.childValues.isEmpty()) {
                            writer.writeTaggedPlaceholder()
                        } else {
                            writer.writeTaggedPlaceholderWithDefault { writeTemplateExpressions(expr.childValues, writer) }
                        }
                    }
                    else -> TODO("Unreachable")
                }
            }
        }
    }
}
