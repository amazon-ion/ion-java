// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.ion_1_1

import com.amazon.ion.Decimal
import com.amazon.ion.IonType
import com.amazon.ion.Macro
import com.amazon.ion.Timestamp
import com.amazon.ion.bytecode.ir.OperationKind
import com.amazon.ion.bytecode.util.ByteSlice
import java.math.BigDecimal
import java.math.BigInteger
import java.util.function.Consumer
import java.util.function.Function

/**
 * A fluent builder for creating [Macro] instances with a type-safe, Java-friendly API.
 *
 * **Example:**
 * ```java
 * Macro macro = MacroBuilder.newBuilder()
 *     .struct(struct -> struct
 *         .fieldName("name")
 *         .string("John")
 *         .fieldName("age")
 *         .placeholder())
 *     .build();
 * ```
 */
class MacroBuilder private constructor() {

    companion object {
        /** Creates a new MacroBuilder instance to start building a macro template. */
        @JvmStatic
        fun newBuilder(): TemplateBody = BuilderImpl()
    }

    interface TemplateBody {
        /** Adds annotations to the next value. */
        fun annotated(vararg annotations: String?): TemplateBody

        /** Creates a null value. */
        fun nullValue(): FinalState

        /** Creates a null value of the specified type. */
        fun nullValue(type: IonType): FinalState

        /** Creates a boolean value. */
        fun boolValue(value: Boolean): FinalState

        /** Creates an integer value from a Long. */
        fun intValue(value: Long): FinalState

        /** Creates an integer value from a BigInteger. */
        fun intValue(value: BigInteger): FinalState

        /** Creates a float value. */
        fun floatValue(value: Double): FinalState

        /** Creates a decimal value from an Ion Decimal. */
        fun decimalValue(value: Decimal): FinalState

        /** Creates a decimal value from a BigDecimal. */
        fun decimalValue(value: BigDecimal): FinalState

        /** Creates a timestamp value. */
        fun timestampValue(value: Timestamp): FinalState

        /** Creates a string value. */
        fun stringValue(value: String): FinalState

        /** Creates a symbol value. */
        fun symbolValue(value: String?): FinalState

        /** Creates a clob value from a byte array. */
        fun clobValue(value: ByteArray): FinalState

        /** Creates a blob value from a byte array. */
        fun blobValue(value: ByteArray): FinalState

        /** Creates a list value with the specified content. */
        fun listValue(content: Consumer<Sequence>): FinalState

        /** Creates an S-expression value with the specified content. */
        fun sexpValue(content: Consumer<Sequence>): FinalState

        /** Creates a struct value with the specified field definitions. */
        fun structValue(content: Function<StructFieldName, StructFieldName>): FinalState
    }

    interface FinalState {
        /** Builds and returns the completed Macro instance. */
        fun build(): Macro
    }

    interface Sequence {
        /** Adds annotations to the next value in the sequence. */
        fun annotated(vararg annotations: String?): Sequence

        /** Adds a null value to the sequence. */
        fun nullValue(): Sequence

        /** Adds a null value of the specified type to the sequence. */
        fun nullValue(type: IonType): Sequence

        /** Adds a boolean value to the sequence. */
        fun boolValue(value: Boolean): Sequence

        /** Adds an integer value from a Long to the sequence. */
        fun intValue(value: Long): Sequence

        /** Adds an integer value from a BigInteger to the sequence. */
        fun intValue(value: BigInteger): Sequence

        /** Adds a float value to the sequence. */
        fun floatValue(value: Double): Sequence

        /** Adds a decimal value from an Ion Decimal to the sequence. */
        fun decimalValue(value: Decimal): Sequence

        /** Adds a decimal value from a BigDecimal to the sequence. */
        fun decimalValue(value: BigDecimal): Sequence

        /** Adds a timestamp value to the sequence. */
        fun timestampValue(value: Timestamp): Sequence

        /** Adds a string value to the sequence. */
        fun stringValue(value: String): Sequence

        /** Adds a symbol value to the sequence. */
        fun symbolValue(value: String?): Sequence

        /** Adds a clob value from a byte array to the sequence. */
        fun clobValue(value: ByteArray): Sequence

        /** Adds a blob value from a byte array to the sequence. */
        fun blobValue(value: ByteArray): Sequence

        /** Adds a placeholder for macro parameter substitution to the sequence. */
        fun placeholder(): Sequence

        /** Adds a tagless placeholder of the specified scalar type to the sequence. */
        fun taglessPlaceholder(type: TaglessScalarType): Sequence

        /** Adds a placeholder with a default value to the sequence. */
        fun placeholderWithDefault(defaultValue: Function<Value, ValueReady>): Sequence

        /** Adds a list value with the specified content to the sequence. */
        fun listValue(content: Consumer<Sequence>): Sequence

        /** Adds an S-expression value with the specified content to the sequence. */
        fun sexpValue(content: Consumer<Sequence>): Sequence

        /** Adds a struct value with the specified field definitions to the sequence. */
        fun structValue(content: Function<StructFieldName, StructFieldName>): Sequence
    }

    interface Value {
        /** Adds annotations to the value. */
        fun annotated(vararg annotations: String?): Value

        /** Creates a null value. */
        fun nullValue(): ValueReady

        /** Creates a null value of the specified type. */
        fun nullValue(type: IonType): ValueReady

        /** Creates a boolean value. */
        fun boolValue(value: Boolean): ValueReady

        /** Creates an integer value from a Long. */
        fun intValue(value: Long): ValueReady

        /** Creates an integer value from a BigInteger. */
        fun intValue(value: BigInteger): ValueReady

        /** Creates a float value. */
        fun floatValue(value: Double): ValueReady

        /** Creates a decimal value from an Ion Decimal. */
        fun decimalValue(value: Decimal): ValueReady

        /** Creates a decimal value from a BigDecimal. */
        fun decimalValue(value: BigDecimal): ValueReady

        /** Creates a timestamp value. */
        fun timestampValue(value: Timestamp): ValueReady

        /** Creates a string value. */
        fun stringValue(value: String): ValueReady

        /** Creates a symbol value. */
        fun symbolValue(value: String?): ValueReady

        /** Creates a clob value from a byte array. */
        fun clobValue(value: ByteArray): ValueReady

        /** Creates a blob value from a byte array. */
        fun blobValue(value: ByteArray): ValueReady

        /** Creates a list value with the specified content. */
        fun listValue(content: Consumer<Sequence>): ValueReady

        /** Creates an S-expression value with the specified content. */
        fun sexpValue(content: Consumer<Sequence>): ValueReady

        /** Creates a struct value with the specified field definitions. */
        fun structValue(content: Function<StructFieldName, StructFieldName>): ValueReady
    }

    interface ValueReady

    interface StructFieldName {
        /** Sets the field name for the next struct field. */
        fun fieldName(name: String?): StructFieldValue
    }

    interface StructFieldValue {
        /** Adds annotations to the struct field value. */
        fun annotated(vararg annotations: String?): StructFieldValue

        /** Sets the struct field value to null. */
        fun nullValue(): StructFieldName

        /** Sets the struct field value to null of the specified type. */
        fun nullValue(type: IonType): StructFieldName

        /** Sets the struct field value to a boolean. */
        fun boolValue(value: Boolean): StructFieldName

        /** Sets the struct field value to an integer from a Long. */
        fun intValue(value: Long): StructFieldName

        /** Sets the struct field value to an integer from a BigInteger. */
        fun intValue(value: BigInteger): StructFieldName

        /** Sets the struct field value to a float. */
        fun floatValue(value: Double): StructFieldName

        /** Sets the struct field value to a decimal from an Ion Decimal. */
        fun decimalValue(value: Decimal): StructFieldName

        /** Sets the struct field value to a decimal from a BigDecimal. */
        fun decimalValue(value: BigDecimal): StructFieldName

        /** Sets the struct field value to a timestamp. */
        fun timestampValue(value: Timestamp): StructFieldName

        /** Sets the struct field value to a string. */
        fun stringValue(value: String): StructFieldName

        /** Sets the struct field value to a symbol. */
        fun symbolValue(value: String?): StructFieldName

        /** Sets the struct field value to a clob from a byte array. */
        fun clobValue(value: ByteArray): StructFieldName

        /** Sets the struct field value to a blob from a byte array. */
        fun blobValue(value: ByteArray): StructFieldName

        /** Sets the struct field value to a placeholder for macro parameter substitution. */
        fun placeholder(): StructFieldName

        /** Sets the struct field value to a tagless placeholder of the specified scalar type. */
        fun taglessPlaceholder(type: TaglessScalarType): StructFieldName

        /** Sets the struct field value to a placeholder with a default value. */
        fun placeholderWithDefault(defaultValue: Function<Value, ValueReady>): StructFieldName

        /** Sets the struct field value to a list with the specified content. */
        fun listValue(content: Consumer<Sequence>): StructFieldName

        /** Sets the struct field value to an S-expression with the specified content. */
        fun sexpValue(content: Consumer<Sequence>): StructFieldName

        /** Sets the struct field value to a struct with the specified field definitions. */
        fun structValue(content: Function<StructFieldName, StructFieldName>): StructFieldName
    }

    /**
     * Implementation of the fluent builder interfaces.
     */
    private class BuilderImpl : Value, ValueReady, Sequence, StructFieldName, StructFieldValue, TemplateBody, FinalState {

        private var expressions: MutableList<TemplateExpression> = ArrayList()

        // Scalars
        override fun nullValue(): BuilderImpl = nullValue(IonType.NULL)
        override fun nullValue(type: IonType): BuilderImpl = apply { objectExpression(OperationKind.NULL, type) }
        override fun boolValue(value: Boolean): BuilderImpl = apply { primitiveExpression(OperationKind.BOOL, if (value) 1L else 0L) }
        override fun intValue(value: Long): BuilderImpl = apply { primitiveExpression(OperationKind.INT, value) }
        override fun intValue(value: BigInteger): BuilderImpl = apply { objectExpression(OperationKind.INT, value) }
        override fun floatValue(value: Double): BuilderImpl = apply { primitiveExpression(OperationKind.FLOAT, java.lang.Double.doubleToRawLongBits(value)) }
        override fun decimalValue(value: Decimal): BuilderImpl = apply { objectExpression(OperationKind.DECIMAL, value) }
        override fun decimalValue(value: BigDecimal): BuilderImpl = decimalValue(Decimal.valueOf(value))
        override fun timestampValue(value: Timestamp): BuilderImpl = apply { objectExpression(OperationKind.TIMESTAMP, value) }
        override fun stringValue(value: String): BuilderImpl = apply { objectExpression(OperationKind.STRING, value) }
        override fun symbolValue(value: String?): BuilderImpl = apply { objectExpression(OperationKind.SYMBOL, value) }
        override fun clobValue(value: ByteArray): BuilderImpl = apply { objectExpression(OperationKind.CLOB, ByteSlice(value, 0, value.size)) }
        override fun blobValue(value: ByteArray): BuilderImpl = apply { objectExpression(OperationKind.BLOB, ByteSlice(value, 0, value.size)) }

        // Containers
        override fun listValue(content: Consumer<Sequence>): BuilderImpl = apply { containerExpression(OperationKind.LIST) { content.accept(this) } }
        override fun sexpValue(content: Consumer<Sequence>): BuilderImpl = apply { containerExpression(OperationKind.SEXP) { content.accept(this) } }
        override fun structValue(content: Function<StructFieldName, StructFieldName>): BuilderImpl = apply { containerExpression(OperationKind.STRUCT) { content.apply(this) } }

        // Placeholders
        override fun placeholder(): BuilderImpl = apply { primitiveExpression(OperationKind.PLACEHOLDER, 0) }
        override fun taglessPlaceholder(type: TaglessScalarType): BuilderImpl = apply { primitiveExpression(OperationKind.PLACEHOLDER, type.getOpcode().toLong()) }
        override fun placeholderWithDefault(defaultValue: Function<Value, ValueReady>): BuilderImpl = apply { containerExpression(OperationKind.PLACEHOLDER) { defaultValue.apply(this) } }

        // Field Name
        override fun fieldName(name: String?): BuilderImpl = apply { objectExpression(OperationKind.FIELD_NAME, name) }

        // Annotations
        override fun annotated(vararg annotations: String?): BuilderImpl = apply { objectExpression(OperationKind.ANNOTATIONS, annotations) }

        override fun build(): Macro = MacroImpl(expressions)

        // ==== Helper Methods ==== //

        private fun primitiveExpression(kind: Int, value: Long) {
            expressions.add(TemplateExpression(kind, primitiveValue = value))
        }

        private fun objectExpression(kind: Int, value: Any?) {
            expressions.add(TemplateExpression(kind, objectValue = value))
        }

        private inline fun containerExpression(kind: Int, content: () -> Unit) {
            val startExpression = TemplateExpression(kind, 0, null, TemplateExpression.EMPTY_EXPRESSION_ARRAY)
            expressions.add(startExpression)
            val start = expressions.size
            content()
            val end = expressions.size
            val childExpressions = expressions.subList(start, end)
            startExpression.childValues = childExpressions.toTypedArray<TemplateExpression>()
            childExpressions.clear()
        }
    }
}
