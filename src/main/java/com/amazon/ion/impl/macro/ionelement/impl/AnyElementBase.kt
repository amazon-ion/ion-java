/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.ion.impl.macro.ionelement.impl

import com.amazon.ion.Decimal
import com.amazon.ion.IonWriter
import com.amazon.ion.Timestamp
import com.amazon.ion.impl.macro.ionelement.api.AnyElement
import com.amazon.ion.impl.macro.ionelement.api.BlobElement
import com.amazon.ion.impl.macro.ionelement.api.BoolElement
import com.amazon.ion.impl.macro.ionelement.api.ByteArrayView
import com.amazon.ion.impl.macro.ionelement.api.ClobElement
import com.amazon.ion.impl.macro.ionelement.api.ContainerElement
import com.amazon.ion.impl.macro.ionelement.api.DecimalElement
import com.amazon.ion.impl.macro.ionelement.api.ElementType
import com.amazon.ion.impl.macro.ionelement.api.ElementType.BLOB
import com.amazon.ion.impl.macro.ionelement.api.ElementType.BOOL
import com.amazon.ion.impl.macro.ionelement.api.ElementType.CLOB
import com.amazon.ion.impl.macro.ionelement.api.ElementType.DECIMAL
import com.amazon.ion.impl.macro.ionelement.api.ElementType.FLOAT
import com.amazon.ion.impl.macro.ionelement.api.ElementType.INT
import com.amazon.ion.impl.macro.ionelement.api.ElementType.LIST
import com.amazon.ion.impl.macro.ionelement.api.ElementType.NULL
import com.amazon.ion.impl.macro.ionelement.api.ElementType.SEXP
import com.amazon.ion.impl.macro.ionelement.api.ElementType.STRING
import com.amazon.ion.impl.macro.ionelement.api.ElementType.STRUCT
import com.amazon.ion.impl.macro.ionelement.api.ElementType.SYMBOL
import com.amazon.ion.impl.macro.ionelement.api.ElementType.TIMESTAMP
import com.amazon.ion.impl.macro.ionelement.api.FloatElement
import com.amazon.ion.impl.macro.ionelement.api.IntElement
import com.amazon.ion.impl.macro.ionelement.api.IntElementSize
import com.amazon.ion.impl.macro.ionelement.api.IonElement
import com.amazon.ion.impl.macro.ionelement.api.ListElement
import com.amazon.ion.impl.macro.ionelement.api.LobElement
import com.amazon.ion.impl.macro.ionelement.api.SeqElement
import com.amazon.ion.impl.macro.ionelement.api.SexpElement
import com.amazon.ion.impl.macro.ionelement.api.StringElement
import com.amazon.ion.impl.macro.ionelement.api.StructElement
import com.amazon.ion.impl.macro.ionelement.api.StructField
import com.amazon.ion.impl.macro.ionelement.api.SymbolElement
import com.amazon.ion.impl.macro.ionelement.api.TextElement
import com.amazon.ion.impl.macro.ionelement.api.TimestampElement
import com.amazon.ion.impl.macro.ionelement.api.constraintError
import com.amazon.ion.system.IonTextWriterBuilder
import java.math.BigInteger

private val TEXT_WRITER_BUILDER = IonTextWriterBuilder.standard()

/**
 * Provides most of the implementation of [AnyElement] to any of the narrowed types of [IonElement].
 *
 * The only function that must be overridden by any derived narrowing implementation of [IonElement] is the non-null
 * value accessor function that corresponds to the narrowed Ion type.
 */
internal abstract class AnyElementBase : AnyElement {

    override fun asAnyElement(): AnyElement = this

    override val isNull: Boolean get() = false
    protected abstract fun writeContentTo(writer: IonWriter)

    override fun writeTo(writer: IonWriter) {
        if (this.annotations.any()) {
            writer.setTypeAnnotations(*this.annotations.toTypedArray())
        }
        this.writeContentTo(writer)
    }

    override fun toString() = StringBuilder().also { buf ->
        TEXT_WRITER_BUILDER.build(buf).use { writeTo(it) }
    }.toString()

    override val integerSize: IntElementSize get() = constraintError(this, "integerSize not valid for this Element")

    private inline fun <reified T : IonElement> requireTypeAndCastOrNull(allowedType: ElementType): T? {
        if (this.type == NULL) {
            return null
        }

        if (this.type != allowedType)
            errIfNotTyped(allowedType)

        return when {
            // this could still be a typed null
            this.isNull -> null
            else -> this as T
        }
    }

    private fun errIfNotTyped(allowedType: ElementType): Nothing {
        constraintError(this, "Expected an element of type $allowedType but found an element of type ${this.type}")
    }

    private fun errIfNotTyped(allowedType: ElementType, allowedType2: ElementType): Nothing {
        constraintError(this, "Expected an element of type $allowedType or $allowedType2 but found an element of type ${this.type}")
    }

    private fun errIfNotTyped(allowedType: ElementType, allowedType2: ElementType, allowedType3: ElementType): Nothing {
        constraintError(this, "Expected an element of type $allowedType, $allowedType2 or $allowedType3 but found an element of type ${this.type}")
    }

    private inline fun <reified T : IonElement> requireTypeAndCastOrNull(allowedType: ElementType, allowedType2: ElementType): T? {
        if (this.type == NULL) {
            return null
        }

        if (this.type != allowedType && this.type != allowedType2)
            errIfNotTyped(allowedType, allowedType2)

        return when {
            // this could still be a typed null
            this.isNull -> null
            else -> this as T
        }
    }

    private inline fun <reified T : IonElement> requireTypeAndCastOrNull(allowedType: ElementType, allowedType2: ElementType, allowedType3: ElementType): T? {
        if (this.type == NULL) {
            return null
        }

        if (this.type != allowedType && this.type != allowedType2 && this.type != allowedType3)
            constraintError(this, "Expected an element of type $allowedType, $allowedType2 or $allowedType3 but found an element of type ${this.type}")

        return when {
            // this could still be a typed null
            this.isNull -> null
            else -> this as T
        }
    }

    private inline fun <reified T : IonElement> requireTypeAndCast(allowedType: ElementType): T {
        requireTypeAndCastOrNull<T>(allowedType) ?: constraintError(this, "Required non-null value of type $allowedType but found a $this")
        return this as T
    }

    private inline fun <reified T : IonElement> requireTypeAndCast(allowedType: ElementType, allowedType2: ElementType): T {
        requireTypeAndCastOrNull<T>(allowedType, allowedType2) ?: constraintError(this, "Required non-null value of type $allowedType or $allowedType2 but found a $this")
        return this as T
    }

    private inline fun <reified T : IonElement> requireTypeAndCast(allowedType: ElementType, allowedType2: ElementType, allowedType3: ElementType): T {
        requireTypeAndCastOrNull<T>(allowedType, allowedType2, allowedType3) ?: constraintError(this, "Required non-null value of type $allowedType, $allowedType2 or $allowedType3 but found a $this")
        return this as T
    }

    // The default as*() functions do not need to be overridden by child classes.
    final override fun asBoolean(): BoolElement = requireTypeAndCast(BOOL)
    final override fun asBooleanOrNull(): BoolElement? = requireTypeAndCastOrNull(BOOL)
    final override fun asInt(): IntElement = requireTypeAndCast(INT)
    final override fun asIntOrNull(): IntElement? = requireTypeAndCastOrNull(INT)
    final override fun asDecimal(): DecimalElement = requireTypeAndCast(DECIMAL)
    final override fun asDecimalOrNull(): DecimalElement? = requireTypeAndCastOrNull(DECIMAL)
    final override fun asFloat(): FloatElement = requireTypeAndCast(FLOAT)
    final override fun asFloatOrNull(): FloatElement? = requireTypeAndCastOrNull(FLOAT)
    final override fun asText(): TextElement = requireTypeAndCast(STRING, SYMBOL)
    final override fun asTextOrNull(): TextElement? = requireTypeAndCastOrNull(STRING, SYMBOL)
    final override fun asString(): StringElement = requireTypeAndCast(STRING)
    final override fun asStringOrNull(): StringElement? = requireTypeAndCastOrNull(STRING)
    final override fun asSymbol(): SymbolElement = requireTypeAndCast(SYMBOL)
    final override fun asSymbolOrNull(): SymbolElement? = requireTypeAndCastOrNull(SYMBOL)
    final override fun asTimestamp(): TimestampElement = requireTypeAndCast(TIMESTAMP)
    final override fun asTimestampOrNull(): TimestampElement? = requireTypeAndCastOrNull(TIMESTAMP)
    final override fun asLob(): LobElement = requireTypeAndCast(BLOB, CLOB)
    final override fun asLobOrNull(): LobElement? = requireTypeAndCastOrNull(BLOB, CLOB)
    final override fun asBlob(): BlobElement = requireTypeAndCast(BLOB)
    final override fun asBlobOrNull(): BlobElement? = requireTypeAndCastOrNull(BLOB)
    final override fun asClob(): ClobElement = requireTypeAndCast(CLOB)
    final override fun asClobOrNull(): ClobElement? = requireTypeAndCastOrNull(CLOB)
    final override fun asContainer(): ContainerElement = requireTypeAndCast(LIST, STRUCT, SEXP)
    final override fun asContainerOrNull(): ContainerElement? = requireTypeAndCastOrNull(LIST, STRUCT, SEXP)
    final override fun asSeq(): SeqElement = requireTypeAndCast(LIST, SEXP)
    final override fun asSeqOrNull(): SeqElement? = requireTypeAndCastOrNull(LIST, SEXP)
    final override fun asList(): ListElement = requireTypeAndCast(LIST)
    final override fun asListOrNull(): ListElement? = requireTypeAndCastOrNull(LIST)
    final override fun asSexp(): SexpElement = requireTypeAndCast(SEXP)
    final override fun asSexpOrNull(): SexpElement? = requireTypeAndCastOrNull(SEXP)
    final override fun asStruct(): StructElement = requireTypeAndCast(STRUCT)
    final override fun asStructOrNull(): StructElement? = requireTypeAndCastOrNull(STRUCT)

    // The type-specific methods in this group must be overridden in the narrow implementations of [IonElement].
    // The default implementations here throw, complaining about an unexpected type.
    override val booleanValue: Boolean get() = errIfNotTyped(BOOL)
    override val longValue: Long get() = errIfNotTyped(INT)
    override val bigIntegerValue: BigInteger get() = errIfNotTyped(INT)
    override val textValue: String get() = errIfNotTyped(STRING, SYMBOL)
    override val stringValue: String get() = errIfNotTyped(STRING)
    override val symbolValue: String get() = errIfNotTyped(SYMBOL)
    override val decimalValue: Decimal get() = errIfNotTyped(DECIMAL)
    override val doubleValue: Double get() = errIfNotTyped(FLOAT)
    override val timestampValue: Timestamp get() = errIfNotTyped(TIMESTAMP)
    override val bytesValue: ByteArrayView get() = errIfNotTyped(BLOB, CLOB)
    override val blobValue: ByteArrayView get() = errIfNotTyped(BLOB)
    override val clobValue: ByteArrayView get() = errIfNotTyped(CLOB)
    override val containerValues: Collection<AnyElement> get() = errIfNotTyped(LIST, SEXP, STRUCT)
    override val seqValues: List<AnyElement> get() = errIfNotTyped(LIST, SEXP)
    override val listValues: List<AnyElement> get() = errIfNotTyped(LIST)
    override val sexpValues: List<AnyElement> get() = errIfNotTyped(SEXP)
    override val structFields: Collection<StructField> get() = errIfNotTyped(STRUCT)

    // Default implementations that perform the type check and wrap the corresponding non-nullable version.
    final override val booleanValueOrNull: Boolean? get() = requireTypeAndCastOrNull<BoolElement>(BOOL)?.booleanValue
    final override val longValueOrNull: Long? get() = requireTypeAndCastOrNull<IntElement>(INT)?.longValue
    final override val bigIntegerValueOrNull: BigInteger? get() = requireTypeAndCastOrNull<IntElement>(INT)?.bigIntegerValue
    final override val textValueOrNull: String? get() = requireTypeAndCastOrNull<TextElement>(STRING, SYMBOL)?.textValue
    final override val stringValueOrNull: String? get() = requireTypeAndCastOrNull<StringElement>(STRING)?.textValue
    final override val symbolValueOrNull: String? get() = requireTypeAndCastOrNull<SymbolElement>(SYMBOL)?.textValue
    final override val decimalValueOrNull: Decimal? get() = requireTypeAndCastOrNull<DecimalElement>(DECIMAL)?.decimalValue
    final override val doubleValueOrNull: Double? get() = requireTypeAndCastOrNull<FloatElement>(FLOAT)?.doubleValue
    final override val timestampValueOrNull: Timestamp? get() = requireTypeAndCastOrNull<TimestampElement>(TIMESTAMP)?.timestampValue
    final override val bytesValueOrNull: ByteArrayView? get() = requireTypeAndCastOrNull<LobElement>(BLOB, CLOB)?.bytesValue
    final override val blobValueOrNull: ByteArrayView? get() = requireTypeAndCastOrNull<BlobElement>(BLOB)?.bytesValue
    final override val clobValueOrNull: ByteArrayView? get() = requireTypeAndCastOrNull<ClobElement>(CLOB)?.bytesValue
    final override val containerValuesOrNull: Collection<AnyElement>? get() = requireTypeAndCastOrNull<ContainerElement>(LIST, SEXP, STRUCT)?.values
    final override val seqValuesOrNull: List<AnyElement>? get() = requireTypeAndCastOrNull<SeqElement>(LIST, SEXP)?.values
    final override val listValuesOrNull: List<AnyElement>? get() = requireTypeAndCastOrNull<ListElement>(LIST)?.values
    final override val sexpValuesOrNull: List<AnyElement>? get() = requireTypeAndCastOrNull<SexpElement>(SEXP)?.values
    final override val structFieldsOrNull: Collection<StructField>? get() = requireTypeAndCastOrNull<StructElement>(STRUCT)?.fields
}
