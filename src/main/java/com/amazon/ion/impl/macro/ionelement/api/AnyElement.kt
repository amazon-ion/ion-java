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

package com.amazon.ion.impl.macro.ionelement.api

import com.amazon.ion.Decimal
import com.amazon.ion.Timestamp
import com.amazon.ion.impl.macro.ionelement.api.ElementType.BLOB
import com.amazon.ion.impl.macro.ionelement.api.ElementType.BOOL
import com.amazon.ion.impl.macro.ionelement.api.ElementType.CLOB
import com.amazon.ion.impl.macro.ionelement.api.ElementType.DECIMAL
import com.amazon.ion.impl.macro.ionelement.api.ElementType.INT
import com.amazon.ion.impl.macro.ionelement.api.ElementType.LIST
import com.amazon.ion.impl.macro.ionelement.api.ElementType.NULL
import com.amazon.ion.impl.macro.ionelement.api.ElementType.SEXP
import com.amazon.ion.impl.macro.ionelement.api.ElementType.STRING
import com.amazon.ion.impl.macro.ionelement.api.ElementType.STRUCT
import com.amazon.ion.impl.macro.ionelement.api.ElementType.SYMBOL
import com.amazon.ion.impl.macro.ionelement.api.ElementType.TIMESTAMP
import java.math.BigInteger

/**
 * Represents an Ion element whose type is not known at compile-time.
 *
 * [IonElement] is returned by all of the [IonElementLoader] functions and is the data type for children of [ListElement],
 * [SexpElement] and [StructElement]. If the type of an Ion element is not known at compile-time, the type may be easily
 * asserted at runtime and a narrower [IonElement] interface may be obtained instead.
 *
 * All implementations of [AnyElement] are required to also implement the correct *Element interface for the actual type
 * of the element (e.g. an [AnyElement] that is an Ion string must also implement [StringElement]).
 *
 * Two categories of methods are present on this type:
 *
 * - Value accessors (the `*Value` and `*ValueOrNull` properties)
 * - Narrowing functions (`as*()` and `as*OrNull()`
 *
 * Use of the accessor functions allow for concise expression of two purposes simultaneously:
 *
 * - An expectation of the Ion type of the given element
 * - An expectation of the nullability of the given element
 *
 * If either of these expectations is violated an [IonElementConstraintException] is thrown.  If the given element
 * has an [IonLocation] in its metadata, it is included with the [IonElementConstraintException] which can be used to
 * generate an error message that points to the specific location of the failure within the Ion-text document
 * (i.e. line & column) or within the Ion-binary document (i.e. byte offset).
 *
 * Value Accessor Examples:
 *
 * ```
 * val e: AnyElement = loadSingleElement("1")
 * val value: Long = e.longValue
 * // e.longValue throws if e is null or not an INT
 * ```
 *
 * ```
 * val e: AnyElement = loadSingleElement("[1, 2]")
 * val values: List<Long> = e.listValues.map { it.longValue }
 * // e.listValues.map { it.longValue } throws if:
 * //  - e is null or not a list
 * //  - any child element in e is null or not an int
 * ```
 *
 * ```
 * val e: AnyElement = loadSingleElement("some_symbol")
 * val value: String = e.textValue
 * // throws if (e is null) or (not a STRING or SYMBOL).
 * ```
 *
 * Narrowing Function Examples:
 *
 * ```
 * val e: AnyElement = loadSingleElement("1")
 * val n: IntElement = e.asInt()
 * // e.asInt() throws if e is null or not an INT
 * ```
 *
 * ```
 * val e: AnyElement = loadSingleElement("[1, 2]")
 * val l: ListElement = e.asList()
 * // e.asList() throws if e is null or not a list
 *
 * val values: List<IntElement> = l.values.map { it.asInt() }
 * // l.values.map { it.asInt() } throws if: any child element in l is null or not an int
 * ```
 *
 * ```
 * val e: AnyElement = loadSingleElement("some_symbol")
 * val t: String = e.textValue
 * // throws if (e is null) or (not a STRING or SYMBOL).
 * ```
 *
 * #### Deciding which accessor function to use
 *
 * **Note:  for the sake of brevity, the following section omits the nullable narrowing functions (`as*OrNull`) and
 * nullable value accessors (`*ValueOrNull` and `*ValuesOrNull`).  These should be used whenever an Ion-null value is
 * allowed.
 *
 * The table below shows which accessor functions can be used for each [ElementType].
 *
 * | [ElementType]    | Value Accessors                                 | Narrowing Functions                    |
 * |------------------------------|------------------------------------------------------------------------------|
 * | [NULL]           | (any with `OrNull` suffix)                      | (any with `OrNull` suffix)             |
 * | [BOOL]           | [booleanValue]                                  | [asBoolean]                            |
 * | [INT]            | [longValue], [bigIntegerValue]                  | [asInt]                                |
 * | [STRING]         | [textValue], [stringValue]                      | [asText], [asString]                   |
 * | [SYMBOL]         | [textValue], [symbolValue]                      | [asText], [asSymbol]                   |
 * | [DECIMAL]        | [decimalValue]                                  | [asDecimal]                            |
 * | [TIMESTAMP]      | [timestampValue]                                | [asTimestamp]                          |
 * | [CLOB]           | [bytesValue], [clobValue]                       | [asLob], [asClob]                      |
 * | [BLOB]           | [bytesValue], [blobValue]                       | [asLob], [asBlob]                      |
 * | [LIST]           | [containerValues], [seqValues], [listValues]    | [asContainer], [asSeq], [asList]       |
 * | [SEXP]           | [containerValues], [seqValues], [sexpValues]    | [asContainer], [asSeq], [asSexp]       |
 * | [STRUCT]         | [containerValues], [structFields]               | [asContainer], [asStruct]              |
 *
 * Notes:
 * - The value returned from [containerValues] when the [type] is [STRUCT] is the values within the struct without
 * their field names.
 *
 * #### Equality
 *
 * Any accessor function returning [Collection<AnyElement>] or [List<AnyElement]] uses the definition of equality for
 * the [Object.equals] and [Object.hashCode] functions that is defined by [List<T>].  This is different from equality
 * as defined by the Ion specification.
 *
 * For example:
 *
 * ```
 * val list: AnyElement = loadSingleElement("[1, 2, 3]").asAnyElement()
 * val sexp: AnyElement = loadSingleElement("(1 2 3)").asAnyElement()
 *
 * // The following is `true` because equality is defined by `List<T>`.
 * sexp.values.equals(list.sexp.values)
 *
 * // The following is false because equality is defined by the Ion specification which specifies that lists and
 * // s-expressions are not equivalent.
 * sexp.equals(list)
 * ```
 *
 * @see [IonElement]
 */
public interface AnyElement : IonElement {

    /**
     * Attempts to narrow this element to a [BoolElement].
     * If this element is not an Ion `bool`, or if it is `null.bool`, throws an [IonElementConstraintException].
     */
    public fun asBoolean(): BoolElement

    /**
     * Attempts to narrow this element to a [BoolElement] or `null`.
     * If this element is not an Ion `bool` or `null.null`, throws an [IonElementConstraintException].
     */
    public fun asBooleanOrNull(): BoolElement?

    /**
     * Attempts to narrow this element to a [IntElement].
     * If this element is not an Ion `int`, or if it is `null.int`, throws an [IonElementConstraintException].
     */
    public fun asInt(): IntElement

    /**
     * Attempts to narrow this element to a [IntElement] or `null`.
     * If this element is not an Ion `int` or `null.null`, throws an [IonElementConstraintException].
     */
    public fun asIntOrNull(): IntElement?

    /**
     * Attempts to narrow this element to a [DecimalElement].
     * If this element is not an Ion `decimal`, or if it is `null.decimal`, throws an [IonElementConstraintException].
     */
    public fun asDecimal(): DecimalElement

    /**
     * Attempts to narrow this element to a [DecimalElement] or `null`.
     * If this element is not an Ion `decimal` or `null.null`, throws an [IonElementConstraintException].
     */
    public fun asDecimalOrNull(): DecimalElement?

    /**
     * Attempts to narrow this element to a [FloatElement].
     * If this element is not an Ion `float`, or if it is `null.float`, throws an [IonElementConstraintException].
     */
    public fun asFloat(): FloatElement

    /**
     * Attempts to narrow this element to a [FloatElement] or `null`.
     * If this element is not an Ion `float` or `null.null`, throws an [IonElementConstraintException].
     */
    public fun asFloatOrNull(): FloatElement?

    /**
     * Attempts to narrow this element to a [TextElement].
     * If this element is not an Ion `symbol` or `string`, or if it is `null.symbol` or `null.string`, throws an
     * [IonElementConstraintException].
     */
    public fun asText(): TextElement

    /**
     * Attempts to narrow this element to a [TextElement] or `null`.
     * If this element is not an Ion `symbol`, Ion `string` or `null.null`, throws an [IonElementConstraintException].
     */
    public fun asTextOrNull(): TextElement?

    /**
     * Attempts to narrow this element to a [StringElement].
     * If this element is not an Ion `string`, or if it is `null.string`, throws an [IonElementConstraintException].
     */
    public fun asString(): StringElement

    /**
     * Attempts to narrow this element to a [StringElement] or `null`.
     * If this element is not an Ion `string` or `null.null`, throws an [IonElementConstraintException].
     */
    public fun asStringOrNull(): StringElement?

    /**
     * Attempts to narrow this element to a [SymbolElement].
     * If this element is not an Ion `symbol`, or if it is `null.symbol`, throws an [IonElementConstraintException].
     */
    public fun asSymbol(): SymbolElement

    /**
     * Attempts to narrow this element to a [SymbolElement] or `null`.
     * If this element is not an Ion `symbol` or `null.null`, throws an [IonElementConstraintException].
     */
    public fun asSymbolOrNull(): SymbolElement?

    /**
     * Attempts to narrow this element to a [TimestampElement].
     * If this element is not an Ion `timestamp`, or if it is `null.timestamp`, throws an [IonElementConstraintException].
     */
    public fun asTimestamp(): TimestampElement

    /**
     * Attempts to narrow this element to a [TimestampElement] or `null`.
     * If this element is not an Ion `timestamp` or `null.null`, throws an [IonElementConstraintException].
     */
    public fun asTimestampOrNull(): TimestampElement?

    /**
     * Attempts to narrow this element to a [LobElement].
     * If this element is not an Ion `blob` or `clob`, or if it is `null.blob` or `null.clob`, throws an
     * [IonElementConstraintException].
     */
    public fun asLob(): LobElement

    /**
     * Attempts to narrow this element to a [LobElement] or `null`.
     * If this element is not an Ion `blob`, Ion `clob`, or `null.null`, throws an [IonElementConstraintException].
     */
    public fun asLobOrNull(): LobElement?

    /**
     * Attempts to narrow this element to a [BlobElement].
     * If this element is not an Ion `blob`, or if it is `null.blob`, throws an [IonElementConstraintException].
     */
    public fun asBlob(): BlobElement

    /**
     * Attempts to narrow this element to a [BlobElement] or `null`.
     * If this element is not an Ion `blob` or `null.null`, throws an [IonElementConstraintException].
     */
    public fun asBlobOrNull(): BlobElement?

    /**
     * Attempts to narrow this element to a [ClobElement].
     * If this element is not an Ion `clob`, or if it is `null.clob`, throws an [IonElementConstraintException].
     */
    public fun asClob(): ClobElement

    /**
     * Attempts to narrow this element to a [ClobElement] or `null`.
     * If this element is not an Ion `clob` or `null.null`, throws an [IonElementConstraintException].
     */
    public fun asClobOrNull(): ClobElement?

    /**
     * Attempts to narrow this element to a [ContainerElement].
     * If this element is not an Ion `list`, `sexp`, or `struct, or if it is any Ion `null`, throws an
     * [IonElementConstraintException].
     */
    public fun asContainer(): ContainerElement

    /**
     * Attempts to narrow this element to a [ContainerElement] or `null`.
     * If this element is not an Ion `list`, `sexp`, `struct`, or `null.null`, throws an [IonElementConstraintException].
     */
    public fun asContainerOrNull(): ContainerElement?

    /**
     * Attempts to narrow this element to a [SeqElement].
     * If this element is not an Ion `list` or `sexp`, or if it is any Ion `null`, throws an
     * [IonElementConstraintException].
     */
    public fun asSeq(): SeqElement

    /**
     * Attempts to narrow this element to a [SeqElement] or `null`.
     * If this element is not an Ion `list`, `sexp`, or `null.null`, throws an [IonElementConstraintException].
     */
    public fun asSeqOrNull(): SeqElement?

    /**
     * Attempts to narrow this element to a [ListElement].
     * If this element is not an Ion `list`, or if it is `null.list`, throws an [IonElementConstraintException].
     */
    public fun asList(): ListElement

    /**
     * Attempts to narrow this element to a [ListElement] or `null`.
     * If this element is not an Ion `list` or `null.null`, throws an [IonElementConstraintException].
     */
    public fun asListOrNull(): ListElement?

    /**
     * Attempts to narrow this element to a [SexpElement].
     * If this element is not an Ion `sexp`, or if it is `null.sexp`, throws an [IonElementConstraintException].
     */
    public fun asSexp(): SexpElement

    /**
     * Attempts to narrow this element to a [SexpElement] or `null`.
     * If this element is not an Ion `sexp` or `null.null`, throws an [IonElementConstraintException].
     */
    public fun asSexpOrNull(): SexpElement?

    /**
     * Attempts to narrow this element to a [StructElement].
     * If this element is not an Ion `struct`, or if it is `null.struct`, throws an [IonElementConstraintException].
     */
    public fun asStruct(): StructElement

    /**
     * Attempts to narrow this element to a [StructElement] or `null`.
     * If this element is not an Ion `struct` or `null.null`, throws an [IonElementConstraintException].
     */
    public fun asStructOrNull(): StructElement?

    /**
     * If this is an Ion integer, returns its [IntElementSize] otherwise, throws [IonElementConstraintException].
     */
    public val integerSize: IntElementSize

    /**
     * The value of the element, assuming that the element is a non-null Ion `bool`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val booleanValue: Boolean

    /**
     * The value of the element, assuming that the element is an Ion `bool` or `null.null`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val booleanValueOrNull: Boolean?

    /**
     * The value of the element, assuming that the element is a non-null Ion `int`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     * Also throws [IonElementConstraintException] if the value is outside the range of a 64-bit signed integer.
     */
    public val longValue: Long

    /**
     * The value of the element, assuming that the element is an Ion `long` or `null.null`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     * Also throws [IonElementConstraintException] if the value is outside the range of a 64-bit signed integer.
     */
    public val longValueOrNull: Long?

    /**
     * The value of the element, assuming that the element is a non-null Ion `int`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val bigIntegerValue: BigInteger

    /**
     * The value of the element, assuming that the element is an Ion `int` or `null.null`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val bigIntegerValueOrNull: BigInteger?

    /**
     * The value of the element, assuming that the element is a non-null Ion `string` or `symbol`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val textValue: String

    /**
     * The value of the element, assuming that the element is an Ion `string` or `symbol` or `null.null`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val textValueOrNull: String?

    /**
     * The value of the element, assuming that the element is a non-null Ion `string`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val stringValue: String

    /**
     * The value of the element, assuming that the element is an Ion `string` or `null.null`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val stringValueOrNull: String?

    /**
     * The value of the element, assuming that the element is a non-null Ion `symbol`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val symbolValue: String

    /**
     * The value of the element, assuming that the element is an Ion `symbol` or `null.null`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val symbolValueOrNull: String?

    /**
     * The value of the element, assuming that the element is a non-null Ion `decimal`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val decimalValue: Decimal

    /**
     * The value of the element, assuming that the element is an Ion `decimal` or `null.null`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val decimalValueOrNull: Decimal?

    /**
     * The value of the element, assuming that the element is a non-null Ion `float`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val doubleValue: Double

    /**
     * The value of the element, assuming that the element is an Ion `float` or `null.null`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val doubleValueOrNull: Double?

    /**
     * The value of the element, assuming that the element is a non-null Ion `timestamp`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val timestampValue: Timestamp

    /**
     * The value of the element, assuming that the element is an Ion `timestamp` or `null.null`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val timestampValueOrNull: Timestamp?

    /**
     * The value of the element, assuming that the element is a non-null Ion `blob` or `clob`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val bytesValue: ByteArrayView

    /**
     * The value of the element, assuming that the element is an Ion `blob` or `clob` or `null.null`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val bytesValueOrNull: ByteArrayView?

    /**
     * The value of the element, assuming that the element is a non-null Ion `blob`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val blobValue: ByteArrayView

    /**
     * The value of the element, assuming that the element is an Ion `blob` or `null.null`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val blobValueOrNull: ByteArrayView?

    /**
     * The value of the element, assuming that the element is a non-null Ion `clob`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val clobValue: ByteArrayView

    /**
     * The value of the element, assuming that the element is an Ion `clob` or `null.null`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val clobValueOrNull: ByteArrayView?

    /**
     * The Ion elements contained in this element, assuming that it is a non-null Ion `list`, `sexp` or `struct`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val containerValues: Collection<AnyElement>

    /**
     * The Ion elements contained in this element, assuming that it is an Ion `list`, `sexp` or `struct` or `null.null`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val containerValuesOrNull: Collection<AnyElement>?

    /**
     * The Ion elements contained in this element, assuming that it is a non-null Ion `list` or `sexp`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val seqValues: List<AnyElement>

    /**
     * The Ion elements contained in this element, assuming that it is an Ion `list` or `sexp` or `null.null`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val seqValuesOrNull: List<AnyElement>?

    /**
     * The Ion elements contained in this element, assuming that it is a non-null Ion `list`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val listValues: List<AnyElement>

    /**
     * The Ion elements contained in this element, assuming that it is an Ion `list` or `null.null`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val listValuesOrNull: List<AnyElement>?

    /**
     * The Ion elements contained in this element, assuming that it is a non-null Ion `sexp`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val sexpValues: List<AnyElement>

    /**
     * The Ion elements contained in this element, assuming that it is an Ion `sexp` or `null.null`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val sexpValuesOrNull: List<AnyElement>?

    /**
     * The fields contained in this element, assuming that it is a non-null Ion `struct`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val structFields: Collection<StructField>

    /**
     * The fields contained in this element, assuming that it is an Ion `struct` or `null.null`.
     * Attempting to access this property on an element that violates the type assumption will result in an  [IonElementConstraintException].
     */
    public val structFieldsOrNull: Collection<StructField>?

    override fun copy(annotations: List<String>, metas: MetaContainer): AnyElement
    override fun withAnnotations(vararg additionalAnnotations: String): AnyElement
    override fun withAnnotations(additionalAnnotations: Iterable<String>): AnyElement
    override fun withoutAnnotations(): AnyElement
    override fun withMetas(additionalMetas: MetaContainer): AnyElement
    override fun withMeta(key: String, value: Any): AnyElement
    override fun withoutMetas(): AnyElement
}
