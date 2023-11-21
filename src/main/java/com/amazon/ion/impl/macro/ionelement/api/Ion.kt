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

@file: JvmName("Ion")
package com.amazon.ion.impl.macro.ionelement.api

import com.amazon.ion.Decimal
import com.amazon.ion.Timestamp
import com.amazon.ion.impl.macro.ionelement.impl.BigIntIntElementImpl
import com.amazon.ion.impl.macro.ionelement.impl.BlobElementImpl
import com.amazon.ion.impl.macro.ionelement.impl.BoolElementImpl
import com.amazon.ion.impl.macro.ionelement.impl.ClobElementImpl
import com.amazon.ion.impl.macro.ionelement.impl.DecimalElementImpl
import com.amazon.ion.impl.macro.ionelement.impl.FloatElementImpl
import com.amazon.ion.impl.macro.ionelement.impl.ListElementImpl
import com.amazon.ion.impl.macro.ionelement.impl.LongIntElementImpl
import com.amazon.ion.impl.macro.ionelement.impl.NullElementImpl
import com.amazon.ion.impl.macro.ionelement.impl.SexpElementImpl
import com.amazon.ion.impl.macro.ionelement.impl.StringElementImpl
import com.amazon.ion.impl.macro.ionelement.impl.StructElementImpl
import com.amazon.ion.impl.macro.ionelement.impl.StructFieldImpl
import com.amazon.ion.impl.macro.ionelement.impl.SymbolElementImpl
import com.amazon.ion.impl.macro.ionelement.impl.TimestampElementImpl
import java.math.BigInteger
import kotlinx.collections.immutable.PersistentList
import kotlinx.collections.immutable.toPersistentList
import kotlinx.collections.immutable.toPersistentMap

internal typealias Annotations = List<String>

/*

# Notes

## vararg and collection constructors

The vararg element collection constructors Kotlin API are not fully idiomatic from the perspective of Java, because
Java requires variadic parameters to be the last parameter, but kotlin does not.  When a vararg parameter is defined
in Kotlin that is not in the last one, it appears to Java to be simply an array.  Unfortunately, `ionListOf`,
`ionSexpOf` and `ionStructOf` constructors all accept a vararg parameter as the first argument and this cannot change
without breaking many clients.

This each of these functions has an overload that from Java appears to accept either an IonElement[] or an
Iterable<IonElement>.

## @JvmOverloads

@JvmOverloads is a very useful tool for Java compatibility that synthesizes overloads which specify the default values
of your parameters when left unspecified by calling code, however, this do not generate overloads for all possible
combinations of values.

For example, the following definition:

```
@JvmOverloads
public fun ionInt(l: Long, annotations: Annotations = emptyList(), metas: MetaContainer = emptyMetaContainer()): IntElement = ...
```

Synthesizes the following overloads:

```
// Java syntax
IntElement ionInt(Long l) { ... }
IntElement ionInt(Long l, Annotations annotations) { ... }
IntElement ionInt(Long l, Annotations annotations, MetaContainer metas) { ... }
```

Missing from this is an overload that accepts only the the `l` and `metas` parameters, which has to be added
manually:

```
IntElement ionInt(Long l, MetaContainer metas) { ... }`
```

Below, we use a combination of @JvmOverloads and the manually implemented overloads for each Ion data type.
*/

/**
 * Creates an [IonElement] that represents an Ion `null.null` or a typed `null` with the specified metas
 * and annotations. */
@JvmOverloads
public fun ionNull(
    elementType: ElementType = ElementType.NULL,
    annotations: Annotations = emptyList(),
    metas: MetaContainer = emptyMetaContainer()
): IonElement =
    ALL_NULLS.getValue(elementType).let {
        when {
            annotations.any() -> it.withAnnotations(annotations)
            else -> it
        }
    }.let {
        when {
            metas.any() -> it.withMetas(metas)
            else -> it
        }
    }

/**
 * Creates an [IonElement] that represents an Ion `null.null` or a typed `null` with the specified metas
 * and annotations. */
public fun ionNull(
    elementType: ElementType = ElementType.NULL,
    metas: MetaContainer
): IonElement = ionNull(elementType, emptyList(), metas)

/** Creates a [StringElement] that represents an Ion `symbol`. */
@JvmOverloads
public fun ionString(
    s: String,
    annotations: Annotations = emptyList(),
    metas: MetaContainer = emptyMetaContainer()
): StringElement = StringElementImpl(
    value = s,
    annotations = annotations.toPersistentList(),
    metas = metas.toPersistentMap()
)
/** Creates a [StringElement] that represents an Ion `symbol`. */
public fun ionString(
    s: String,
    metas: MetaContainer
): StringElement = ionString(s, emptyList(), metas)

/** Creates a [SymbolElement] that represents an Ion `symbol`. */
@JvmOverloads
public fun ionSymbol(
    s: String,
    annotations: Annotations = emptyList(),
    metas: MetaContainer = emptyMetaContainer()
): SymbolElement = SymbolElementImpl(
    value = s,
    annotations = annotations.toPersistentList(),
    metas = metas.toPersistentMap()
)

/** Creates a [SymbolElement] that represents an Ion `symbol`. */
public fun ionSymbol(
    s: String,
    metas: MetaContainer
): SymbolElement = ionSymbol(s, emptyList(), metas)

/** Creates a [TimestampElement] that represents an Ion `timestamp`. */
@JvmOverloads
public fun ionTimestamp(
    s: String,
    annotations: Annotations = emptyList(),
    metas: MetaContainer = emptyMetaContainer()
): TimestampElement = TimestampElementImpl(
    timestampValue = Timestamp.valueOf(s),
    annotations = annotations.toPersistentList(),
    metas = metas.toPersistentMap()
)

/** Creates a [TimestampElement] that represents an Ion `timestamp`. */
public fun ionTimestamp(
    s: String,
    metas: MetaContainer
): TimestampElement = ionTimestamp(s, emptyList(), metas)

/** Creates a [TimestampElement] that represents an Ion `timestamp`. */
@JvmOverloads
public fun ionTimestamp(
    timestamp: Timestamp,
    annotations: Annotations = emptyList(),
    metas: MetaContainer = emptyMetaContainer()
): TimestampElement = TimestampElementImpl(
    timestampValue = timestamp,
    annotations = annotations.toPersistentList(),
    metas = metas.toPersistentMap()
)

/** Creates a [TimestampElement] that represents an Ion `timestamp`. */
public fun ionTimestamp(
    timestamp: Timestamp,
    metas: MetaContainer
): TimestampElement = ionTimestamp(timestamp, emptyList(), metas)

/** Creates an [IntElement] that represents an Ion `int`. */
@JvmOverloads
public fun ionInt(
    l: Long,
    annotations: Annotations = emptyList(),
    metas: MetaContainer = emptyMetaContainer()
): IntElement =
    LongIntElementImpl(
        longValue = l,
        annotations = annotations.toPersistentList(),
        metas = metas.toPersistentMap()
    )

/** Creates an [IntElement] that represents an Ion `int`. */
public fun ionInt(
    l: Long,
    metas: MetaContainer
): IntElement = ionInt(l, emptyList(), metas)

/** Creates an [IntElement] that represents an Ion `BitInteger`. */
@JvmOverloads
public fun ionInt(
    bigInt: BigInteger,
    annotations: Annotations = emptyList(),
    metas: MetaContainer = emptyMetaContainer()
): IntElement = BigIntIntElementImpl(
    bigIntegerValue = bigInt,
    annotations = annotations.toPersistentList(),
    metas = metas.toPersistentMap()
)

/** Creates an [IntElement] that represents an Ion `BitInteger`. */
public fun ionInt(
    bigInt: BigInteger,
    metas: MetaContainer
): IntElement = ionInt(bigInt, emptyList(), metas)

/** Creates a [BoolElement] that represents an Ion `bool`. */
@JvmOverloads
public fun ionBool(
    b: Boolean,
    annotations: Annotations = emptyList(),
    metas: MetaContainer = emptyMetaContainer()
): BoolElement = BoolElementImpl(
    booleanValue = b,
    annotations = annotations.toPersistentList(),
    metas = metas.toPersistentMap()
)

/** Creates a [BoolElement] that represents an Ion `bool`. */
public fun ionBool(
    b: Boolean,
    metas: MetaContainer
): BoolElement = ionBool(b, emptyList(), metas)

/** Creates a [FloatElement] that represents an Ion `float`. */
@JvmOverloads
public fun ionFloat(
    d: Double,
    annotations: Annotations = emptyList(),
    metas: MetaContainer = emptyMetaContainer()
): FloatElement = FloatElementImpl(
    doubleValue = d,
    annotations = annotations.toPersistentList(),
    metas = metas.toPersistentMap()
)

/** Creates a [FloatElement] that represents an Ion `float`. */
public fun ionFloat(
    d: Double,
    metas: MetaContainer
): FloatElement = ionFloat(d, emptyList(), metas)

/** Creates a [DecimalElement] that represents an Ion `decimall`. */
@JvmOverloads
public fun ionDecimal(
    bigDecimal: Decimal,
    annotations: Annotations = emptyList(),
    metas: MetaContainer = emptyMetaContainer()
): DecimalElement = DecimalElementImpl(
    decimalValue = bigDecimal,
    annotations = annotations.toPersistentList(),
    metas = metas.toPersistentMap()
)

/** Creates a [DecimalElement] that represents an Ion `decimall`. */
public fun ionDecimal(
    bigDecimal: Decimal,
    metas: MetaContainer
): DecimalElement = ionDecimal(bigDecimal, emptyList(), metas)

/**
 * Creates a [BlobElement] that represents an Ion `blob`.
 *
 * Note that the [ByteArray] is cloned so immutability can be enforced.
 */
@JvmOverloads
public fun ionBlob(
    bytes: ByteArray,
    annotations: Annotations = emptyList(),
    metas: MetaContainer = emptyMetaContainer()
): BlobElement = BlobElementImpl(
    bytes = bytes.clone(),
    annotations = annotations.toPersistentList(),
    metas = metas.toPersistentMap()
)

/**
 * Creates a [BlobElement] that represents an Ion `blob`.
 *
 * Note that the [ByteArray] is cloned so immutability can be enforced.
 */
public fun ionBlob(
    bytes: ByteArray,
    metas: MetaContainer
): BlobElement = ionBlob(bytes, emptyList(), metas)

/** Returns the empty [BlobElement] singleton. */
public fun emptyBlob(): BlobElement = EMPTY_BLOB

/**
 * Creates a [ClobElement] that represents an Ion `clob`.
 *
 * Note that the [ByteArray] is cloned so immutability can be enforced.
 */
@JvmOverloads
public fun ionClob(
    bytes: ByteArray,
    annotations: Annotations = emptyList(),
    metas: MetaContainer = emptyMetaContainer()
): ClobElement = ClobElementImpl(
    bytes = bytes.clone(),
    annotations = annotations.toPersistentList(),
    metas = metas.toPersistentMap()
)
/**
 * Creates a [ClobElement] that represents an Ion `clob`.
 *
 * Note that the [ByteArray] is cloned so immutability can be enforced.
 */
public fun ionClob(
    bytes: ByteArray,
    metas: MetaContainer = emptyMetaContainer()
): ClobElement = ionClob(bytes, emptyList(), metas)

/** Returns the empty [ClobElement] singleton. */
public fun emptyClob(): ClobElement = EMPTY_CLOB

/** Creates a [ListElement] that represents an Ion `list`. */
@JvmOverloads
public fun ionListOf(
    iterable: Iterable<IonElement>,
    annotations: Annotations = emptyList(),
    metas: MetaContainer = emptyMetaContainer()
): ListElement =
    ListElementImpl(
        values = iterable.map { it.asAnyElement() }.toPersistentList(),
        annotations = annotations.toPersistentList(),
        metas = metas.toPersistentMap()
    )

/** Creates a [ListElement] that represents an Ion `list`. */
public fun ionListOf(
    iterable: Iterable<IonElement>,
    metas: MetaContainer
): ListElement = ionListOf(iterable, emptyList(), metas)

/**
 * Creates a [ListElement] that represents an Ion `list`.
 *
 * If calling from Java, please use one of the overloads which accepts the child elements in an instance of
 * `Iterable<IonElement>`.
 */
public fun ionListOf(
    vararg elements: IonElement,
    annotations: Annotations = emptyList(),
    metas: MetaContainer = emptyMetaContainer()
): ListElement =
    ionListOf(
        iterable = elements.asIterable(),
        annotations = annotations,
        metas = metas
    )

/** Creates a [ListElement] that represents an Ion `list`. */
public fun ionListOf(
    vararg elements: IonElement
): ListElement =
    ionListOf(
        iterable = elements.asIterable(),
        annotations = emptyList(),
        metas = emptyMetaContainer()
    )

/** Returns a [ListElement] representing an empty Ion `list`. */
public fun emptyIonList(): ListElement = EMPTY_LIST

/** Creates an [SexpElement] that represents an Ion `sexp`. */
@JvmOverloads
public fun ionSexpOf(
    iterable: Iterable<IonElement>,
    annotations: Annotations = emptyList(),
    metas: MetaContainer = emptyMetaContainer()
): SexpElement =
    SexpElementImpl(
        values = iterable.map { it.asAnyElement() }.toPersistentList(),
        annotations = annotations.toPersistentList(),
        metas = metas.toPersistentMap()
    )

/** Creates an [SexpElement] that represents an Ion `sexp`. */
public fun ionSexpOf(
    iterable: Iterable<IonElement>,
    metas: MetaContainer
): SexpElement = ionSexpOf(iterable, emptyList(), metas)

/** Creates a [SexpElement] that represents an Ion `sexp`. */
@JvmOverloads
public fun ionSexpOf(
    vararg elements: IonElement,
    annotations: Annotations = emptyList(),
    metas: MetaContainer = emptyMetaContainer()
): SexpElement =
    ionSexpOf(
        iterable = elements.asIterable(),
        annotations = annotations,
        metas = metas
    )

/** Returns a [SexpElement] representing an empty Ion `sexp`. */
public fun emptyIonSexp(): SexpElement = EMPTY_SEXP

/** Returns a [StructElement] representing an empty Ion `struct`. */
public fun emptyIonStruct(): StructElement = EMPTY_STRUCT

/** Creates a [StructField] . */
public fun field(key: String, value: IonElement): StructField =
    StructFieldImpl(name = key, value = value.asAnyElement())

/** Creates a [StructElement] that represents an Ion `struct` with the specified fields. */
@JvmOverloads
public fun ionStructOf(
    fields: Iterable<StructField>,
    annotations: Annotations = emptyList(),
    metas: MetaContainer = emptyMetaContainer()
): StructElement =
    StructElementImpl(
        allFields = fields.toPersistentList(),
        annotations = annotations.toPersistentList(),
        metas = metas.toPersistentMap()
    )

/** Creates a [StructElement] that represents an Ion `struct` with the specified fields. */
public fun ionStructOf(
    fields: Iterable<StructField>,
    metas: MetaContainer
): StructElement = ionStructOf(fields, emptyList(), metas)

/** Creates a [StructElement] that represents an Ion `struct` with the specified fields. */
@JvmOverloads
public fun ionStructOf(
    vararg fields: StructField,
    annotations: Annotations = emptyList(),
    metas: MetaContainer = emptyMetaContainer()
): StructElement =
    ionStructOf(
        fields = fields.asIterable(),
        annotations = annotations.toPersistentList(),
        metas = metas
    )

/** Creates an [AnyElement] that represents an Ion `struct` with the specified fields. */
@JvmOverloads
public fun ionStructOf(
    vararg fields: Pair<String, IonElement>,
    annotations: Annotations = emptyList(),
    metas: MetaContainer = emptyMetaContainer()
): StructElement =
    ionStructOf(
        fields.map { field(it.first, it.second.asAnyElement()) },
        annotations,
        metas
    )

// Memoized empty PersistentList for the memoized container types and null values
private val EMPTY_PERSISTENT_LIST: PersistentList<Nothing> = emptyList<Nothing>().toPersistentList()

// Memoized empty instances of our container types.
private val EMPTY_LIST = ListElementImpl(EMPTY_PERSISTENT_LIST, EMPTY_PERSISTENT_LIST, EMPTY_METAS)
private val EMPTY_SEXP = SexpElementImpl(EMPTY_PERSISTENT_LIST, EMPTY_PERSISTENT_LIST, EMPTY_METAS)
private val EMPTY_STRUCT = StructElementImpl(EMPTY_PERSISTENT_LIST, EMPTY_PERSISTENT_LIST, EMPTY_METAS)
private val EMPTY_BLOB = BlobElementImpl(ByteArray(0), EMPTY_PERSISTENT_LIST, EMPTY_METAS)
private val EMPTY_CLOB = ClobElementImpl(ByteArray(0), EMPTY_PERSISTENT_LIST, EMPTY_METAS)

// Memoized instances of all of our null values.
private val ALL_NULLS = ElementType.values().map {
    it to NullElementImpl(it, EMPTY_PERSISTENT_LIST, EMPTY_METAS) as IonElement
}.toMap()
