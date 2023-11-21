package com.amazon.ion.impl.macro.ionelement.api

import com.amazon.ion.Decimal
import com.amazon.ion.IonWriter
import com.amazon.ion.Timestamp
import java.math.BigInteger

/**
 * Represents an immutable Ion element.
 *
 * Specifies the contract that is common to all Ion elements but does not specify the type of data being represented.
 *
 * #### IonElement Hierarchy
 *
 * Each type in the following hierarchy extends [IonElement] by adding strongly typed accessor functions that
 * correspond to one or more Ion data types.  Except for [AnyElement], the types inheriting from [IonElement] are
 * referred to as "narrow types".
 *
 * - [IonElement]
 *     - [AnyElement]
 *     - [BoolElement]
 *     - [IntElement]
 *     - [FloatElement]
 *     - [DecimalElement]
 *     - [TimestampElement]
 *     - [TextElement]
 *         - [StringElement]
 *         - [SymbolElement]
 *     - [LobElement]
 *         - [BlobElement]
 *         - [ClobElement]
 *     - [ContainerElement]
 *         - [SeqElement]
 *             - [ListElement]
 *             - [SexpElement]
 *         - [StructElement]
 *
 * #### Equivalence
 *
 * All implementations of [IonElement] implement [Object.equals] and [Object.hashCode] according to the Ion
 * specification.
 *
 * Collections returned from the following properties implement [Object.equals] and [Object.hashCode] according to the
 * requirements of [List<T>], wherein order is significant.
 *
 * - [ContainerElement.values]
 * - [SeqElement.values]
 * - [ListElement.values]
 * - [SexpElement.values]
 * - [StructElement.values]

 * Be aware that this can yield inconsistent results when working with structs, due to their unordered nature.
 *
 * ```
 * val s = loadSingleElement("{ a: 1, b: 2 }").asStruct()
 * val l = loadSingleElement("[1, 2]").asList()
 *
 * // The following has an undefined result because the order of values returned by [StructElement.values] is not
 * // guaranteed:
 *
 * s.values.equals(l.values)
 * ```
 *
 * When in doubt, prefer use of [Object.equals] and [Object.hashCode] on the [IonElement] instance.
 */
public interface IonElement {

    /**
     * All [IonElement] implementations must convertible to [AnyElement].
     *
     * Since all [IonElement] implementations in this library also implement [AnyElement] this is no more
     * expensive than a cast.  The purpose of this interface function is to be very clear about the requirement
     * that all implementations of [IonElement] are convertible to [AnyElement].
     */
    public fun asAnyElement(): AnyElement

    /** The Ion data type of the current node.  */
    public val type: ElementType

    /** This [IonElement]'s metadata. */
    public val metas: MetaContainer

    /** This element's Ion type annotations. */
    public val annotations: List<String>

    /** Returns true if the current value is `null.null` or any typed null. */
    public val isNull: Boolean

    /** Returns a shallow copy of the current node, replacing the annotations and metas with those specified. */
    public fun copy(annotations: List<String> = this.annotations, metas: MetaContainer = this.metas): IonElement

    /** Writes the current Ion element to the specified [IonWriter]. */
    public fun writeTo(writer: IonWriter)

    /** Converts the current element to Ion text. */
    override fun toString(): String

    /*
     * The following `with*` mutators are repeated on every sub-interface because any approach using generics that is
     * ergonomic for Java code results in conflicting function declarations for classes that both extend AnyElementBase
     * and implement a type-specific sub-interface of IonElement.
     */

    /** Returns a shallow copy of the current node with the specified additional annotations. */
    public fun withAnnotations(vararg additionalAnnotations: String): IonElement

    /** Returns a shallow copy of the current node with the specified additional annotations. */
    public fun withAnnotations(additionalAnnotations: Iterable<String>): IonElement

    /** Returns a shallow copy of the current node with all annotations removed. */
    public fun withoutAnnotations(): IonElement

    /**
     * Returns a shallow copy of the current node with the specified additional metadata, overwriting any metas
     * that already exist with the same keys.
     */
    public fun withMetas(additionalMetas: MetaContainer): IonElement

    /**
     * Returns a shallow copy of the current node with the specified additional meta, overwriting any meta
     * that previously existed with the same key.
     *
     * When adding multiple metas, consider [withMetas] instead.
     */
    public fun withMeta(key: String, value: Any): IonElement

    /** Returns a shallow copy of the current node without any metadata. */
    public fun withoutMetas(): IonElement
}

/** Represents an Ion bool. */
public interface BoolElement : IonElement {
    public val booleanValue: Boolean
    override fun copy(annotations: List<String>, metas: MetaContainer): BoolElement
    override fun withAnnotations(vararg additionalAnnotations: String): BoolElement
    override fun withAnnotations(additionalAnnotations: Iterable<String>): BoolElement
    override fun withoutAnnotations(): BoolElement
    override fun withMetas(additionalMetas: MetaContainer): BoolElement
    override fun withMeta(key: String, value: Any): BoolElement
    override fun withoutMetas(): BoolElement
}

/** Represents an Ion timestamp. */
public interface TimestampElement : IonElement {
    public val timestampValue: Timestamp
    override fun copy(annotations: List<String>, metas: MetaContainer): TimestampElement

    override fun withAnnotations(vararg additionalAnnotations: String): TimestampElement
    override fun withAnnotations(additionalAnnotations: Iterable<String>): TimestampElement
    override fun withoutAnnotations(): TimestampElement
    override fun withMetas(additionalMetas: MetaContainer): TimestampElement
    override fun withMeta(key: String, value: Any): TimestampElement
    override fun withoutMetas(): TimestampElement
}

/** Indicates the size of the integer element. */
public enum class IntElementSize {
    /** For integer values representable by a [Long]. */
    LONG,
    /** For values larger than [Long.MAX_VALUE] or smaller than [Long.MIN_VALUE]. */
    BIG_INTEGER
}

/** Represents an Ion int. */
public interface IntElement : IonElement {

    /** The size of this [IntElement]. */
    public val integerSize: IntElementSize

    /**
     * Use this property to access the integer value of this [IntElement] when its value fits in a [Long].
     *
     * @throws IonElementConstraintException if [integerSize] is [IntElementSize.BIG_INTEGER].
     */
    public val longValue: Long

    /**
     * This property may be used to access the integer value of this [IntElement] if its value does not fit in a [Long].
     */
    public val bigIntegerValue: BigInteger
    override fun copy(annotations: List<String>, metas: MetaContainer): IntElement

    override fun withAnnotations(vararg additionalAnnotations: String): IntElement
    override fun withAnnotations(additionalAnnotations: Iterable<String>): IntElement
    override fun withoutAnnotations(): IntElement
    override fun withMetas(additionalMetas: MetaContainer): IntElement
    override fun withMeta(key: String, value: Any): IntElement
    override fun withoutMetas(): IntElement
}

/** Represents an Ion decimal. */
public interface DecimalElement : IonElement {
    public val decimalValue: Decimal
    override fun copy(annotations: List<String>, metas: MetaContainer): DecimalElement

    override fun withAnnotations(vararg additionalAnnotations: String): DecimalElement
    override fun withAnnotations(additionalAnnotations: Iterable<String>): DecimalElement
    override fun withoutAnnotations(): DecimalElement
    override fun withMetas(additionalMetas: MetaContainer): DecimalElement
    override fun withMeta(key: String, value: Any): DecimalElement
    override fun withoutMetas(): DecimalElement
}

/**
 * Represents an Ion float.
 */
public interface FloatElement : IonElement {
    public val doubleValue: Double
    override fun copy(annotations: List<String>, metas: MetaContainer): FloatElement

    override fun withAnnotations(vararg additionalAnnotations: String): FloatElement
    override fun withAnnotations(additionalAnnotations: Iterable<String>): FloatElement
    override fun withoutAnnotations(): FloatElement
    override fun withMetas(additionalMetas: MetaContainer): FloatElement
    override fun withMeta(key: String, value: Any): FloatElement
    override fun withoutMetas(): FloatElement
}

/** Represents an Ion string or symbol. */
public interface TextElement : IonElement {
    public val textValue: String
    override fun copy(annotations: List<String>, metas: MetaContainer): TextElement

    override fun withAnnotations(vararg additionalAnnotations: String): TextElement
    override fun withAnnotations(additionalAnnotations: Iterable<String>): TextElement
    override fun withoutAnnotations(): TextElement
    override fun withMetas(additionalMetas: MetaContainer): TextElement
    override fun withMeta(key: String, value: Any): TextElement
    override fun withoutMetas(): TextElement
}

/**
 * Represents an Ion string.
 *
 * Includes no additional functionality over [TextElement], but serves to provide additional type safety when
 * working with elements that must be Ion strings.
 */
public interface StringElement : TextElement {
    override fun copy(annotations: List<String>, metas: MetaContainer): StringElement

    override fun withAnnotations(vararg additionalAnnotations: String): StringElement
    override fun withAnnotations(additionalAnnotations: Iterable<String>): StringElement
    override fun withoutAnnotations(): StringElement
    override fun withMetas(additionalMetas: MetaContainer): StringElement
    override fun withMeta(key: String, value: Any): StringElement
    override fun withoutMetas(): StringElement
}

/**
 * Represents an Ion symbol.
 *
 * Includes no additional functionality over [TextElement], but serves to provide additional type safety when
 * working with elements that must be Ion symbols.
 */
public interface SymbolElement : TextElement {
    override fun copy(annotations: List<String>, metas: MetaContainer): SymbolElement

    override fun withAnnotations(vararg additionalAnnotations: String): SymbolElement
    override fun withAnnotations(additionalAnnotations: Iterable<String>): SymbolElement
    override fun withoutAnnotations(): SymbolElement
    override fun withMetas(additionalMetas: MetaContainer): SymbolElement
    override fun withMeta(key: String, value: Any): SymbolElement
    override fun withoutMetas(): SymbolElement
}

/** Represents an Ion clob or blob. */
public interface LobElement : IonElement {
    public val bytesValue: ByteArrayView
    override fun copy(annotations: List<String>, metas: MetaContainer): LobElement

    override fun withAnnotations(vararg additionalAnnotations: String): LobElement
    override fun withAnnotations(additionalAnnotations: Iterable<String>): LobElement
    override fun withoutAnnotations(): LobElement
    override fun withMetas(additionalMetas: MetaContainer): LobElement
    override fun withMeta(key: String, value: Any): LobElement
    override fun withoutMetas(): LobElement
}

/**
 * Represents an Ion blob.
 *
 * Includes no additional functionality over [LobElement], but serves to provide additional type safety when
 * working with elements that must be Ion blobs.
 */
public interface BlobElement : LobElement {
    override fun copy(annotations: List<String>, metas: MetaContainer): BlobElement

    override fun withAnnotations(vararg additionalAnnotations: String): BlobElement
    override fun withAnnotations(additionalAnnotations: Iterable<String>): BlobElement
    override fun withoutAnnotations(): BlobElement
    override fun withMetas(additionalMetas: MetaContainer): BlobElement
    override fun withMeta(key: String, value: Any): BlobElement
    override fun withoutMetas(): BlobElement
}

/**
 * Represents an Ion clob.
 *
 * Includes no additional functionality over [LobElement], but serves to provide additional type safety when
 * working with elements that must be Ion clobs.
 */
public interface ClobElement : LobElement {
    override fun copy(annotations: List<String>, metas: MetaContainer): ClobElement

    override fun withAnnotations(vararg additionalAnnotations: String): ClobElement
    override fun withAnnotations(additionalAnnotations: Iterable<String>): ClobElement
    override fun withoutAnnotations(): ClobElement
    override fun withMetas(additionalMetas: MetaContainer): ClobElement
    override fun withMeta(key: String, value: Any): ClobElement
    override fun withoutMetas(): ClobElement
}

/**
 * Represents an Ion list, s-expression or struct.
 *
 * Items within [values] may or may not be in a defined order.  The order is defined for lists and s-expressions,
 * but undefined for structs.
 *
 * #### Equality
 *
 * See the note about equivalence in the documentation for [IonElement].
 *
 * @see [IonElement]
 */
public interface ContainerElement : IonElement {
    /** The number of values in this container. */
    public val size: Int

    public val values: Collection<AnyElement>

    override fun copy(annotations: List<String>, metas: MetaContainer): ContainerElement

    override fun withAnnotations(vararg additionalAnnotations: String): ContainerElement
    override fun withAnnotations(additionalAnnotations: Iterable<String>): ContainerElement
    override fun withoutAnnotations(): ContainerElement
    override fun withMetas(additionalMetas: MetaContainer): ContainerElement
    override fun withMeta(key: String, value: Any): ContainerElement
    override fun withoutMetas(): ContainerElement
}

/**
 * Represents an ordered collection element such as an Ion list or s-expression.
 *
 * Includes no additional functionality over [ContainerElement], but serves to provide additional type safety when
 * working with ordered collection elements.
 *
 * #### Equivalence
 *
 * See the note about equivalence in the documentation for [IonElement].
 *
 * @see [IonElement]
 */
public interface SeqElement : ContainerElement {
    /** Narrows the return type of [ContainerElement.values] to [List<AnyElement>]. */
    override val values: List<AnyElement>

    override fun copy(annotations: List<String>, metas: MetaContainer): SeqElement

    override fun withAnnotations(vararg additionalAnnotations: String): SeqElement
    override fun withAnnotations(additionalAnnotations: Iterable<String>): SeqElement
    override fun withoutAnnotations(): SeqElement
    override fun withMetas(additionalMetas: MetaContainer): SeqElement
    override fun withMeta(key: String, value: Any): SeqElement
    override fun withoutMetas(): SeqElement
}
/**
 * Represents an Ion list.
 *
 * Includes no additional functionality over [SeqElement], but serves to provide additional type safety when
 * working with elements that must be Ion lists.
 *
 * #### Equivalence
 *
 * See the note about equivalence in the documentation for [IonElement].
 *
 * @see [IonElement]
 */
public interface ListElement : SeqElement {
    override fun copy(annotations: List<String>, metas: MetaContainer): ListElement

    override fun withAnnotations(vararg additionalAnnotations: String): ListElement
    override fun withAnnotations(additionalAnnotations: Iterable<String>): ListElement
    override fun withoutAnnotations(): ListElement
    override fun withMetas(additionalMetas: MetaContainer): ListElement
    override fun withMeta(key: String, value: Any): ListElement
    override fun withoutMetas(): ListElement
}

/**
 * Represents an Ion s-expression.
 *
 * Includes no additional functionality over [SeqElement], but serves to provide additional type safety when
 * working with elements that must be Ion s-expressions.
 *
 * #### Equivalence
 *
 * See the note about equivalence in the documentation for [IonElement].
 *
 * @see [IonElement]
 */
public interface SexpElement : SeqElement {
    override fun copy(annotations: List<String>, metas: MetaContainer): SexpElement

    override fun withAnnotations(vararg additionalAnnotations: String): SexpElement
    override fun withAnnotations(additionalAnnotations: Iterable<String>): SexpElement
    override fun withoutAnnotations(): SexpElement
    override fun withMetas(additionalMetas: MetaContainer): SexpElement
    override fun withMeta(key: String, value: Any): SexpElement
    override fun withoutMetas(): SexpElement
}

/**
 * Represents an Ion struct.
 *
 * Includes functions for accessing the fields of a struct.
 *
 * #### Equivalence
 *
 * See the note about equivalence in the documentation for [IonElement].
 *
 * @see [IonElement]
 */
public interface StructElement : ContainerElement {

    /** This struct's unordered collection of fields. */
    public val fields: Collection<StructField>

    /**
     * Retrieves the value of the first field found with the specified name.
     *
     * In the case of multiple fields with the specified name, the caller assume that one is picked at random.
     *
     * @throws IonElementException If there are no fields with the specified [fieldName].
     */
    public operator fun get(fieldName: String): AnyElement

    /** The same as [get] but returns a null reference if the field does not exist.  */
    public fun getOptional(fieldName: String): AnyElement?

    /** Retrieves all values with a given field name. Returns an empty iterable if the field does not exist. */
    public fun getAll(fieldName: String): Iterable<AnyElement>

    /** Returns true if this StructElement has at least one field with the given field name. */
    public fun containsField(fieldName: String): Boolean

    override fun copy(annotations: List<String>, metas: MetaContainer): StructElement
    override fun withAnnotations(vararg additionalAnnotations: String): StructElement
    override fun withAnnotations(additionalAnnotations: Iterable<String>): StructElement
    override fun withoutAnnotations(): StructElement
    override fun withMetas(additionalMetas: MetaContainer): StructElement
    override fun withMeta(key: String, value: Any): StructElement
    override fun withoutMetas(): StructElement
}
