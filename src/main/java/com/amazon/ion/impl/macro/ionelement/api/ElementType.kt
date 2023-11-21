package com.amazon.ion.impl.macro.ionelement.api

import com.amazon.ion.IonType
import com.amazon.ion.impl.macro.ionelement.api.ElementType.BLOB
import com.amazon.ion.impl.macro.ionelement.api.ElementType.CLOB
import com.amazon.ion.impl.macro.ionelement.api.ElementType.LIST
import com.amazon.ion.impl.macro.ionelement.api.ElementType.SEXP
import com.amazon.ion.impl.macro.ionelement.api.ElementType.STRING
import com.amazon.ion.impl.macro.ionelement.api.ElementType.STRUCT
import com.amazon.ion.impl.macro.ionelement.api.ElementType.SYMBOL

/**
 * Indicates the type of the Ion value represented by an instance of [AnyElement].
 *
 * [ElementType] has all the same members as `ion-java`'s [IonType] except for [IonType.DATAGRAM] because [AnyElement]
 * has no notion of datagrams.  It also exposes [isText], [isContainer] and [isLob] as properties instead of as static
 * functions.
 */
public enum class ElementType(
    /** True if the current [ElementType] is [STRING] or [SYMBOL]. */
    public val isText: Boolean,

    /**
     * True if the current [ElementType] is [LIST] or [SEXP] or [STRUCT].
     *
     * Ordering of the child elements cannot be guaranteed for [STRUCT] elements.
     */
    public val isContainer: Boolean,

    /**
     * True if the current [ElementType] is [LIST] or [SEXP].
     *
     * Ordering of the child elements is guaranteed.
     */
    public val isSeq: Boolean,

    /** True if the current [ElementType] is [CLOB] or [BLOB]. */
    public val isLob: Boolean
) {
    // Other scalar types
    NULL(false, false, false, false),
    BOOL(false, false, false, false),
    INT(false, false, false, false),
    FLOAT(false, false, false, false),
    DECIMAL(false, false, false, false),
    TIMESTAMP(false, false, false, false),

    // String-valued types
    SYMBOL(true, false, false, false),
    STRING(true, false, false, false),

    // Binary-valued types
    CLOB(false, false, false, true),
    BLOB(false, false, false, true),

    // Container types
    LIST(false, true, true, false),
    SEXP(false, true, true, false),

    STRUCT(false, true, false, false);

    /** Converts this [ElementType] to [IonType]. */
    public fun toIonType(): IonType = when (this) {
        NULL -> IonType.NULL
        BOOL -> IonType.BOOL
        INT -> IonType.INT
        FLOAT -> IonType.FLOAT
        DECIMAL -> IonType.DECIMAL
        TIMESTAMP -> IonType.TIMESTAMP
        SYMBOL -> IonType.SYMBOL
        STRING -> IonType.STRING
        CLOB -> IonType.CLOB
        BLOB -> IonType.BLOB
        LIST -> IonType.LIST
        SEXP -> IonType.SEXP
        STRUCT -> IonType.STRUCT
    }
}

/**
 * Converts the receiver [IonType] to [ElementType].
 *
 * @throws [IllegalStateException] if the receiver is [IonType.DATAGRAM] because [AnyElement] has no notion of
 * datagrams.
 */
public fun IonType.toElementType(): ElementType = when (this) {
    IonType.NULL -> ElementType.NULL
    IonType.BOOL -> ElementType.BOOL
    IonType.INT -> ElementType.INT
    IonType.FLOAT -> ElementType.FLOAT
    IonType.DECIMAL -> ElementType.DECIMAL
    IonType.TIMESTAMP -> ElementType.TIMESTAMP
    IonType.SYMBOL -> ElementType.SYMBOL
    IonType.STRING -> ElementType.STRING
    IonType.CLOB -> ElementType.CLOB
    IonType.BLOB -> ElementType.BLOB
    IonType.LIST -> ElementType.LIST
    IonType.SEXP -> ElementType.SEXP
    IonType.STRUCT -> ElementType.STRUCT
    IonType.DATAGRAM -> error("IonType.DATAGRAM has no ElementType equivalent")
}
