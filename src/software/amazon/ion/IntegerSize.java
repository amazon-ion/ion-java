// Copyright (c) 2016 Amazon.com, Inc.  All rights reserved.

package software.amazon.ion;

/**
 * Indicates the smallest-possible Java type of an Ion {@code int} value.
 */
public enum IntegerSize
{
    /**
     * Fits in the Java {@code int} primitive (four bytes).
     * The value can be retrieved through methods like {@link IonReader#intValue()}
     * or {@link IonInt#intValue()} without data loss.
     */
    INT,

    /**
     * Fits in the Java {@code int} primitive (eight bytes).
     * The value can be retrieved through methods like {@link IonReader#longValue()}
     * or {@link IonInt#longValue()} without data loss.
     */
    LONG,

    /**
     * Larger than eight bytes. This value can be retrieved through methods like
     * {@link IonReader#bigIntegerValue()} or {@link IonInt#bigIntegerValue()}
     * without data loss.
     */
    BIG_INTEGER,

}
