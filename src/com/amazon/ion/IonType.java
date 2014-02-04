// Copyright (c) 2007-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;


/**
 * Enumeration identifying the core Ion data types.
 * <p>
 * Note that {@link #DATAGRAM} is a pseudo-type used only by
 * {@link IonDatagram}, and it is not a legal value in most places where
 * {@code IonType} is used.
 */
public enum IonType
{
    NULL,
    BOOL,
    INT,
    FLOAT,
    DECIMAL,
    TIMESTAMP,
    SYMBOL,
    STRING,
    CLOB,
    BLOB,
    LIST,
    SEXP,
    STRUCT,
    DATAGRAM;


    /**
     * Determines whether a type represents an Ion container.
     * This includes {@link #DATAGRAM}.
     *
     * @param t may be null.
     *
     * @return true when {@code t} is {@link #LIST}, {@link #SEXP},
     * {@link #STRUCT}, or {@link #DATAGRAM}.
     *
     * @since IonJava R13
     */
    public static boolean isContainer(IonType t)
    {
        return (t != null && (t.ordinal() >= LIST.ordinal()));
    }

    /**
     * Determines whether a type represents an Ion text scalar, namely
     * {@link #STRING} or {@link #SYMBOL} (but not {@link #CLOB}).
     *
     * @param t may be null.
     *
     * @return true when {@code t} is {@link #STRING} or {@link #SYMBOL}.
     *
     * @since IonJava R15
     */
    public static boolean isText(IonType t)
    {
        return (t == STRING) || (t == SYMBOL);
    }

    /**
     * Determines whether a type represents an Ion LOB scalar, namely
     * {@link #BLOB} or {@link #CLOB}.
     *
     * @param t may be null.
     *
     * @return true when {@code t} is {@link #BLOB} or {@link #CLOB}.
     *
     * @since IonJava R15
     */
    public static boolean isLob(IonType t)
    {
        return (t == BLOB) || (t == CLOB);
    }
}
