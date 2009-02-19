// Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.

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
    DATAGRAM
}
