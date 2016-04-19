/*
 * Copyright 2007-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion;


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
     */
    public static boolean isLob(IonType t)
    {
        return (t == BLOB) || (t == CLOB);
    }
}
