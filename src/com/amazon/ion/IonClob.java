/* Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion;

import java.io.Reader;
import java.nio.charset.Charset;

/**
 * An Ion <code>clob</code> value.
 */
public interface IonClob
    extends IonValue, IonLob
{
    /**
     * Creates a new {@link Reader} that provides the value of this clob as
     * text, decoding the raw bytes using a given character set.
     *
     * @param cs must not be <code>null</code>.
     * @return a new reader positioned at the start of the clob,
     * or <code>null</code> if <code>this.isNullValue()</code>.
     */
    public Reader newReader(Charset cs);


    /**
     * Gets the value of this clob as a Java {@link String} value, decoding
     * the raw bytes using a given character set.  This is a convenience
     * wrapper around {@link #newReader(Charset)}.
     * <p>
     * The behavior of this method when the clob bytes are not valid in the
     * given charset is unspecified.
     *
     * @param cs must not be <code>null</code>.
     * @return the decoded text,
     * or <code>null</code> if <code>this.isNullValue()</code>.
     */
    public String stringValue(Charset cs);


    public IonClob clone();
}
