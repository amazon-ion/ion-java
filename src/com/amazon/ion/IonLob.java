/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.io.InputStream;

/**
 *
 */
public interface IonLob
    extends IonValue
{
    /**
     * Creates a new {@link InputStream} that returns the data as raw
     * bytes.
     *
     * @return a new stream positioned at the start of the lob,
     * or <code>null</code> if <code>this.isNullValue()</code>.
     */
    public InputStream newInputStream();

    /**
     * Gets all the data of this lob, or <code>null</code> if this is an Ion
     * <code>null</code> value.
     *
     * @return a new byte array,
     * or <code>null</code> if <code>this.isNullValue()</code>.
     */
    public byte[] newBytes();

    /**
     * Sets the data of this lob.
     * <p>
     * TODO: define copy behavior?
     *
     * @param bytes the new data for the lob;
     * may be <code>null</code> to make this an Ion <code>null</code> value.
     */
    public void setBytes(byte[] bytes);

    /**
     * Gets the size in bytes of this lob.
     *
     * @return the lob's size in bytes
     */
    public int byteSize();
}
