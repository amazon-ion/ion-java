/*
 * Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved.
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
     * Sets the data of this lob, copying bytes from an array.
     *
     * @param bytes the new data for the lob;
     * may be <code>null</code> to make this an Ion <code>null</code> value.
     */
    public void setBytes(byte[] bytes);

    /**
     * Sets the data of this lob, copying bytes from part of an array.
     * <p>
     * This method copies {@code length} bytes from the given array into this
     * value, starting at the given offset in the array.
     *
     * @param bytes the new data for the lob;
     * may be <code>null</code> to make this an Ion <code>null</code> value.
     * @param offset the offset within the array of the first byte to copy;
     * must be non-negative an no larger than {@code bytes.length}.
     * @param length the number of bytes to be copied from the given array;
     * must be non-negative an no larger than {@code bytes.length - offset}.
     *
     * @throws IndexOutOfBoundsException
     * if the preconditions on the {@code offset} and {@code length} parameters
     * are not met.
     */
    public void setBytes(byte[] bytes, int offset, int length);

    /**
     * Gets the size in bytes of this lob.
     *
     * @return the lob's size in bytes.
     */
    public int byteSize();


    public IonLob clone();
}
