/* Copyright (c) 2008 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion;

import java.io.IOException;

/**
 *
 */
public interface IonBinaryWriter
    extends IonWriter
{

    /**
     * Gets the size in bytes of this binary data.
     * This is generally needed before calling {@link #getBytes()} or
     * {@link #getBytes(byte[], int, int)}.
     *
     * @return the size in bytes.
     */
    public int byteSize();

    /**
     * Copies the current contents of this writer as a new byte array holding
     * Ion binary-encoded data.
     * This allocates an array of the size needed to exactly
     * hold the output and copies the entire value to it.
     *
     * @return the byte array with the writers output
     * @throws IOException
     */
    public byte[] getBytes()
        throws IOException;


    /**
     * Copies the current contents of the writer to a given byte array
     * array.  This starts writing to the array at offset and writes
     * up to maxlen bytes.
     * If this writer is not able to stop in the middle of its
     * work this may overwrite the array and later throw and exception.
     *
     * @param bytes users byte array to write into
     * @param offset initial offset in the array to write into
     * @param maxlen maximum number of bytes to write
     * @return number of bytes written
     * @throws IOException
     */
    public int getBytes(byte[] bytes, int offset, int maxlen)
        throws IOException;

}
