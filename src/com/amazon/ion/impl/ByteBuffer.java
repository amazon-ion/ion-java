// Copyright (c) 2008-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import java.io.IOException;
import java.io.OutputStream;

/**
 * This is a general purpose interface to support operations
 * over byte sources for Ion operations.  The implementations
 * are resposible for managing container state during operations
 * as well as the underlying data in the buffer itself.
 */
public interface ByteBuffer
{
    /**
     * creates a reader and sets it to the beginning of the buffer
     * under managment here for use.
     * @return a new reader
     */
    public abstract ByteReader getReader();

    /**
     * creates a writer and sets it to the beginning of the buffer
     * under managment here for use.
     * @return a new writer
     */
    public abstract ByteWriter getWriter();

    /**
     * wraps up the current buffer as a new byte array and passed
     * ownership of the copy to the caller.
     * @return a copy of the buffer in a new byte array
     */
    public abstract byte[]     getBytes();

    /**
     * copies the contents of the buffer into a user supplied
     * byte array.  It copies into the user buffer starting at
     * the supplied offset and attempts to copy no more than
     * length bytes.  If the implmentation cannot determine the
     * length to be copied before copying it may overwrite some
     * of the user buffer and will throw at the end of the copy.
     * (note that is may throw an out of bounds exception if
     * the write extends beyond the length of the callers array)
     * @param buffer users byte array to copy into
     * @param offset first byte of the array to use
     * @param length length of the byte array available to copy into
     * @return number of bytes actually copied
     */
    public abstract int        getBytes(byte[] buffer, int offset, int length);

    /**
     * copies the contents of the buffer into and output stream.
     * @param out stream to write into
     * @throws IOException
     */
    public abstract void       writeBytes(OutputStream out) throws IOException;
}
