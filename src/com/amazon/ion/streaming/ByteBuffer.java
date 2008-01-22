/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.streaming;

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
    public abstract ByteReader getReader();
    public abstract ByteWriter getWriter();
    public abstract byte[]     getBytes();
    public abstract int        getBytes(byte[] buffer, int offset, int length);
    public abstract void       writeBytes(OutputStream out) throws IOException;
}
