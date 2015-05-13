// Copyright (c) 2015 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import java.io.IOException;


/**
 * A destination sink that can be fed bytes.  The typical usage is a {@link ByteTransferReader} that funnels data
 * to an binary Ion target.
 */
public interface _Private_ByteTransferSink
{
    /**
     * Writes the given data to the sink.
     *
     * @param data      The byte array to write.
     * @param off       The offset in the array to write from.
     * @param len       The length of data to write.
     */
    public void writeBytes(byte[] data, int off, int len) throws IOException;
}
