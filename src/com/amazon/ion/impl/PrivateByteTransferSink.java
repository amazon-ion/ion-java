// Copyright (c) 2015 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import java.io.IOException;


/**
 * A destination sink that can be fed bytes.  The typical usage is a {@link PrivateByteTransferReader} that funnels data
 * to an binary Ion target.
 *
 * @deprecated This is an internal API that is subject to change without notice.
 */
@Deprecated
public interface PrivateByteTransferSink
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
