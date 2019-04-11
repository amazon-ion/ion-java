/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An {@link IonWriter} that encodes Ion binary data.
 *
 * @deprecated Use {@link IonSystem#newBinaryWriter(OutputStream, SymbolTable...)} instead.
 */
@Deprecated
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
    @Deprecated
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
    @Deprecated
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
    @Deprecated
    public int getBytes(byte[] bytes, int offset, int maxlen)
        throws IOException;

    /**
     * Writes the current contents of the writer to the output
     * stream.  This is only valid if the writer is not in the
     * middle of writing a container.
     *
     * @param userstream OutputStream to write the bytes to
     * @return int length of bytes written
     * @throws IOException
     */
    @Deprecated
    public int writeBytes(OutputStream userstream)
        throws IOException;

}
