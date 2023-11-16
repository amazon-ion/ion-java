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

package com.amazon.ion.impl;

import com.amazon.ion.IonBinaryWriter;
import com.amazon.ion.IonException;
import com.amazon.ion.IonWriter;
import com.amazon.ion.impl.BlockedBuffer.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * NOT FOR APPLICATION USE!
 * <p>
 * Adapts the binary {@link IonWriter} implementation to the deprecated
 * {@link IonBinaryWriter} interface.
 */
@Deprecated
public final class _Private_IonBinaryWriterImpl
    extends IonWriterUserBinary
    implements IonBinaryWriter
{
    _Private_IonBinaryWriterImpl(_Private_IonBinaryWriterBuilder options,
                                 IonWriterSystemBinary           systemWriter)
    {
        super(options, systemWriter);
    }


    private BufferedOutputStream getOutputStream()
    {
        IonWriterSystemBinary systemWriter =
            (IonWriterSystemBinary)_system_writer;
        return (BufferedOutputStream) systemWriter.getOutputStream();
    }


    public int byteSize()
    {
        try {
            finish();
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        int size = getOutputStream().byteSize();
        return size;
    }

    public byte[] getBytes() throws IOException
    {
        finish();
        byte[] bytes = getOutputStream().getBytes();
        return bytes;
    }

    public int getBytes(byte[] bytes, int offset, int len)
        throws IOException
    {
        finish();
        int written = getOutputStream().getBytes(bytes, offset, len);
        return written;
    }

    public int writeBytes(OutputStream userstream) throws IOException
    {
        finish();
        int written = getOutputStream().writeBytes(userstream);
        return written;
    }
}
