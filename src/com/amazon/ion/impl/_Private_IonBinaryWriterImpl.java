// Copyright (c) 2010-2014 Amazon.com, Inc.  All rights reserved.

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
@SuppressWarnings("deprecation")
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
