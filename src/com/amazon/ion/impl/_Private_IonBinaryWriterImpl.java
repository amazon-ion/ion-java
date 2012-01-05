// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonBinaryWriter;
import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.ValueFactory;
import com.amazon.ion.impl.BlockedBuffer.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Adapts the binary {@link IonWriter} implementation to the deprecated
 * {@link IonBinaryWriter} interface.
 */
@SuppressWarnings("deprecation")
@Deprecated
public class _Private_IonBinaryWriterImpl
    extends IonWriterUserBinary
    implements IonBinaryWriter
{
    public _Private_IonBinaryWriterImpl(IonCatalog catalog,
                                        SymbolTable defaultSystemSymtab,
                                        ValueFactory symtabValueFactory,
                                        boolean streamCopyOptimized,
                                        SymbolTable... imports)
    {
        super(catalog, symtabValueFactory,
              new IonWriterSystemBinary(defaultSystemSymtab,
                                        new BufferedOutputStream(),
                                        false /* autoflush */,
                                        true /* ensureInitialIvm */),
              streamCopyOptimized,
              imports);
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
