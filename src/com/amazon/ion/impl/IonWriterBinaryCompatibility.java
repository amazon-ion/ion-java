// Copyright (c) 2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonBinaryWriter;
import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonSystem;
import com.amazon.ion.impl.BlockedBuffer.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 *  These classes wrap the actual binary writers
 *  to offer the deprecated "get byte" style methods.
 *  If uses the BlockedBuffer's output stream that
 *  uses a BlockedBuffer for caching the bytes and
 *  offers the "get byte" methods.
 */
public abstract class IonWriterBinaryCompatibility
    implements IonBinaryWriter
{
    private static OutputStream make_output_stream()
    {
        BufferedOutputStream out = new BufferedOutputStream();
        return out;
    }

    abstract BufferedOutputStream get_output_stream();

    public static class System
        extends IonWriterSystemBinary
        implements IonBinaryWriter
    {

        public System(IonSystem sys, boolean autoFlush)
        {
            super(sys.getSystemSymbolTable(), make_output_stream(), autoFlush, false /* suppressIVM */);
            assert(getOutputStream() instanceof BlockedBuffer.BufferedOutputStream);
        }

        BufferedOutputStream get_output_stream()
        {
            OutputStream out = this.getOutputStream();
            assert(out instanceof BufferedOutputStream);
            return (BufferedOutputStream)out;
        }

        // we have to put these in twice since we don't have multiple inheritance
        // one of the few useful tasks it can be applied to (if it was available)
        public int byteSize()
        {
            try {
                finish();
            }
            catch (IOException e) {
                throw new IonException(e);
            }
            int size = get_output_stream().byteSize();
            return size;
        }

        public byte[] getBytes() throws IOException
        {
            finish();
            byte[] bytes = get_output_stream().getBytes();
            return bytes;
        }

        public int getBytes(byte[] bytes, int offset, int len)
            throws IOException
        {
            finish();
            int written = get_output_stream().getBytes(bytes, offset, len);
            return written;
        }
    }
    public static class User
        extends IonWriterUserBinary
        implements IonBinaryWriter
    {
        public User(IonSystem system, IonCatalog catalog)
        {
            super(system, catalog, new System(system, false /* autoflush */ ), false /* suppressIVM */);

            assert(getOutputStream() instanceof BufferedOutputStream);
        }

        BufferedOutputStream get_output_stream()
        {
            OutputStream out = this.getOutputStream();
            assert(out instanceof BufferedOutputStream);
            return (BufferedOutputStream)out;
        }

        // we have to put these in twice since we don't have multiple inheritance
        public int byteSize()
        {
            try {
                finish();
            }
            catch (IOException e) {
                throw new IonException(e);
            }
            int size = get_output_stream().byteSize();
            return size;
        }

        public byte[] getBytes() throws IOException
        {
            finish();
            byte[] bytes = get_output_stream().getBytes();
            return bytes;
        }

        public int getBytes(byte[] bytes, int offset, int len)
            throws IOException
        {
            finish();
            int written = get_output_stream().getBytes(bytes, offset, len);
            return written;
        }

        public int writeBytes(OutputStream userstream) throws IOException
        {
            finish();
            int written = get_output_stream().writeBytes(userstream);
            return written;
        }
    }
}
