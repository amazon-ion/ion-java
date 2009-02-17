// Copyright (c) 2008-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonWriter;
import com.amazon.ion.impl.IonBinary.BufferManager;
import com.amazon.ion.impl.IonBinary.Reader;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * For internal use only!
 */
final class IonImplUtils
{
    /**
     * TODO Jonker 2009-02-12: Actual lookahead limit is unclear to me!
     *
     * (null.timestamp) requires 11 ASCII chars to distinguish from
     * (null.timestamps) aka (null '.' 'timestamps')
     *
     * @see IonCharacterReader#DEFAULT_BUFFER_SIZE
     * @see IonCharacterReader#BUFFER_PADDING
     */
    public static final int MAX_LOOKAHEAD_UTF16 = 11;


    public static final Iterator<?> EMPTY_ITERATOR = new Iterator() {
        public boolean hasNext() { return false; }
        public Object  next()    { throw new NoSuchElementException(); }
        public void    remove()  { throw new IllegalStateException(); }
    };


    public static <T> void addAllNonNull(Collection<T> dest, Iterator<T> src)
    {
        if (src != null)
        {
            while (src.hasNext())
            {
                T sym = src.next();
                if (sym != null)
                {
                    dest.add(sym);
                }
            }
        }
    }


    public static byte[] loadFileBytes(File file)
        throws IOException
    {
        int len = (int)file.length();
        byte[] buf = new byte[len];

        FileInputStream in = new FileInputStream(file);
        try {
            // TODO I don't think buffering here is helpful since we are
            // doing a bulk read into our own buffer.
            BufferedInputStream bin = new BufferedInputStream(in);
            try {
                bin.read(buf);
            }
            finally {
                bin.close();
            }
        }
        finally {
            in.close();
        }

        return buf;
    }


    public static byte[] loadStreamBytes(InputStream in)
        throws IOException
    {
        BufferManager buffer = new BufferManager(in);
        Reader bufReader = buffer.reader();
        bufReader.sync();
        bufReader.setPosition(0);
        byte[] bytes = bufReader.getBytes();
        return bytes;
    }


    public static String loadReader(java.io.Reader in)
        throws IOException
    {
        StringBuilder buf = new StringBuilder(2048);

        char[] chars = new char[2048];

        int len;
        while ((len = in.read(chars)) != -1)
        {
            buf.append(chars, 0, len);
        }

        return buf.toString();
    }


    public static boolean streamIsIonBinary(PushbackInputStream pushback)
        throws IonException, IOException
    {
        boolean isBinary = false;
        byte[] cookie = new byte[IonConstants.BINARY_VERSION_MARKER_SIZE];

        int len = pushback.read(cookie);
        if (len == IonConstants.BINARY_VERSION_MARKER_SIZE) {
            isBinary = IonBinary.matchBinaryVersionMarker(cookie);
        }
        if (len > 0) {
            pushback.unread(cookie, 0, len);
        }
        return isBinary;
    }


    /**
     * Returns the current value as a String using the Ion toString() serialization
     * format.  This is only valid if there is an underlying value.  This is
     * logically equivalent to getIonValue().toString() but may be more efficient
     * and does not require an IonSystem context to operate.
     */
    public static String valueToString(IonReader reader)
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter writer = new IonTextWriter(out);
        try
        {
            writer.writeValue(reader);
        }
        catch (IOException e)
        {
            throw new IllegalStateException(e);
        }
        String s = out.toString();
        return s;
    }
}
