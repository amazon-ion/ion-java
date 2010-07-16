// Copyright (c) 2008-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.IonBinary.BufferManager;
import com.amazon.ion.impl.IonBinary.Reader;
import com.amazon.ion.impl.IonWriterUserText.TextOptions;
import com.amazon.ion.system.SystemFactory;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
 * For internal use only!
 */
public final class IonImplUtils // TODO this class shouldn't be public
{
    /**
     * Marker for code points relevant to removal of IonReader.hasNext().
     */
    public static final boolean READER_HASNEXT_REMOVED = false;


    /** Just a zero-length String array, used to avoid allocation. */
    public final static String[] EMPTY_STRING_ARRAY = new String[0];

    /** Just a zero-length int array, used to avoid allocation. */
    public final static int[] EMPTY_INT_ARRAY = new int[0];

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


    public static final ListIterator<?> EMPTY_ITERATOR = new ListIterator() {
        public boolean hasNext()     { return false; }
        public boolean hasPrevious() { return false; }

        public Object  next()     { throw new NoSuchElementException(); }
        public Object  previous() { throw new NoSuchElementException(); }
        public void    remove()   { throw new IllegalStateException(); }

        public int nextIndex()     { return  0; }
        public int previousIndex() { return -1; }

        public void add(Object o) { throw new UnsupportedOperationException(); }
        public void set(Object o) { throw new UnsupportedOperationException(); }
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
        TextOptions options = new TextOptions(false, true, false); // pretty print, ascii only, filter symbol tables

        // This is vaguely inappropriate.
        SymbolTable systemSymtab =
            SystemFactory.newSystem().getSystemSymbolTable();
        IonWriterSystemText writer =
            new IonWriterSystemText(systemSymtab, out, options);
        // IonWriter writer = IonWriterUserText new IonTextWriter(out);

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

    static final class StringIterator implements Iterator<String>
    {
        String [] _values;
        int       _pos;

        StringIterator(String[] values) {
            _values = values;
        }
        public boolean hasNext() {
            return (_pos < _values.length);
        }
        public String next() {
            if (!hasNext()) throw new NoSuchElementException();
            return _values[_pos++];
        }
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    static final class IntIterator implements Iterator<Integer>
    {
        int []  _values;
        int     _pos;
        int     _len;

        IntIterator(int[] values) {
            this(values, 0, values.length);
        }
        IntIterator(int[] values, int off, int len) {
            _values = values;
            _len = len;
            _pos = off;
        }
        public boolean hasNext() {
            return (_pos < _len);
        }
        public Integer next() {
            if (!hasNext()) throw new NoSuchElementException();
            int value = _values[_pos++];
            return value;
        }
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}
