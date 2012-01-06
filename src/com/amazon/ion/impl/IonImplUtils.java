// Copyright (c) 2008-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static com.amazon.ion.util.IonStreamUtils.isIonBinary;

import com.amazon.ion.EmptySymbolException;
import com.amazon.ion.InternedSymbol;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.UnknownSymbolException;
import com.amazon.ion.impl.IonBinary.BufferManager;
import com.amazon.ion.impl.IonBinary.Reader;
import com.amazon.ion.system.IonTextWriterBuilder;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.TimeZone;

/**
 * For internal use only!
 */
public final class IonImplUtils // TODO this class shouldn't be public
{
    /**
     * Marker for code points relevant to removal of IonReader.hasNext().
     */
    public static final boolean READER_HASNEXT_REMOVED = false;


    /** Just a zero-length byte array, used to avoid allocation. */
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

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



    public static final String ASCII_CHARSET_NAME = "US-ASCII";

    public static final Charset ASCII_CHARSET =
        Charset.forName(ASCII_CHARSET_NAME);

    /** The string {@code "UTF-8"}. */
    public static final String UTF8_CHARSET_NAME = "UTF-8";

    public static final Charset UTF8_CHARSET =
        Charset.forName(UTF8_CHARSET_NAME);


    /**
     * The UTC {@link TimeZone}.
     *
     * TODO determine if this is well-defined.
     */
    public static final TimeZone UTC = TimeZone.getTimeZone("UTC");



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

    @SuppressWarnings("unchecked")
    public static final <T> ListIterator<T> emptyIterator()
    {
        return (ListIterator<T>) EMPTY_ITERATOR;
    }

    /**
     * Replacement for Java6 {@link Arrays#copyOf(byte[], int)}.
     */
    public static byte[] copyOf(byte[] original, int newLength)
    {
        byte[] result = new byte[newLength];
        System.arraycopy(original, 0, result, 0,
                         Math.min(newLength, original.length));
        return result;
    }

    public static <T> void addAll(Collection<T> dest, Iterator<T> src)
    {
        if (src != null)
        {
            while (src.hasNext())
            {
                T value = src.next();
                dest.add(value);
            }
        }
    }

    public static <T> void addAllNonNull(Collection<T> dest, Iterator<T> src)
    {
        if (src != null)
        {
            while (src.hasNext())
            {
                T value = src.next();
                if (value != null)
                {
                    dest.add(value);
                }
            }
        }
    }


    /**
     * Throws {@link EmptySymbolException} if any of the strings are null or
     * empty.
     *
     * @param strings must not be null array.
     */
    public static void ensureNonEmptySymbols(String[] strings)
    {
        for (String s : strings)
        {
            if (s == null || s.length() == 0)
            {
                throw new EmptySymbolException();
            }
        }
    }

    /**
     * Throws {@link EmptySymbolException} if any of the symbols are null or
     * their text empty.
     *
     * @param symbols must not be null array.
     */
    public static void ensureNonEmptySymbols(InternedSymbol[] symbols)
    {
        for (InternedSymbol s : symbols)
        {
            if (s == null || s.getText() != null && s.getText().length() == 0)
            {
                throw new EmptySymbolException();
            }
        }
    }

    /**
     * @return not null
     */
    public static InternedSymbolImpl newInternedSymbol(String text, int sid)
    {
        return new InternedSymbolImpl(text, sid);
    }

    /**
     * @return not null
     */
    public static InternedSymbolImpl newInternedSymbol(int sid)
    {
        return new InternedSymbolImpl(sid);
    }

    /**
     * Checks symbol content.
     * @return not null
     */
    public static InternedSymbol newInternedSymbol(SymbolTable symtab,
                                                   String text)
    {
        // TODO ION-267 symtab should not be null
        if (text == null || text.length() == 0)
        {
            throw new EmptySymbolException();
        }
        InternedSymbol is = (symtab == null ? null : symtab.find(text));
        if (is == null)
        {
            is = new InternedSymbolImpl(text, UNKNOWN_SYMBOL_ID);
        }
        return is;
    }

    /**
     * @return not null
     */
    public static InternedSymbol newInternedSymbol(SymbolTable symtab,
                                                   int sid)
    {
        if (sid < 1) throw new IllegalArgumentException();

        // TODO ION-267 symtab should not be null
        String text = (symtab == null ? null : symtab.findKnownSymbol(sid));
        return new InternedSymbolImpl(text, sid);
    }

    /**
     * Validates each text element.
     * @param text may be null or empty.
     * @return not null.
     */
    public static InternedSymbol[] newInternedSymbols(SymbolTable symtab,
                                                      String... text)
    {
        if (text != null)
        {
            int count = text.length;
            if (count != 0)
            {
                InternedSymbol[] result = new InternedSymbol[count];
                for (int i = 0; i < count; i++)
                {
                    String s = text[i];
                    result[i] = newInternedSymbol(symtab, s);
                }
                return result;
            }
        }
        return InternedSymbol.EMPTY_ARRAY;
    }

    /**
     * @param syms may be null or empty.
     * @return not null.
     */
    public static InternedSymbol[] newInternedSymbols(SymbolTable symtab,
                                                      int... syms)
    {
        if (syms != null)
        {
            int count = syms.length;
            if (syms.length != 0)
            {
                InternedSymbol[] result = new InternedSymbol[count];
                for (int i = 0; i < count; i++)
                {
                    int s = syms[i];
                    result[i] = newInternedSymbol(symtab, s);
                }
                return result;
            }
        }
        return InternedSymbol.EMPTY_ARRAY;
    }


    public static InternedSymbol localize(SymbolTable symtab,
                                          InternedSymbol sym)
    {
        String text = sym.getText();
        int sid = sym.getId();

        if (symtab != null)  // TODO ION-267 require symtab
        {
            if (text == null)
            {
                text = symtab.findKnownSymbol(sid);
                if (text != null)
                {
                    sym = new InternedSymbolImpl(text, sid);
                }
            }
            else
            {
                InternedSymbol newSym = symtab.find(text);
                if (newSym != null)
                {
                    sym = newSym;
                }
                else if (sid >= 0)
                {
                    // We can't trust the sid, discard it.
                    sym = new InternedSymbolImpl(text, UNKNOWN_SYMBOL_ID);
                }
            }
        }
        else if (text != null && sid >= 0)
        {
            // We can't trust the sid, discard it.
            sym = new InternedSymbolImpl(text, UNKNOWN_SYMBOL_ID);
        }
        return sym;
    }


    /**
    *
    * @param syms may be mutated, replacing entries with localized updates!
    */
    public static void localize(SymbolTable symtab,
                                InternedSymbol[] syms,
                                int count)
    {
        for (int i = 0; i < count; i++)
        {
            InternedSymbol sym = syms[i];
            InternedSymbol updated = localize(symtab, sym);
            if (updated != sym) syms[i] = updated;
        }
    }

    /**
     *
     * @param syms may be mutated, replacing entries with localized updates!
     */
    public static void localize(SymbolTable symtab,
                                InternedSymbol[] syms)
    {
        localize(symtab, syms, syms.length);
    }


    public static String[] toStrings(InternedSymbol[] symbols, int count)
    {
        if (count == 0) return IonImplUtils.EMPTY_STRING_ARRAY;

        String[] annotations = new String[count];
        for (int i = 0; i < count; i++)
        {
            InternedSymbol sym = symbols[i];
            String text = sym.getText();
            if (text == null)
            {
                throw new UnknownSymbolException(sym.getId());
            }
            annotations[i] = text;
        }
        return annotations;
    }

    public static int[] toSids(InternedSymbol[] symbols, int count)
    {
        if (count == 0) return IonImplUtils.EMPTY_INT_ARRAY;

        int[] sids = new int[count];
        for (int i = 0; i < count; i++)
        {
            InternedSymbol sym = symbols[i];
            sids[i] = sym.getId();
        }
        return sids;
    }

    //========================================================================

    /**
     * Encodes a String into bytes of a given encoding.
     * <p>
     * This method is preferred to {@link Charset#encode(String)} and
     * {@link String#getBytes(String)} (<em>etc.</em>)
     * since those methods will replace or ignore bad input, and here we throw
     * an exception.
     *
     * @param s the string to encode.
     *
     * @return the encoded string, not null.
     *
     * @throws IonException if there's a {@link CharacterCodingException}.
     */
    public static byte[] encode(String s, Charset charset)
    {
        CharsetEncoder encoder = charset.newEncoder();
        try
        {
            ByteBuffer buffer = encoder.encode(CharBuffer.wrap(s));
            byte[] bytes = buffer.array();

            // Make another copy iff there's garbage after the limit.
            int limit = buffer.limit();
            if (limit < bytes.length)
            {
                bytes = copyOf(bytes, limit);
            }

            return bytes;
        }
        catch (CharacterCodingException e)
        {
            throw new IonException("Invalid string data", e);
        }
    }


    /**
     * Decodes a byte sequence into a string, given a {@link Charset}.
     * <p>
     * This method is preferred to {@link Charset#decode(ByteBuffer)} and
     * {@link String#String(byte[], Charset)} (<em>etc.</em>)
     * since those methods will replace or ignore bad input, and here we throw
     * an exception.
     *
     * @param bytes the data to decode.
     *
     * @return the decoded string, not null.
     *
     * @throws IonException if there's a {@link CharacterCodingException}.
     */
    public static String decode(byte[] bytes, Charset charset)
    {
        CharsetDecoder decoder = charset.newDecoder();
        try
        {
            CharBuffer buffer = decoder.decode(ByteBuffer.wrap(bytes));
            return buffer.toString();
        }
        catch (CharacterCodingException e)
        {
            String message =
                "Input is not valid " + charset.displayName() + " data";
            throw new IonException(message, e);
        }
    }


    /**
     * Encodes a String into UTF-8 bytes.
     * <p>
     * This method is preferred to {@link Charset#encode(String)} and
     * {@link String#getBytes(String)} (<em>etc.</em>)
     * since those methods will replace or ignore bad input, and here we throw
     * an exception.
     *
     * @param s the string to encode.
     *
     * @return the encoded UTF-8 bytes, not null.
     *
     * @throws IonException if there's a {@link CharacterCodingException}.
     */
    public static byte[] utf8(String s)
    {
        return encode(s, UTF8_CHARSET);
    }

    /**
     * Decodes a UTF-8 byte sequence to a String.
     * <p>
     * This method is preferred to {@link Charset#decode(ByteBuffer)} and
     * {@link String#String(byte[], Charset)} (<em>etc.</em>)
     * since those methods will replace or ignore bad input, and here we throw
     * an exception.
     *
     * @param bytes the data to decode.
     *
     * @return the decoded string, not null.
     *
     * @throws IonException if there's a {@link CharacterCodingException}.
     */
    public static String utf8(byte[] bytes)
    {
        return decode(bytes, UTF8_CHARSET);
    }


    //========================================================================

    /**
     * Calls {@link InputStream#read(byte[], int, int)} until the buffer is
     * filled or EOF is encountered.
     * This method will block until the request is satisfied.
     *
     * @param in        The stream to read from.
     * @param buf       The buffer to read to.
     *
     * @return the number of bytes read from the stream.  May be less than
     *  {@code buf.length} if EOF is encountered before reading that far.
     *
     * @see #readFully(InputStream, byte[], int, int)
     */
    public static int readFully(InputStream in, byte[] buf)
    throws IOException
    {
        return readFully(in, buf, 0, buf.length);
    }


    /**
     * Calls {@link InputStream#read(byte[], int, int)} until the requested
     * length is read or EOF is encountered.
     * This method will block until the request is satisfied.
     *
     * @param in        The stream to read from.
     * @param buf       The buffer to read to.
     * @param offset    The offset of the buffer to read from.
     * @param length    The length of the data to read.
     *
     * @return the number of bytes read from the stream.  May be less than
     *  {@code length} if EOF is encountered before reading that far.
     *
     * @see #readFully(InputStream, byte[])
     */
    public static int readFully(InputStream in, byte[] buf,
                                int offset, int length)
    throws IOException
    {
        int readBytes = 0;
        while (readBytes < length)
        {
            int amount = in.read(buf, offset, length - readBytes);
            if (amount < 0)
            {
                // EOF
                return readBytes;
            }
            readBytes += amount;
            offset += amount;
        }
        return readBytes;
    }


    public static byte[] loadFileBytes(File file)
        throws IOException
    {
        long len = file.length();
        if (len < 0 || len > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("File too long: " + file);
        }

        byte[] buf = new byte[(int) len];

        FileInputStream in = new FileInputStream(file);
        try {
            int readBytesCount = in.read(buf);
            if (readBytesCount != len || in.read() != -1)
            {
                throw new IOException("Read the wrong number of bytes from "
                                       + file);
            }
        }
        finally {
            in.close();
        }

        return buf;
    }

    public static String utf8FileToString(File file)
        throws IonException, IOException
    {
        byte[] utf8Bytes = IonImplUtils.loadFileBytes(file);
        String s = utf8(utf8Bytes);
        return s;
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

        int len = readFully(pushback, cookie);
        if (len == IonConstants.BINARY_VERSION_MARKER_SIZE) {
            isBinary = isIonBinary(cookie);
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

        IonTextWriterBuilder b = IonTextWriterBuilder.standard();
        b.setCharset(IonTextWriterBuilder.ASCII);
        IonWriter writer = b.build(out);

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

    public static boolean symtabExtends(SymbolTable superset, SymbolTable subset)
    {
        if (superset == subset) return true;

        if (superset.isLocalTable() && subset.isLocalTable())
        {
            // TODO ION-253 compare Ion version

            if (superset.getMaxId() < subset.getMaxId()) return false;

            // Stupid hack to prevent this from running away on big symtabs.
            if (20 < subset.getMaxId()) return false;

            // TODO ION-253 Optimize more by checking name, version, max ids of each import
            SymbolTable[] superImports = superset.getImportedTables();
            SymbolTable[] subImports = subset.getImportedTables();

            if (! Arrays.equals(superImports, subImports)) return false;

            // TODO ION-253 This is a ridiculous thing to do frequently.
            // What happen when we do this repeatedly (eg copying a stream)
            // and the symtabs are large?  That's O(n) effort each time!!
            // Can we memoize the result somehow?
            // Or just limit this comparison to "small" symtabs?
            Iterator<String> subSymbols = subset.iterateDeclaredSymbolNames();
            Iterator<String> superSymbols = superset.iterateDeclaredSymbolNames();
            while (subSymbols.hasNext())
            {
                if (! superSymbols.hasNext()) return false;

                String sub = subSymbols.next();
                String sup = superSymbols.next();
                if (! sub.equals(sup)) return false;
            }
            return true;
        }

        return false;
    }


    /**
     * Private to route clients through the static methods, which can
     * optimize the empty-list case.
     */
    private static final class StringIterator implements Iterator<String>
    {
        private final String[] _values;
        private int            _pos;
        private final int      _len;

        StringIterator(String[] values, int len) {
            _values = values;
            _len = len;
        }
        public boolean hasNext() {
            return (_pos < _len);
        }
        public String next() {
            if (!hasNext()) throw new NoSuchElementException();
            return _values[_pos++];
        }
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public static final Iterator<String> stringIterator(String... values)
    {
        if (values == null || values.length == 0)
        {
            return IonImplUtils.<String>emptyIterator();
        }
        return new StringIterator(values, values.length);
    }

    public static final Iterator<String> stringIterator(String[] values, int len)
    {
        if (values == null || values.length == 0 || len == 0)
        {
            return IonImplUtils.<String>emptyIterator();
        }
        return new StringIterator(values, len);
    }

    /**
     * Private to route clients through the static methods, which can
     * optimize the empty-list case.
     */
    private static final class IntIterator implements Iterator<Integer>
    {
        private final int []  _values;
        private int           _pos;
        private final int     _len;

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

    public static final Iterator<Integer> intIterator(int... values)
    {
        if (values == null || values.length == 0)
        {
            return IonImplUtils.<Integer>emptyIterator();
        }
        return new IntIterator(values);
    }

    public static final Iterator<Integer> intIterator(int[] values, int len)
    {
        if (values == null || values.length == 0 || len == 0)
        {
            return IonImplUtils.<Integer>emptyIterator();
        }
        return new IntIterator(values, 0, len);
    }
}
