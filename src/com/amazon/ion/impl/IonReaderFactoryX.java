// Copyright (c) 2009-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl.UnifiedInputStreamX.makeStream;
import static com.amazon.ion.impl._Private_IonConstants.BINARY_VERSION_MARKER_SIZE;
import static com.amazon.ion.util.IonStreamUtils.isIonBinary;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTextReader;
import com.amazon.ion.IonValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

/**
 *     temporary factory to create the July 2009
 *     version of the readers (both text and binary)
 *     these should be moved into system or some
 *     other class for public consumption and this
 *     class should be deprecated
 */
@SuppressWarnings("deprecation")
public class IonReaderFactoryX
{
    public static final IonTextReader makeReader(IonSystem system, char[] chars) {
        IonTextReader r = makeReader(system, system.getCatalog(), chars, 0, chars.length);
        return r;
    }
    public static final IonTextReader makeReader(IonSystem system, char[] chars, int offset, int length) {
        IonTextReader r = makeReader(system, system.getCatalog(), chars, offset, length);
        return r;
    }
    public static final IonTextReader makeReader(IonSystem system, CharSequence chars) {
        IonTextReader r = makeReader(system, chars, 0, chars.length());
        return r;
    }
    public static final IonTextReader makeReader(IonSystem system, CharSequence chars, int offset, int length) {
        IonTextReader r = makeReader(system, system.getCatalog(), chars, offset, length);
        return r;
    }
    public static final IonTextReader makeReader(IonSystem system, Reader chars) {
        IonTextReader r = makeReader(system, system.getCatalog(), chars);
        return r;
    }
    public static final IonReader makeReader(IonSystem system, IonValue value) {
        IonReader r = makeReader(system, system.getCatalog(), value);
        return r;
    }

    // and you can supply your own catalog (otherwise we used the system catalog
    public static final IonTextReader makeReader(IonSystem system, IonCatalog catalog, char[] chars) {
        IonTextReader r = makeReader(system, catalog, chars, 0, chars.length);
        return r;
    }

    public static final IonTextReader makeReader(IonSystem system,
                                                 IonCatalog catalog,
                                                 char[] chars,
                                                 int offset,
                                                 int length)
    {
        UnifiedInputStreamX in = makeStream(chars, offset, length);
        IonTextReader r = new IonReaderTextUserX(system, catalog, in, offset);
        return r;
    }

    public static final IonTextReader makeReader(IonSystem system,
                                                 IonCatalog catalog,
                                                 CharSequence chars)
    {
        UnifiedInputStreamX in = makeStream(chars);
        IonTextReader r = new IonReaderTextUserX(system, catalog, in);
        return r;
    }

    public static final IonTextReader makeReader(IonSystem system,
                                                 IonCatalog catalog,
                                                 CharSequence chars,
                                                 int offset,
                                                 int length)
    {
        UnifiedInputStreamX in = makeStream(chars, offset, length);
        IonTextReader r = new IonReaderTextUserX(system, catalog, in, offset);
        return r;
    }

    public static final IonTextReader makeReader(IonSystem system,
                                                 IonCatalog catalog,
                                                 Reader chars) {
        UnifiedInputStreamX in;
        try {
            in = makeStream(chars);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        IonTextReader r = new IonReaderTextUserX(system, catalog, in);
        return r;
    }

    public static final IonReader makeReader(IonSystem system, IonCatalog catalog, IonValue value) {
        IonReader r = new IonReaderTreeUserX(value, catalog);
        return r;
    }

    // or with a system and the right request you can get a system reader
    // but since system reader don't do symbol tables a catalog is not
    // only useless but misleading
    public static IonTextReader makeSystemReader(IonSystem system,
                                                 String ionText)
    {
        UnifiedInputStreamX in = makeStream(ionText);
        IonTextReader r = new IonReaderTextSystemX(system, in);
        return r;
    }

    public static final IonTextReader makeSystemReader(IonSystem system,
                                                       char[] chars)
    {
        UnifiedInputStreamX in = makeStream(chars);
        IonTextReader r = new IonReaderTextSystemX(system, in);
        return r;
    }

    public static final IonTextReader makeSystemReader(IonSystem system,
                                                       char[] chars,
                                                       int offset,
                                                       int length)
    {
        UnifiedInputStreamX in = makeStream(chars, offset, length);
        IonTextReader r = new IonReaderTextSystemX(system, in);
        return r;
    }

    public static final IonTextReader makeSystemReader(IonSystem system,
                                                       CharSequence chars)
    {
        UnifiedInputStreamX in = makeStream(chars);
        IonTextReader r = new IonReaderTextSystemX(system, in);
        return r;
    }

    public static final IonTextReader makeSystemReader(IonSystem system,
                                                       CharSequence chars,
                                                       int offset,
                                                       int length)
    {
        UnifiedInputStreamX in = makeStream(chars, offset, length);
        IonTextReader r = new IonReaderTextSystemX(system, in);
        return r;
    }

    public static final IonTextReader makeSystemReader(IonSystem system,
                                                       Reader chars)
    {
        UnifiedInputStreamX in;
        try {
            in = makeStream(chars);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        IonTextReader r = new IonReaderTextSystemX(system, in);
        return r;
    }

    public static final IonReader makeSystemReader(IonSystem system,
                                                   IonValue value)
    {
        if (system != null && system != value.getSystem()) {
            throw new IonException("you can't mix values from different systems");
        }
        IonReader r = new IonReaderTreeSystem(value);
        return r;
    }


    // with a system you get a user reader
    public static final IonReader makeReader(IonSystem system, byte[] bytes)
    {
        IonReader r = makeReader(system, system.getCatalog(), bytes, 0, bytes.length);
        return r;
    }

    public static final IonReader makeReader(IonSystem system, byte[] bytes, int offset, int length)
    {
        IonReader r = makeReader(system, system.getCatalog(), bytes, offset, length);
        return r;
    }

    public static final IonReader makeReader(IonSystem system,
                                             IonCatalog catalog,
                                             byte[] bytes,
                                             int offset,
                                             int length)
    {
        UnifiedInputStreamX iis =
            UnifiedInputStreamX.makeStream(bytes, offset, length);
        IonReader r;
        if (isIonBinary(bytes, offset, length)) {
            r = new IonReaderBinaryUserX(system, catalog, iis, offset);
        }
        else {
            r = new IonReaderTextUserX(system, catalog, iis, offset);
        }
        return r;
    }
    public static final IonReader makeReader(IonSystem system, InputStream is)
    {
        IonReader r = makeReader(system, system.getCatalog(), is);
        return r;
    }

    public static final IonReader makeReader(IonSystem system,
                                             IonCatalog catalog,
                                             InputStream is)
    {
        IonReader r;
        UnifiedInputStreamX uis;
        try {
            uis = UnifiedInputStreamX.makeStream(is);
            if (has_binary_cookie(uis)) {
                r = new IonReaderBinaryUserX(system, catalog, uis);
            }
            else {
                r = new IonReaderTextUserX(system, catalog, uis);
            }
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        return r;
    }

    // and on request you can get a system reader when you include a system
    public static final IonReader makeSystemReader(IonSystem system, byte[] bytes)
    {
        IonReader r = makeSystemReader(system, bytes, 0, bytes.length);
        return r;
    }
    public static final IonReader makeSystemReader(IonSystem system, byte[] bytes, int offset, int length)
    {
        UnifiedInputStreamX iis =
            UnifiedInputStreamX.makeStream(bytes, offset, length);
        IonReader r;
        if (isIonBinary(bytes, offset, length)) {
            r = new IonReaderBinarySystemX(system, iis); // FIXME pass offset?
        }
        else {
            r = new IonReaderTextSystemX(system, iis);
        }
        return r;
    }
    public static final IonReader makeSystemReader(IonSystem system, InputStream is)
    {
        IonReader r;
        UnifiedInputStreamX uis;
        try {
            uis = UnifiedInputStreamX.makeStream(is);
            if (has_binary_cookie(uis)) {
                r = new IonReaderBinarySystemX(system, uis);
            }
            else {
                r = new IonReaderTextSystemX(system, uis);
            }
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        return r;
    }

    //
    //  helper functions
    //
    private static final boolean has_binary_cookie(UnifiedInputStreamX uis) throws IOException
    {
        byte[] bytes = new byte[BINARY_VERSION_MARKER_SIZE];

        // try to read the first 4 bytes and unread them (we want
        // the data stream undisturbed by our peeking ahead)
        int len;
        for (len = 0; len < BINARY_VERSION_MARKER_SIZE; len++) {
            int c = uis.read();
            if (c == UnifiedInputStreamX.EOF) {
                break;
            }
            bytes[len] = (byte)c;
        }
        for (int ii=len; ii>0; ) {
            ii--;
            uis.unread(bytes[ii] & 0xff);
        }
        boolean is_cookie = isIonBinary(bytes, 0, len);
        return is_cookie;
    }
}
