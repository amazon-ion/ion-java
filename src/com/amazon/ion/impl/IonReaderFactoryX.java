// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
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
public class IonReaderFactoryX
{
    //
    // characters sources must be text input
    //
    // without a system you can only get a system reader
    public static final IonReader makeReader(char[] chars) {
        IonReader r = new IonReaderTextSystemX(chars);
        return r;
    }
    public static final IonReader makeReader(char[] chars, int offset, int length) {
        IonReader r = new IonReaderTextSystemX(chars, offset, length);
        return r;
    }
    public static final IonReader makeReader(CharSequence chars) {
        IonReader r = new IonReaderTextSystemX(chars);
        return r;
    }
    public static final IonReader makeReader(CharSequence chars, int offset, int length) {
        IonReader r = new IonReaderTextSystemX(chars, offset, length);
        return r;
    }
    public static final IonReader makeReader(Reader chars) {
        IonReader r = new IonReaderTextSystemX(chars);
        return r;
    }
    // with a system you get a user reader
    public static final IonReader makeReader(IonSystem system, char[] chars) {
        IonReader r = new IonReaderTextUserX(system, chars);
        return r;
    }
    public static final IonReader makeReader(IonSystem system, char[] chars, int offset, int length) {
        IonReader r = new IonReaderTextUserX(system, chars, offset, length);
        return r;
    }
    public static final IonReader makeReader(IonSystem system, CharSequence chars) {
        IonReader r = new IonReaderTextUserX(system, chars);
        return r;
    }
    public static final IonReader makeReader(IonSystem system, CharSequence chars, int offset, int length) {
        IonReader r = new IonReaderTextUserX(system, chars, offset, length);
        return r;
    }
    public static final IonReader makeReader(IonSystem system, Reader chars) {
        IonReader r = new IonReaderTextUserX(system, chars);
        return r;
    }

    //
    // for bytes sources we have to check the cookie
    //
    // with a system you get a user reader
    private static final boolean has_binary_cookie(UnifiedInputStreamX uis) throws IOException
    {
        byte[] bytes = new byte[IonConstants.BINARY_VERSION_MARKER_SIZE];

        // try to read the first 4 bytes and unread them (we want
        // the data stream undisturbed by our peeking ahead)
        int len;
        for (len = 0; len < IonConstants.BINARY_VERSION_MARKER_SIZE; len++) {
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
        boolean is_cookie = has_binary_cookie(bytes, 0 , len);
        return is_cookie;
    }
    private static final boolean has_binary_cookie(byte[] bytes, int offset, int length)
    {
        if (length < IonConstants.BINARY_VERSION_MARKER_SIZE) {
            return false;
        }
        for (int ii=0; ii<IonConstants.BINARY_VERSION_MARKER_SIZE; ii++) {
            if (bytes[offset + ii] != IonConstants.BINARY_VERSION_MARKER_1_0[ii]) {
                return false;
            }
        }
        return true;
    }

    public static final IonReader makeReader(byte[] bytes)
    {
        IonReader r = makeReader(bytes, 0, bytes.length);
        return r;
    }
    public static final IonReader makeReader(byte[] bytes, int offset, int length)
    {
        IonReader r;
        if (has_binary_cookie(bytes, offset, length)) {
            r = new IonReaderBinarySystemX(bytes, offset, length);
        }
        else {
            r = new IonReaderTextSystemX(bytes, offset, length);
        }
        return r;
    }
    public static final IonReader makeReader(InputStream is)
    {
        IonReader r;
        UnifiedInputStreamX uis;
        try {
            uis = UnifiedInputStreamX.makeStream(is);
            if (has_binary_cookie(uis)) {
                r = new IonReaderBinarySystemX(uis);
            }
            else {
                r = new IonReaderTextSystemX(uis);
            }
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        return r;
    }
    // with a system you get a user reader
    public static final IonReader makeReader(IonSystem system, byte[] bytes)
    {
        IonReader r = makeReader(system, bytes, 0, bytes.length);
        return r;
    }
    public static final IonReader makeReader(IonSystem system, byte[] bytes, int offset, int length)
    {
        IonReader r;
        if (has_binary_cookie(bytes, offset, length)) {
            r = new IonReaderBinaryUserX(system, bytes, offset, length);
        }
        else {
            r = new IonReaderTextUserX(system, bytes, offset, length);
        }
        return r;
    }
    public static final IonReader makeReader(IonSystem system, InputStream is)
    {
        IonReader r;
        UnifiedInputStreamX uis;
        try {
            uis = UnifiedInputStreamX.makeStream(is);
            if (has_binary_cookie(uis)) {
                r = new IonReaderBinaryUserX(system, uis);
            }
            else {
                r = new IonReaderTextUserX(system, uis);
            }
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        return r;
    }
}
