/*
 * Copyright 2009-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion.impl;

import static software.amazon.ion.impl.PrivateIonConstants.BINARY_VERSION_MARKER_SIZE;
import static software.amazon.ion.impl.UnifiedInputStreamX.makeStream;
import static software.amazon.ion.util.IonStreamUtils.isIonBinary;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.zip.GZIPInputStream;
import software.amazon.ion.IonCatalog;
import software.amazon.ion.IonException;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonValue;
import software.amazon.ion.util.IonStreamUtils;

/**
 * @deprecated This is an internal API that is subject to change without notice.
 */
@Deprecated
public final class PrivateIonReaderFactory
{
    public static final IonReader makeReader(IonSystem system,
                                             IonCatalog catalog,
                                             byte[] bytes)
    {
        return makeReader(system, catalog, bytes, 0, bytes.length);
    }

    public static IonReader makeSystemReader(IonSystem system, byte[] bytes)
    {
        return makeSystemReader(system, bytes, 0, bytes.length);
    }


    public static final IonReader makeReader(IonSystem system,
                                             IonCatalog catalog,
                                             byte[] bytes,
                                             int offset,
                                             int length)
    {
        try
        {
            UnifiedInputStreamX uis = makeUnifiedStream(bytes, offset, length);
            return makeReader(system, catalog, uis, offset);
        }
        catch (IOException e)
        {
            throw new IonException(e);
        }
    }

    public static IonReader makeSystemReader(IonSystem system,
                                             byte[] bytes,
                                             int offset,
                                             int length)
    {
        try
        {
            UnifiedInputStreamX uis = makeUnifiedStream(bytes, offset, length);
            return makeSystemReader(system, uis, offset);
        }
        catch (IOException e)
        {
            throw new IonException(e);
        }
    }


    public static final IonReader makeReader(IonSystem system,
                                                 IonCatalog catalog,
                                                 char[] chars)
    {
        return makeReader(system, catalog, chars, 0, chars.length);
    }

    public static final IonReader makeSystemReader(IonSystem system,
                                                   char[] chars)
    {
        UnifiedInputStreamX in = makeStream(chars);
        return new IonReaderTextSystemX(system, in);
    }


    public static final IonReader makeReader(IonSystem system,
                                             IonCatalog catalog,
                                             char[] chars,
                                             int offset,
                                             int length)
    {
        UnifiedInputStreamX in = makeStream(chars, offset, length);
        return new IonReaderTextUserX(system, catalog, in, offset);
    }

    public static final IonReader makeSystemReader(IonSystem system,
                                                   char[] chars,
                                                   int offset,
                                                   int length)
    {
        UnifiedInputStreamX in = makeStream(chars, offset, length);
        return new IonReaderTextSystemX(system, in);
    }


    public static final IonReader makeReader(IonSystem system,
                                                 IonCatalog catalog,
                                                 CharSequence chars)
    {
        UnifiedInputStreamX in = makeStream(chars);
        return new IonReaderTextUserX(system, catalog, in);
    }

    public static final IonReader makeSystemReader(IonSystem system,
                                                   CharSequence chars)
    {
        UnifiedInputStreamX in = makeStream(chars);
        return new IonReaderTextSystemX(system, in);
    }


    public static final IonReader makeReader(IonSystem system,
                                             IonCatalog catalog,
                                             CharSequence chars,
                                             int offset,
                                             int length)
    {
        UnifiedInputStreamX in = makeStream(chars, offset, length);
        return new IonReaderTextUserX(system, catalog, in, offset);
    }

    public static final IonReader makeSystemReader(IonSystem system,
                                                   CharSequence chars,
                                                   int offset,
                                                   int length)
    {
        UnifiedInputStreamX in = makeStream(chars, offset, length);
        return new IonReaderTextSystemX(system, in);
    }


    public static final IonReader makeReader(IonSystem system,
                                             IonCatalog catalog,
                                             InputStream is)
    {
        try {
            UnifiedInputStreamX uis = makeUnifiedStream(is);
            return makeReader(system, catalog, uis, 0);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
    }

    public static IonReader makeSystemReader(IonSystem system,
                                             InputStream is)
    {
        try {
            UnifiedInputStreamX uis = makeUnifiedStream(is);
            return makeSystemReader(system, uis, 0);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
    }


    public static final IonReader makeReader(IonSystem system,
                                             IonCatalog catalog,
                                             Reader chars)
    {
        try {
            UnifiedInputStreamX in = makeStream(chars);
            return new IonReaderTextUserX(system, catalog, in);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
    }

    public static final IonReader makeSystemReader(IonSystem system,
                                                       Reader chars)
    {
        try {
            UnifiedInputStreamX in = makeStream(chars);
            return new IonReaderTextSystemX(system, in);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
    }


    public static final IonReader makeReader(IonSystem system,
                                             IonCatalog catalog,
                                             IonValue value)
    {
        return new IonReaderTreeUserX(value, catalog);
    }

    public static final IonReader makeSystemReader(IonSystem system,
                                                   IonValue value)
    {
        if (system != null && system != value.getSystem()) {
            throw new IonException("you can't mix values from different systems");
        }
        return new IonReaderTreeSystem(value);
    }


    //=========================================================================



    private static IonReader makeReader(IonSystem system,
                                        IonCatalog catalog,
                                        UnifiedInputStreamX uis,
                                        int offset)
        throws IOException
    {
        IonReader r;
        if (has_binary_cookie(uis)) {
            r = new IonReaderBinaryUserX(system, catalog, uis, offset);
        }
        else {
            r = new IonReaderTextUserX(system, catalog, uis, offset);
        }
        return r;
    }

    private static IonReader makeSystemReader(IonSystem system,
                                              UnifiedInputStreamX uis,
                                              int offset)
        throws IOException
    {
        IonReader r;
        if (has_binary_cookie(uis)) {
            // TODO pass offset, or spans will be incorrect
            r = new IonReaderBinarySystemX(system, uis);
        }
        else {
            // TODO pass offset, or spans will be incorrect
            r = new IonReaderTextSystemX(system, uis);
        }
        return r;
    }

    //
    //  helper functions
    //

    private static UnifiedInputStreamX makeUnifiedStream(byte[] bytes,
                                                         int offset,
                                                         int length)
        throws IOException
    {
        UnifiedInputStreamX uis;
        if (IonStreamUtils.isGzip(bytes, offset, length))
        {
            ByteArrayInputStream baos =
                new ByteArrayInputStream(bytes, offset, length);
            GZIPInputStream gzip = new GZIPInputStream(baos);
            uis = UnifiedInputStreamX.makeStream(gzip);
        }
        else
        {
            uis = UnifiedInputStreamX.makeStream(bytes, offset, length);
        }
        return uis;
    }


    private static UnifiedInputStreamX makeUnifiedStream(InputStream in)
        throws IOException
    {
        in.getClass(); // Force NPE

        // TODO avoid multiple wrapping streams, use the UIS for the pushback
        in = IonStreamUtils.unGzip(in);
        UnifiedInputStreamX uis = UnifiedInputStreamX.makeStream(in);
        return uis;
    }


    private static final boolean has_binary_cookie(UnifiedInputStreamX uis)
        throws IOException
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
