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

import static com.amazon.ion.impl.UnifiedInputStreamX.makeStream;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTextReader;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.util.IonStreamUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.zip.GZIPInputStream;

/**
 * NOT FOR APPLICATION USE!
 */
@SuppressWarnings("deprecation")
public final class _Private_IonReaderFactory
{

    public static IonReader makeSystemReader(byte[] bytes)
    {
        return makeSystemReader(bytes, 0, bytes.length);
    }


    public static final IonReader makeReaderText(IonCatalog catalog,
                                                 byte[] bytes,
                                                 int offset,
                                                 int length,
                                                 _Private_LocalSymbolTableFactory lstFactory)
    {
        UnifiedInputStreamX uis;
        try
        {
            uis = makeUnifiedStream(bytes, offset, length);
        }
        catch (IOException e)
        {
            throw new IonException(e);
        }
        return new IonReaderTextUserX(catalog, lstFactory, uis, offset);
    }

    public static IonReader makeSystemReader(byte[] bytes,
                                             int offset,
                                             int length)
    {
        return _Private_IonReaderBuilder.buildReader(
            (_Private_IonReaderBuilder) _Private_IonReaderBuilder.standard(),
            bytes,
            offset,
            length,
            _Private_IonReaderFactory::makeSystemReaderBinary,
            _Private_IonReaderFactory::makeSystemReaderText
        );
    }

    public static final IonTextReader makeReaderText(IonCatalog catalog,
                                                     CharSequence chars,
                                                     _Private_LocalSymbolTableFactory lstFactory)
    {
        UnifiedInputStreamX in = makeStream(chars);
        return new IonReaderTextUserX(catalog, lstFactory, in);
    }

    public static final IonReader makeSystemReaderText(CharSequence chars)
    {
        UnifiedInputStreamX in = makeStream(chars);
        return new IonReaderTextSystemX(in);
    }

    public static final IonReader makeReaderText(IonCatalog catalog,
                                                 InputStream is,
                                                 _Private_LocalSymbolTableFactory lstFactory)
    {
        UnifiedInputStreamX uis;
        try {
            uis = makeUnifiedStream(is);
        } catch (IOException e) {
            throw new IonException(e);
        }
        return new IonReaderTextUserX(catalog, lstFactory, uis, 0);
    }

    public static IonReader makeSystemReaderText(InputStream is)
    {
        return _Private_IonReaderBuilder.buildReader(
            (_Private_IonReaderBuilder) _Private_IonReaderBuilder.standard(),
            is,
            _Private_IonReaderFactory::makeSystemReaderBinary,
            _Private_IonReaderFactory::makeSystemReaderText
        );
    }

    private static IonReader makeSystemReaderText(IonCatalog catalog,
                                                  InputStream is,
                                                  _Private_LocalSymbolTableFactory lstFactory)
    {
        UnifiedInputStreamX uis;
        try
        {
            uis = makeUnifiedStream(is);
        }
        catch (IOException e)
        {
            throw new IonException(e);
        }
        return new IonReaderTextSystemX(uis);
    }

    private static IonReader makeSystemReaderText(IonCatalog catalog,
                                                  byte[] bytes,
                                                  int offset,
                                                  int length,
                                                  _Private_LocalSymbolTableFactory lstFactory) {
        UnifiedInputStreamX uis;
        try
        {
            uis = makeUnifiedStream(bytes, offset, length);
        }
        catch (IOException e)
        {
            throw new IonException(e);
        }
        return new IonReaderTextSystemX(uis);
    }

    public static final IonTextReader makeReaderText(IonCatalog catalog,
                                                     Reader chars,
                                                     _Private_LocalSymbolTableFactory lstFactory)
    {
        try {
            UnifiedInputStreamX in = makeStream(chars);
            return new IonReaderTextUserX(catalog, lstFactory, in);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
    }

    public static final IonReader makeSystemReaderText(Reader chars)
    {
        try {
            UnifiedInputStreamX in = makeStream(chars);
            return new IonReaderTextSystemX(in);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
    }

    public static final IonReader makeReader(IonCatalog catalog,
                                             IonValue value,
                                             _Private_LocalSymbolTableFactory lstFactory)
    {
        return new IonReaderTreeUserX(value, catalog, lstFactory);
    }

    public static final IonReader makeSystemReaderText(IonSystem system,
                                                       IonValue value)
    {
        if (system != null && system != value.getSystem()) {
            throw new IonException("you can't mix values from different systems");
        }
        return new IonReaderTreeSystem(value);
    }

    public static final IonReader makeReaderBinary(IonReaderBuilder builder, InputStream is, byte[] alreadyRead, int alreadyReadOff, int alreadyReadLen)
    {
        return new IonReaderContinuableTopLevelBinary(builder, is, alreadyRead, alreadyReadOff, alreadyReadLen);
    }

    public static final IonReader makeSystemReaderBinary(IonReaderBuilder builder, InputStream is, byte[] alreadyRead, int alreadyReadOff, int alreadyReadLen)
    {
        return new IonReaderNonContinuableSystem(
            new IonReaderContinuableCoreBinary(builder.getBufferConfiguration(), is, alreadyRead, alreadyReadOff, alreadyReadLen)
        );
    }

    public static final IonReader makeReaderBinary(IonReaderBuilder builder, byte[] buffer, int off, int len)
    {
        return new IonReaderContinuableTopLevelBinary(builder, buffer, off, len);
    }

    public static final IonReader makeSystemReaderBinary(IonReaderBuilder builder, byte[] buffer, int off, int len)
    {
        return new IonReaderNonContinuableSystem(
            new IonReaderContinuableCoreBinary(builder.getBufferConfiguration(), buffer, off, len)
        );
    }


    //=========================================================================
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
}
