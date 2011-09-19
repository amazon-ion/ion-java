// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.facet.Facets.assumeFacet;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.SeekableReader;
import com.amazon.ion.Span;
import com.amazon.ion.SpanProvider;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.system.IonSystemBuilder;

/**
 * Temporary interface to some in-development features.
 * Support ONLY for use via the DM-SDK 3 library.
 *
 * @deprecated Since R13.
 */
@Deprecated
public class PrivateDmsdkUtils  // TODO ION-252 Remove this
{
    /**
     * @deprecated Since R13.
     */
    @Deprecated
    public static IonReader newBinaryReaderWithPosition(IonSystem system,
                                                        byte[] buffer)
    {
        IonReader r = system.newReader(buffer);
        if (r.asFacet(SeekableReader.class) == null)
        {
            throw new UnsupportedOperationException("reader isn't seekable");

        }
        return r;
    }

    /**
     * @deprecated Since R13.
     */
    @Deprecated
    public static IonReader newBinaryReaderWithPosition(IonSystem system,
                                                        byte[] buffer,
                                                        int offset, int length)
    {
        IonReader r = system.newReader(buffer, offset, length);
        if (r.asFacet(SeekableReader.class) == null)
        {
            throw new UnsupportedOperationException("reader isn't seekable");

        }
        return r;
    }

    /**
     * @deprecated Since R13.
     */
    @Deprecated
    public static IonReader newBinaryReaderWithPosition(IonSystem system,
                                                        IonCatalog catalog,
                                                        byte[] buffer,
                                                        int offset, int length)
    {
        IonReader r = new IonReaderBinaryUserX(system, catalog,
                                               buffer, offset, length);
        if (r.asFacet(SeekableReader.class) == null)
        {
            throw new UnsupportedOperationException("reader isn't seekable");

        }
        return r;
    }


    /**
     *
     * @param reader must have been created via
     * {@link #newBinaryReaderWithPosition}.
     *
     * @return an opaque position marker.
     *
     * @throws IonException if reader is null or doesn't have the
     * {@link SpanProvider} facet.
     *
     * @deprecated Since R13.
     */
    @Deprecated
    public static Object currentValuePosition(IonReader reader)
    {
        return assumeFacet(SpanProvider.class, reader).currentSpan();
    }


    /**
     * Prepares the given reader to re-read a single value, as represented by
     * the given position.  The caller should not call {@link IonReader#next()}
     * afterwards; that will always return null.
     *
     * @param reader must have been created via
     * {@link #newBinaryReaderWithPosition}.
     * @param valuePosition must have been created via
     * {@link #currentValuePosition(IonReader)}.
     *
     * @deprecated Since R13.
     */
    @Deprecated
    public static void rereadValue(IonReader reader, Object valuePosition)
    {
        Span position = (Span) valuePosition;
        assumeFacet(SeekableReader.class, reader).hoist(position);
        reader.next();
    }

    /**
     * @deprecated Since R13.
     *  Use {@link IonSystemBuilder#isStreamCopyOptimized()}.
     */
    @Deprecated
    public static boolean isFastCopyEnabled()
    {
        return IonWriterUserBinary.ourFastCopyEnabled;
    }

    /**
     * @deprecated Since R13.
     *  Use {@link IonSystemBuilder#setStreamCopyOptimized(boolean)}.
     */
    @Deprecated
    public static void setFastCopyEnabled(boolean enable)
    {
        IonWriterUserBinary.ourFastCopyEnabled = enable;
    }


    /**
     * @deprecated Since R13. Use {@link SymbolTable#makeReadOnly()}.
     */
    @Deprecated
    public static void lockLocalSymbolTable(SymbolTable symtab)
    {
        symtab.makeReadOnly();
    }
}
