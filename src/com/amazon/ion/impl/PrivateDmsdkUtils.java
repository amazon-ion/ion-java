// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.Facets.assumeFacet;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.Span;
import com.amazon.ion.SpanReader;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.util.IonStreamUtils;

/**
 * Temporary interface to some in-development features.
 * Support ONLY for use via the DM-SDK 3 library.
 */
public class PrivateDmsdkUtils
{
    public static IonReader newBinaryReaderWithPosition(IonSystem system,
                                                        byte[] buffer)
    {
        return newBinaryReaderWithPosition(system, system.getCatalog(),
                                           buffer, 0, buffer.length);
    }

    public static IonReader newBinaryReaderWithPosition(IonSystem system,
                                                        byte[] buffer,
                                                        int offset, int length)
    {
        return newBinaryReaderWithPosition(system, system.getCatalog(),
                                           buffer, offset, length);
    }

    public static IonReader newBinaryReaderWithPosition(IonSystem system,
                                                        IonCatalog catalog,
                                                        byte[] buffer,
                                                        int offset, int length)
    {
        if (! IonStreamUtils.isIonBinary(buffer, offset, length))
        {
            throw new UnsupportedOperationException("buffer isn't Ion binary");
        }

        return new IonReaderBinaryUserX(system, catalog,
                                               buffer, offset, length);
    }


    /**
     *
     * @param reader must have been created via
     * {@link #newBinaryReaderWithPosition}.
     *
     * @return an opaque position marker.
     *
     * @throws IonException if reader is null or doesn't have the
     * {@link SpanReader} facet.
     */
    public static Object currentValuePosition(IonReader reader)
    {
        return assumeFacet(SpanReader.class, reader).currentSpan();
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
     */
    public static void rereadValue(IonReader reader, Object valuePosition)
    {
        Span position = (Span) valuePosition;
        assumeFacet(SpanReader.class, reader).hoist(position);
        reader.next();
    }


    public static boolean isFastCopyEnabled()
    {
        return IonWriterUserBinary.ourFastCopyEnabled;
    }

    public static void setFastCopyEnabled(boolean enable)
    {
        IonWriterUserBinary.ourFastCopyEnabled = enable;
    }


    public static void lockLocalSymbolTable(SymbolTable symtab)
    {
        ((UnifiedSymbolTable)symtab).makeReadOnly();
    }
}
