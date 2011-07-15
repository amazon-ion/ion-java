// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
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

        return new IonReaderBinaryWithPosition(system, catalog,
                                               buffer, offset, length);
    }


    /**
     *
     * @param reader must have been created via
     * {@link #newBinaryReaderWithPosition}.
     *
     * @return an opaque position marker.
     */
    public static Object currentValuePosition(IonReader reader)
    {
        return ((IonReaderBinaryWithPosition)reader).getCurrentPosition();
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
        IonReaderPosition position = (IonReaderPosition) valuePosition;
        ((IonReaderBinaryWithPosition)reader).seek(position);
        reader.next();
    }
}
