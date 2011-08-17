// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;
import com.amazon.ion.Span;

/**
 *
 */
public class IonReaderTextWithPosition
    extends IonReaderTextUserX
    implements IonReaderWithPosition
{
    // FIXME implement this
    static class IonReaderTextPosition extends IonReaderPositionBase {}

    /**
     * @param system
     * @param catalog
     * @param chars
     * @param offset
     * @param length
     */
    protected IonReaderTextWithPosition(IonSystem system,
                                             IonCatalog catalog,
                                             char[] chars,
                                             int offset,
                                             int length)
    {
        super(system, catalog, chars, offset, length);
    }

    /**
     * @param system
     * @param catalog
     */
    protected IonReaderTextWithPosition(IonSystem system,
                                        IonCatalog catalog,
                                        String chars)
    {
        this(system, catalog, chars.toCharArray(), 0, chars.length());
    }

    public IonReaderPosition getCurrentPosition()
    {
        IonReaderTextPosition pos = new IonReaderTextPosition();



        return pos;
    }

    public void seek(IonReaderPosition position)
    {
        if (!(position instanceof IonReaderTextPosition)) {
            throw new IllegalArgumentException("position must match the reader");
        }
        // FIXME implement this
        throw new UnsupportedOperationException("Seek not currently implemented on text reader");
    }

    public Span currentSpan()
    {
        throw new UnsupportedOperationException();
    }

    public void hoist(Span span)
    {
        throw new UnsupportedOperationException();
    }
}
