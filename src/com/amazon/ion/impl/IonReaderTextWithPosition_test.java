// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonReaderWithPosition;

import com.amazon.ion.IonReaderPosition;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;

/**
 *
 */
public class IonReaderTextWithPosition_test
    extends IonReaderTextUserX
    implements IonReaderWithPosition
{

    static class IonReaderTextPosition implements IonReaderPosition
    {

    }

    /**
     * @param system
     * @param catalog
     * @param chars
     * @param offset
     * @param length
     */
    protected IonReaderTextWithPosition_test(IonSystem system,
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
    protected IonReaderTextWithPosition_test(IonSystem system,
                                             IonCatalog catalog,
                                             String chars
    ) {
        this(system, catalog, chars.toCharArray(), 0, chars.length());
    }
    public IonReaderPosition getCurrentPosition()
    {
        IonReaderTextPosition pos = new IonReaderTextPosition();



        return pos;
    }
    public void seek(IonReaderPosition position)
    {
        if (position instanceof IonReaderTextPosition) {
            // TODO Auto-generated method stub

        }
        else {
            throw new IllegalArgumentException("position must match the reader");
        }

    }

}
