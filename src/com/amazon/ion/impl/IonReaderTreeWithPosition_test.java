// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonReaderPosition;

import com.amazon.ion.IonReaderWithPosition;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonValue;

/**
 *
 */
public class IonReaderTreeWithPosition_test
    extends IonReaderTreeUserX
    implements IonReaderWithPosition
{
    static class IonReaderTreePosition implements IonReaderPosition
    {
        IonValue _value;
    }

    /**
     * @param value
     * @param catalog
     */
    public IonReaderTreeWithPosition_test(IonValue value, IonCatalog catalog)
    {
        super(value, catalog);
    }

    public IonReaderPosition getCurrentPosition()
    {
        IonReaderTreePosition pos = new IonReaderTreePosition();

        if (this._curr == null && this._eof == false) {
            pos._value = this._parent;
        }
        else {
            pos._value = this._curr;
        }
        return pos;
    }

    public void seek(IonReaderPosition position)
    {
        if (position instanceof IonReaderTreePosition) {
            IonReaderTreePosition pos = (IonReaderTreePosition)position;
            this.re_init(pos._value);
        }
        else {
            throw new IllegalArgumentException("position must match the reader");
        }
    }

}
