// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonValue;
import com.amazon.ion.Span;

/**
 *
 */
public class IonReaderTreeWithPosition
    extends IonReaderTreeUserX
    implements IonReaderWithPosition
{
    static class IonReaderTreePosition extends IonReaderPositionBase
    {
        IonValue _value;
    }

    /**
     * @param value
     * @param catalog
     */
    public IonReaderTreeWithPosition(IonValue value, IonCatalog catalog)
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
        hoist(position);
    }


    public Span currentSpan()
    {
        // FIXME implementation not to spec
        return getCurrentPosition();
    }

    public void hoist(Span span)
    {
        if (span instanceof IonReaderTreePosition) {
            IonReaderTreePosition pos = (IonReaderTreePosition)span;
            this.re_init(pos._value);
        }
        else {
            // TODO custom exception
            throw new IllegalArgumentException("Span not appropriate for this reader");
        }
    }
}
