/*
 * Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonNull;
import com.amazon.ion.IonType;
import com.amazon.ion.ValueVisitor;
import java.io.IOException;

/**
 * Implements the Ion <code>null</code> type.
 */
public final class IonNullImpl
    extends IonValueImpl
    implements IonNull
{

    static final int NULL_NULL_TYPEDESC =
        IonConstants.makeTypeDescriptor(IonConstants.tidNull,
                                        IonConstants.lnIsNullAtom);

    /**
     * Constructs a <code>null.null</code> value.
     */
    public IonNullImpl()
    {
        super(NULL_NULL_TYPEDESC);
    }

    /**
     * Constructs a binary-backed null value.
     */
    public IonNullImpl(int typeDesc)
    {
        super(typeDesc);
        if (typeDesc != NULL_NULL_TYPEDESC)
        {
            throw new IonException("Invalid type descriptor byte " + typeDesc);
        }
        assert pos_getType() == IonConstants.tidNull;
    }

    /**
     * makes a copy of this IonNull which, as it has no
     * value (as such), is immutable.
     * This calls IonValueImpl to copy the annotations and the
     * field name if appropriate.  The symbol table is not
     * copied as the value is fully materialized and the symbol
     * table is unnecessary.
     */
    @Override
    public IonNullImpl clone()
    {
        IonNullImpl clone = new IonNullImpl();

        makeReady();
        clone.copyAnnotationsAndFieldNameFrom(this);

        return clone;
    }


    public IonType getType()
    {
        return IonType.NULL;
    }


    @Override
    public final boolean isNullValue()
    {
        return true;
    }


    @Override
    protected int getNativeValueLength()
    {
        return 0;
    }

    @Override
    protected int computeLowNibble(int valuelen)
    {
        return IonConstants.lnIsNullAtom;
    }


    @Override
    protected void doMaterializeValue(IonBinary.Reader reader)
    {
        assert this._isPositionLoaded == true && this._buffer != null;

        // a native value trumps a buffered value
        if (_hasNativeValue) return;

        // the reader will have been positioned for us
        assert reader.position() == this.pos_getOffsetAtValueTD();
        assert this.pos_getType() == IonConstants.tidNull;

        _hasNativeValue = true;
    }


    @Override
    protected void doWriteNakedValue(IonBinary.Writer writer, int valueLen) throws IOException
    {
        throw new IonException("call not needed!");
    }


    public void accept(ValueVisitor visitor)
        throws Exception
    {
        visitor.visit(this);
    }
}
