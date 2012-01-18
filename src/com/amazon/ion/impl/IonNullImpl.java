// Copyright (c) 2007-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl._Private_IonConstants.lnIsNullAtom;
import static com.amazon.ion.impl._Private_IonConstants.makeTypeDescriptor;
import static com.amazon.ion.impl._Private_IonConstants.tidNull;

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
        makeTypeDescriptor(tidNull, lnIsNullAtom);

    private static final int HASH_SIGNATURE =
        IonType.NULL.toString().hashCode();

    /**
     * Constructs a <code>null.null</code> value.
     */
    public IonNullImpl(IonSystemImpl system)
    {
        super(system, NULL_NULL_TYPEDESC);
        _hasNativeValue(true);
    }

    /**
     * Constructs a binary-backed null value.
     */
    public IonNullImpl(IonSystemImpl system, int typeDesc)
    {
        super(system, typeDesc);
        _hasNativeValue(true);

        // This is necessary to trap badly-encoded data.
        // FIXME don't error in this case.
        if (typeDesc != NULL_NULL_TYPEDESC)
        {
            throw new IonException("Invalid type descriptor byte " + typeDesc);
        }
        assert pos_getType() == _Private_IonConstants.tidNull;
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
        IonNullImpl clone = new IonNullImpl(_system);

        makeReady();
        clone.copyAnnotationsFrom(this);

        return clone;
    }

    /**
     * Implements {@link Object#hashCode()} consistent with equals.
     *
     * @return  An int, consistent with the contracts for
     *          {@link Object#hashCode()} and {@link Object#equals(Object)}.
     */
    @Override
    public int hashCode() {
        return HASH_SIGNATURE;
    }

    public IonType getType()
    {
        return IonType.NULL;
    }


    //@Override
    //public final boolean isNullValue()
    //{
    //    assert(this._isNullValue() == true);
    //    return true;
    //}


    @Override
    protected int getNativeValueLength()
    {
        return 0;
    }

    @Override
    protected int computeLowNibble(int valuelen)
    {
        return _Private_IonConstants.lnIsNullAtom;
    }


    @Override
    protected void doMaterializeValue(IonBinary.Reader reader)
    {
        assert this._isPositionLoaded() == true && this._buffer != null;

        // a native value trumps a buffered value
        if (_hasNativeValue()) return;

        // the reader will have been positioned for us
        assert reader.position() == this.pos_getOffsetAtValueTD();
        assert this.pos_getType() == _Private_IonConstants.tidNull;

        _hasNativeValue(true);
    }


    @Override
    protected void doWriteNakedValue(IonBinary.Writer writer, int valueLen) throws IOException
    {
        throw new UnsupportedOperationException("should not be called");
    }


    public void accept(ValueVisitor visitor)
        throws Exception
    {
        visitor.visit(this);
    }
}
