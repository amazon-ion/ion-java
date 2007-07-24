/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import java.io.IOException;

import com.amazon.ion.IonException;
import com.amazon.ion.IonNull;
import com.amazon.ion.ValueVisitor;

/**
 * Implements the Ion <code>null</code> type.
 */
public final class IonNullImpl
    extends IonValueImpl
    implements IonNull
{
    
    static final int _null_typeDesc = 
        IonConstants.makeTypeDescriptorByte(
                    IonConstants.tidNull
                   ,IonConstants.lnIsNullAtom
       );

    /**
     * Constructs a <code>null.null</code> value.
     */
    public IonNullImpl()
    {
        super(_null_typeDesc);
    }

    /**
     * Constructs a binary-backed null value.
     */
    public IonNullImpl(int typeDesc)
    {
        super(typeDesc);
        assert pos_getType() == IonConstants.tidNull;
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
