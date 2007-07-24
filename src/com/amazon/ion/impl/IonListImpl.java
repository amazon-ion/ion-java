/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonList;
import com.amazon.ion.ValueVisitor;

/**
 * Implements the Ion <code>list</code> type.
 */
public final class IonListImpl
    extends IonValueImpl.list
    implements IonList
{
    
    static final int _list_typeDesc = 
        IonConstants.makeTypeDescriptorByte(
                    IonConstants.tidPosInt
                   ,IonConstants.lnIsNullContainer
       );

    
    /**
     * Constructs a null list value.
     */
    public IonListImpl()
    {
        super(_list_typeDesc);
    }

    /**
     * Constructs a list backed by a binary buffer.
     *
     * @param typeDesc
     */
    public IonListImpl(int typeDesc)
     {
         super(typeDesc);
         assert pos_getType() == IonConstants.tidList;
     }


    public void accept(ValueVisitor visitor) throws Exception
    {
        makeReady();
        visitor.visit(this);
    }
}
