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
    extends IonSequenceImpl
    implements IonList
{
    private static final int NULL_LIST_TYPEDESC =
        IonConstants.makeTypeDescriptor(IonConstants.tidList,
                                        IonConstants.lnIsNullSequence);


    /**
     * Constructs a null list value.
     */
    public IonListImpl()
    {
        this(true);
    }

    /**
     * Constructs a null or empty list.
     *
     * @param makeNull indicates whether this should be <code>null.list</code>
     * (if <code>true</code>) or an empty sequence (if <code>false</code>).
     */
    public IonListImpl(boolean makeNull)
    {
        super(NULL_LIST_TYPEDESC, makeNull);
        assert pos_getType() == IonConstants.tidList;
    }

    /**
     * Constructs a non-materialized list backed by a binary buffer.
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
