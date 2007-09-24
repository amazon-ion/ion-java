/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonSexp;
import com.amazon.ion.ValueVisitor;

/**
 * Implements the Ion <code>sexp</code> (S-expression) type.
 */
public class IonSexpImpl
    extends IonSequenceImpl
    implements IonSexp
{
    private static final int NULL_SEXP_TYPEDESC =
        IonConstants.makeTypeDescriptorByte(IonConstants.tidSexp,
                                            IonConstants.lnIsNullSequence);


    /**
     * Constructs a <code>null.sexp</code> value.
     */
    public IonSexpImpl()
    {
        this(true);
    }

    /**
     * Constructs a null or empty S-expression.
     *
     * @param makeNull indicates whether this should be <code>null.sexp</code>
     * (if <code>true</code>) or an empty sequence (if <code>false</code>).
     */
    public IonSexpImpl(boolean makeNull)
    {
        super(NULL_SEXP_TYPEDESC, makeNull);
        assert pos_getType() == IonConstants.tidSexp;
    }


    /**
     * Constructs a non-materialized sexp backed by a binary buffer.
     */
    public IonSexpImpl(int typeDesc)
    {
        super(typeDesc, true);
        assert pos_getType() == IonConstants.tidSexp;
    }

    public void accept(ValueVisitor visitor) throws Exception
    {
        makeReady();
        visitor.visit(this);
    }
}
