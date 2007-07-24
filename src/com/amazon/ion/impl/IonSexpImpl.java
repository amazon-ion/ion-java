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
    extends IonValueImpl.list
    implements IonSexp
{
    
    static final int _sexp_typeDesc = IonConstants.makeTypeDescriptorByte(
                                                         IonConstants.tidSexp
                                                        ,IonConstants.lnIsNullContainer
                                                    );
    /**
     * Constructs a <code>null.sexp</code> value.
     */
    public IonSexpImpl()
    {
        this(_sexp_typeDesc, true);
    }

    public IonSexpImpl(int typeDesc)
    {
        super(typeDesc, true);
        assert pos_getType() == IonConstants.tidSexp;
    }
    
    /**
     * Constructs a null or empty S-expression.
     *
     * @param makeNull indicates whether this should be <code>null.sexp</code>
     * (if <code>true</code>) or an empty sequence (if <code>false</code>).
     */
    public IonSexpImpl(int typeDesc, boolean makeNull)
    {
        super(_sexp_typeDesc, makeNull);
        assert pos_getType() == IonConstants.tidSexp;
    } 

    public void accept(ValueVisitor visitor) throws Exception
    {
        makeReady();
        visitor.visit(this);
    }
}
