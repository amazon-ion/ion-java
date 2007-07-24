/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.util;

import com.amazon.ion.IonBlob;
import com.amazon.ion.IonBool;
import com.amazon.ion.IonClob;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonDecimal;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonList;
import com.amazon.ion.IonNull;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonValue;
import com.amazon.ion.ValueVisitor;

/**
 * A base class for creating Ion {@link ValueVisitor}s.
 * All <code>visit</code> methods are implemented to call
 * {@link #defaultVisit(IonValue)}.
 */
public abstract class AbstractValueVisitor
    implements ValueVisitor
{

    /**
     * Default visitation behavior, called by all <code>visit</code> methods
     * in {@link AbstractValueVisitor}.  Subclasses should override this unless
     * they override all <code>visit</code> methods.
     * <p>
     * This implementation always throws {@link UnsupportedOperationException}.
     *
     * @param value the value to visit.
     * @throws UnsupportedOperationException always thrown unless subclass
     * overrides this implementation.
     * @throws Exception subclasses can throw this; it will be propagated by
     * the other <code>visit</code> methods.
     */
    protected void defaultVisit(IonValue value) throws Exception
    {
        throw new UnsupportedOperationException();
    }

    public void visit(IonBlob value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonBool value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonClob value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonDatagram value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonDecimal value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonFloat value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonInt value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonList value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonNull value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonSexp value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonString value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonStruct value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonSymbol value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonTimestamp value) throws Exception
    {
        defaultVisit(value);
    }
}
