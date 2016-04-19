/*
 * Copyright (c) 2007-2013 Amazon.com, Inc.  All rights reserved.
 */

package software.amazon.ion.util;

import software.amazon.ion.IonBlob;
import software.amazon.ion.IonBool;
import software.amazon.ion.IonClob;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonDecimal;
import software.amazon.ion.IonFloat;
import software.amazon.ion.IonInt;
import software.amazon.ion.IonList;
import software.amazon.ion.IonNull;
import software.amazon.ion.IonSexp;
import software.amazon.ion.IonString;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonSymbol;
import software.amazon.ion.IonTimestamp;
import software.amazon.ion.IonValue;
import software.amazon.ion.ValueVisitor;

/**
 * A base class for extending Ion {@link ValueVisitor}s.
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
