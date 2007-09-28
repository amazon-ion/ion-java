/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonList;
import com.amazon.ion.IonValue;
import com.amazon.ion.LocalSymbolTable;
import com.amazon.ion.ValueVisitor;
import com.amazon.ion.impl.IonBinary.Reader;
import com.amazon.ion.impl.IonBinary.Writer;
import com.amazon.ion.impl.IonSequenceImpl;
import java.io.IOException;
import java.util.ArrayList;

public class TemporaryList
    extends IonSequenceImpl
    implements IonList
{

    ArrayList<IonValueImpl> _tmpelements;

    /**
     * Creates an empty list.
     */
    public TemporaryList() {
        super(IonConstants.makeTypeDescriptor(IonConstants.tidList,
                                              IonConstants.lnNumericZero));
       _tmpelements = new ArrayList<IonValueImpl>();
       setClean();
    }

    @Override
    public void add(IonValue v) {
        add(size(), v, false);
    }

    @Override
    public void add(int index, IonValue v) {
        add(index, v, false);
    }

    @Override
    protected void add(int index, IonValue v, boolean setdirty) {
        this._tmpelements.add((IonValueImpl)v);
    }


    @Override
    public IonValueImpl getFirstChild(LocalSymbolTable ignored)  {
        IonValueImpl value = null;
        if (this._tmpelements.size() > 0) {
            value = get(0);
        }
        return value;
    }

    @Override
    public IonValueImpl get(int ordinal) {
        IonValueImpl value = this._tmpelements.get(ordinal);
        return value;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();

        for (IonValue v : this) {
            sb.append(v.toString());
        }

        return sb.toString();
    }

    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }

    public String[] getTypeAnnotations()
    {
        return null;
    }

    public void setTypeAnnotations(String annotations)
    {
        throw new IonException("unsupported operation");
    }

    @Override
    protected int computeLowNibble(int valuelen)
    {
        throw new IonException("unsupported operation");
    }

    @Override
    public void doMaterializeValue(Reader reader)
        throws IOException
    {
        throw new IonException("unsupported operation");
    }

    @Override
    protected void doWriteNakedValue(Writer writer, int valueLen)
        throws IOException
    {
        throw new IonException("unsupported operation");
    }

    @Override
    protected int getNativeValueLength()
    {
        throw new IonException("unsupported operation");
    }
}