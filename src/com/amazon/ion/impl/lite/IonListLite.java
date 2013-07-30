// Copyright (c) 2010-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.ContainedValueException;
import com.amazon.ion.IonException;
import com.amazon.ion.IonList;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.ValueVisitor;
import java.io.IOException;
import java.util.Collection;

/**
 *
 */
final class IonListLite
    extends IonSequenceLite
    implements IonList
{
    private static final int HASH_SIGNATURE =
        IonType.LIST.toString().hashCode();


    /**
     * Constructs a null or empty list.
     *
     * @param makeNull indicates whether this should be <code>null.list</code>
     * (if <code>true</code>) or an empty sequence (if <code>false</code>).
     */
    IonListLite(IonContext context, boolean makeNull)
    {
        super(context, makeNull);
    }

    /**
     * Constructs a list value <em>not</em> backed by binary.
     *
     * @param elements
     *   the initial set of child elements.  If <code>null</code>, then the new
     *   instance will have <code>{@link #isNullValue()} == true</code>.
     *
     * @throws ContainedValueException if any value in <code>elements</code>
     * has <code>{@link IonValue#getContainer()} != null</code>.
     */
    IonListLite(IonContext context,
                Collection<? extends IonValue> elements)
        throws ContainedValueException
    {
        super(context, elements);
    }

    @Override
    public IonListLite clone()
    {
        IonListLite clone = new IonListLite(this._context.getSystem(), false);

        try {
            clone.copyFrom(this);
        } catch (IOException e) {
            throw new IonException(e);
        }

        return clone;
    }

    @Override
    public int hashCode() {
        return sequenceHashCode(HASH_SIGNATURE);
    }

    @Override
    public IonType getType()
    {
        return IonType.LIST;
    }

    @Override
    public void accept(ValueVisitor visitor)
        throws Exception
    {
        visitor.visit(this);
    }
}
