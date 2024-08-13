// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.lite;

import com.amazon.ion.ContainedValueException;
import com.amazon.ion.IonList;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.ValueVisitor;
import java.util.Collection;


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
    IonListLite(ContainerlessContext context, boolean makeNull)
    {
        super(context, makeNull);
    }

    IonListLite(IonListLite existing, IonContext context)
    {
        super(existing, context);
    }

    /**
     * Constructs a list value
     *
     * @param elements
     *   the initial set of child elements.  If <code>null</code>, then the new
     *   instance will have <code>{@link #isNullValue()} == true</code>.
     *
     * @throws ContainedValueException if any value in <code>elements</code>
     * has <code>{@link IonValue#getContainer()} != null</code>.
     */
    IonListLite(ContainerlessContext context,
                Collection<? extends IonValue> elements)
        throws ContainedValueException
    {
        super(context, elements);
    }

    @Override
    public IonListLite clone() {
        return (IonListLite) deepClone(false);
    }

    @Override
    IonValueLite shallowClone(IonContext context)
    {
        return new IonListLite(this, context);
    }

    @Override
    int hashSignature() {
        return HASH_SIGNATURE;
    }

    @Override
    public IonType getTypeSlow()
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
