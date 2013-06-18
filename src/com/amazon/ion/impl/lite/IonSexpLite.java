// Copyright (c) 2010-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.ContainedValueException;
import com.amazon.ion.IonException;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.ValueVisitor;
import java.io.IOException;
import java.util.Collection;

/**
 *
 */
final class IonSexpLite
    extends IonSequenceLite
    implements IonSexp
{
    private static final int HASH_SIGNATURE =
        IonType.SEXP.toString().hashCode();

    IonSexpLite(IonContext context, boolean isNull)
    {
        super(context, isNull);
    }

    /**
     * Constructs a sexp value <em>not</em> backed by binary.
     *
     * @param elements
     *   the initial set of child elements.  If <code>null</code>, then the new
     *   instance will have <code>{@link #isNullValue()} == true</code>.
     *
     * @throws ContainedValueException if any value in <code>elements</code>
     * has <code>{@link IonValue#getContainer()} != null</code>.
     */
    IonSexpLite(IonContext context,
                Collection<? extends IonValue> elements)
        throws ContainedValueException
    {
        super(context, elements);
    }


    /**
     * creates a copy of this IonSexpImpl.  Most of the work
     * is actually done by IonContainerImpl.copyFrom() and
     * IonValueImpl.copyFrom().
     */
    @Override
    public IonSexpLite clone()
    {
        IonSexpLite clone = new IonSexpLite(_context.getSystem(), false);

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
        return IonType.SEXP;
    }

    public final void writeTo(IonWriter writer) {
        try {
            writer.stepIn(IonType.SEXP);
            for (IonValue iv : this) {
                iv.writeTo(writer);
            }
            writer.stepOut();
        } catch (Exception e) {
            throw new IonException(e);
        }
    }

    @Override
    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }
}
