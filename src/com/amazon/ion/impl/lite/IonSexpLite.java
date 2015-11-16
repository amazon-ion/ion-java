// Copyright (c) 2010-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.ContainedValueException;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.ValueVisitor;
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

    IonSexpLite(IonSexpLite existing, IonContext context)
    {
        super(existing, context);
    }

    /**
     * Constructs a sexp value <em>not</em> backed by binary.
     *
     * @param elements
     *   the initial set of child elements.  If <code>null</code>, then the new
     *   instance will have <code>{@link #isNullValue()} == true</code>.
     *
     * @throws ContainedValueException if any value in <code>elements</code>
     *  has <code>{@link IonValue#getContainer()} != null</code>.
     */
    IonSexpLite(IonContext context,
                Collection<? extends IonValue> elements)
        throws ContainedValueException
    {
        super(context, elements);
    }

    @Override
    IonSexpLite clone(IonContext parentContext)
    {
        return new IonSexpLite(this, parentContext);
    }

    @Override
    public IonSexpLite clone()
    {
        return clearFieldName(this.clone(getSystem()));
    }

    @Override
    public int hashCode(SymbolTable symbolTable) {
        return sequenceHashCode(HASH_SIGNATURE, symbolTable);
    }

    @Override
    public IonType getType()
    {
        return IonType.SEXP;
    }

    @Override
    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }
}
