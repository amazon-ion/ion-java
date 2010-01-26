/*
 * Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.ContainedValueException;
import com.amazon.ion.IonException;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.ValueVisitor;
import java.io.IOException;
import java.util.Collection;

/**
 * Implements the Ion <code>sexp</code> (S-expression) type.
 */
public class IonSexpImpl
    extends IonSequenceImpl
    implements IonSexp
{
    private static final int NULL_SEXP_TYPEDESC =
        IonConstants.makeTypeDescriptor(IonConstants.tidSexp,
                                        IonConstants.lnIsNullSequence);

    private static final int HASH_SIGNATURE =
        IonType.SEXP.toString().hashCode();

    /**
     * Constructs a <code>null.sexp</code> value.
     */
    public IonSexpImpl(IonSystemImpl system)
    {
        this(system, true);
    }

    /**
     * Constructs a null or empty S-expression.
     *
     * @param makeNull indicates whether this should be <code>null.sexp</code>
     * (if <code>true</code>) or an empty sequence (if <code>false</code>).
     */
    public IonSexpImpl(IonSystemImpl system, boolean makeNull)
    {
        super(system, NULL_SEXP_TYPEDESC, makeNull);
        assert pos_getType() == IonConstants.tidSexp;
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
    public IonSexpImpl(IonSystemImpl system,
                       Collection<? extends IonValue> elements)
        throws ContainedValueException
    {
        super(system, NULL_SEXP_TYPEDESC, elements);
    }


    /**
     * Constructs a non-materialized sexp backed by a binary buffer.
     */
    public IonSexpImpl(IonSystemImpl system, int typeDesc)
    {
        super(system, typeDesc, true);
        assert pos_getType() == IonConstants.tidSexp;
    }

    /**
     * creates a copy of this IonSexpImpl.  Most of the work
     * is actually done by IonContainerImpl.copyFrom() and
     * IonValueImpl.copyFrom().
     */
    @Override
    public IonSexpImpl clone()
    {
        IonSexpImpl clone = new IonSexpImpl(_system);

        try {
            clone.copyFrom(this);
        } catch (IOException e) {
            throw new IonException(e);
        }

        return clone;
    }

    /**
     * Implements {@link Object#hashCode()} consistent with equals.
     *
     * @return  An int, consistent with the contracts for
     *          {@link Object#hashCode()} and {@link Object#equals(Object)}.
     */
    @Override
    public int hashCode() {
        return sequenceHashCode(HASH_SIGNATURE);
    }

    public IonType getType()
    {
        return IonType.SEXP;
    }


    public void accept(ValueVisitor visitor) throws Exception
    {
        makeReady();
        visitor.visit(this);
    }
}
