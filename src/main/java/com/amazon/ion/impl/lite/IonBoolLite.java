// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.lite;

import com.amazon.ion.IonBool;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.NullValueException;
import com.amazon.ion.ValueVisitor;
import java.io.IOException;


final class IonBoolLite
    extends IonValueLite
    implements IonBool
{
    private static final int HASH_SIGNATURE =
        IonType.BOOL.toString().hashCode();

    /**
     * Optimizes out a function call for a const result
     */
    protected static final int TRUE_HASH
            = HASH_SIGNATURE ^ (16777619 * Boolean.TRUE.hashCode());

    /**
     * Optimizes out a function call for a const result
     */
    protected static final int FALSE_HASH
            = HASH_SIGNATURE ^ (16777619 * Boolean.FALSE.hashCode());

    /**
     * Constructs a null bool value.
     */
    IonBoolLite(ContainerlessContext context, boolean isNull)
    {
        super(context, isNull);
    }

    IonBoolLite(IonBoolLite existing, IonContext context)
    {
        super(existing, context);
    }

    @Override
    IonValueLite shallowClone(IonContext context)
    {
        return new IonBoolLite(this, context);
    }

    @Override
    public IonBoolLite clone()
    {
        return (IonBoolLite) shallowClone(ContainerlessContext.wrap(getSystem()));
    }

    @Override
    public IonType getTypeSlow()
    {
        return IonType.BOOL;
    }

    @Override
    int hashSignature() {
        return HASH_SIGNATURE;
    }

    @Override
    int scalarHashCode()
    {
        int result = _isBoolTrue() ? TRUE_HASH : FALSE_HASH;
        return hashTypeAnnotations(result);
    }

    public boolean booleanValue()
        throws NullValueException
    {
        validateThisNotNull();
        return this._isBoolTrue();
    }

    public void setValue(boolean b)
    {
        // the called setValue will check if this is locked
        setValue(Boolean.valueOf(b));
    }

    public void setValue(Boolean b)
    {
        checkForLock();
        if (b == null) {
            _isBoolTrue(false);
            _isNullValue(true);
        }
        else {
            _isBoolTrue(b.booleanValue());
            _isNullValue(false);
        }
    }

    @Override
    final void writeBodyTo(IonWriter writer, SymbolTableProvider symbolTableProvider)
        throws IOException
    {
        writer.writeBool(_isBoolTrue());
    }

    @Override
    public void accept(ValueVisitor visitor)
        throws Exception
    {
        visitor.visit(this);
    }
}
