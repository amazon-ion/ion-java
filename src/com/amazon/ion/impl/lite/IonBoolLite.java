// Copyright (c) 2010-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.IonBool;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.NullValueException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.ValueVisitor;
import java.io.IOException;

/**
 *
 */
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
    public IonBoolLite(IonContext context, boolean isNull)
    {
        super(context, isNull);
    }

    IonBoolLite(IonBoolLite existing, IonContext context)
    {
        super(existing, context);
    }

    @Override
    IonBoolLite clone(IonContext context)
    {
        return new IonBoolLite(this, context);
    }

    @Override
    public IonBoolLite clone()
    {
        return clearFieldName(this.clone(getSystem()));
    }

    @Override
    public IonType getType()
    {
        return IonType.BOOL;
    }

    @Override
    int hashCode(SymbolTable symbolTable)
    {
        int result = HASH_SIGNATURE;

        if (!isNullValue())
        {
            result = booleanValue() ? TRUE_HASH : FALSE_HASH;
        }

        return hashTypeAnnotations(result, symbolTable);
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
    final void writeBodyTo(IonWriter writer, SymbolTable symbolTable)
        throws IOException
    {
        if (isNullValue())
        {
            writer.writeNull(IonType.BOOL);
        }
        else
        {
            writer.writeBool(_isBoolTrue());
        }
    }

    @Override
    public void accept(ValueVisitor visitor)
        throws Exception
    {
        visitor.visit(this);
    }
}
