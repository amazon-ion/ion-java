// Copyright (c) 2010-2014 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.Decimal;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.NullValueException;
import com.amazon.ion.ValueVisitor;
import java.io.IOException;
import java.math.BigDecimal;

/**
 *
 */
final class IonFloatLite
    extends IonValueLite
    implements IonFloat
{
    private static final int HASH_SIGNATURE =
        IonType.FLOAT.toString().hashCode();

    private Double _float_value;

    /**
     * Constructs a <code>null.float</code> element.
     */
    public IonFloatLite(IonSystemLite system, boolean isNull)
    {
        super(system, isNull);
    }

    @Override
    public IonFloatLite clone()
    {
        IonFloatLite clone = new IonFloatLite(this._context.getSystem(), false);

        clone.copyMemberFieldsFrom(this);
        clone.setValue(this._float_value);

        return clone;
    }

    @Override
    public int hashCode()
    {
        int result = HASH_SIGNATURE;

        if (!isNullValue())  {
            long bits = Double.doubleToLongBits(doubleValue());
            result ^= (int) ((bits >>> 32) ^ bits);
        }

        return hashTypeAnnotations(result);
    }

    @Override
    public IonType getType()
    {
        return IonType.FLOAT;
    }


    public float floatValue()
        throws NullValueException
    {
        validateThisNotNull();
        return _float_value.floatValue();
    }

    public double doubleValue()
        throws NullValueException
    {
        validateThisNotNull();
        return _float_value.doubleValue();
    }

    public BigDecimal bigDecimalValue()
        throws NullValueException
    {
        if (isNullValue()) {
            return null;
        }
        return Decimal.valueOf(_float_value.doubleValue());
    }

    public void setValue(float value)
    {
        // base set value will check for the lock
        setValue(new Double(value));
    }

    public void setValue(double value)
    {
        // base setValue will check for the lock
        setValue(new Double(value));
    }

    public void setValue(BigDecimal value)
    {
        checkForLock();
        if (value == null)
        {
            _float_value = null;
            _isNullValue(true);
        }
        else
        {
            setValue(value.doubleValue());
        }
    }

    public void setValue(Double d)
    {
        checkForLock();
        _float_value = d;
        _isNullValue(d == null);
    }

    @Override
    final void writeBodyTo(IonWriter writer)
        throws IOException
    {
        if (isNullValue())
        {
            writer.writeNull(IonType.FLOAT);
        }
        else
        {
            writer.writeFloat(_float_value);
        }
    }

    @Override
    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }
}
