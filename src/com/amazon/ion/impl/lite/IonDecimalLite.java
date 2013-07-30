// Copyright (c) 2010-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.Decimal;
import com.amazon.ion.IonDecimal;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.NullValueException;
import com.amazon.ion.ValueVisitor;
import java.io.IOException;
import java.math.BigDecimal;

/**
 *
 */
final class IonDecimalLite
    extends IonValueLite
    implements IonDecimal
{
    private static final int HASH_SIGNATURE =
        IonType.DECIMAL.toString().hashCode();

    private static final int NEGATIVE_ZERO_HASH_SIGNATURE =
        "NEGATIVE ZERO".hashCode();


    public static boolean isNegativeZero(float value)
    {
        if (value != 0) return false;
        // TODO perhaps use Float.compare() instead?
        if ((Float.floatToRawIntBits(value) & 0x80000000) == 0) return false; // test the sign bit
        return true;
    }

    public static boolean isNegativeZero(double value)
    {
        if (value != 0) return false;
        // TODO perhaps use Double.compare() instead?
        if ((Double.doubleToLongBits(value) & 0x8000000000000000L) == 0) return false;
        return true;
    }

    private BigDecimal _decimal_value;

    /**
     * Constructs a <code>null.decimal</code> element.
     */
    public IonDecimalLite(IonSystemLite system, boolean isNull)
    {
        super(system, isNull);
    }

    @Override
    public IonDecimalLite clone()
    {
        IonDecimalLite clone = new IonDecimalLite(this._context.getSystem(), false);

        clone.copyMemberFieldsFrom(this);
        clone.setValue(this._decimal_value);

        return clone;
    }

    @Override
    public int hashCode()
    {
        int result = HASH_SIGNATURE;

        // This is consistent with Decimal.equals(Object), and with Equivalence
        // strict equality checks between two IonDecimals.
        if (!isNullValue())  {
            Decimal dec = decimalValue();
            result ^= dec.hashCode();

            if (dec.isNegativeZero())
            {
                result ^= NEGATIVE_ZERO_HASH_SIGNATURE;
            }
        }

        return hashTypeAnnotations(result);
    }

    @Override
    public IonType getType()
    {
        return IonType.DECIMAL;
    }


    public float floatValue()
        throws NullValueException
    {
        if (_isNullValue()) throw new NullValueException();
        float f = _decimal_value.floatValue();
        return f;
    }

    public double doubleValue()
        throws NullValueException
    {
        if (_isNullValue()) throw new NullValueException();
        double d = _decimal_value.doubleValue();
        return d;
    }

    @Deprecated
    public BigDecimal toBigDecimal()
        throws NullValueException
    {
        return bigDecimalValue();
    }

    public BigDecimal bigDecimalValue()
        throws NullValueException
    {
        return Decimal.bigDecimalValue(_decimal_value); // Works for null.
    }

    public Decimal decimalValue()
        throws NullValueException
    {
        return Decimal.valueOf(_decimal_value); // Works for null.
    }

    public void setValue(long value)
    {
        // base setValue will check for the lock
        setValue(Decimal.valueOf(value));
    }

    public void setValue(float value)
    {
        // base setValue will check for the lock
        setValue(Decimal.valueOf(value));
    }

    public void setValue(double value)
    {
        // base setValue will check for the lock
        setValue(Decimal.valueOf(value));
    }

    public void setValue(BigDecimal value)
    {
        checkForLock();
        _decimal_value = value;
        _isNullValue(value == null);
    }

    @Override
    final void writeBodyTo(IonWriter writer)
        throws IOException
    {
        writer.writeDecimal(_decimal_value);
    }

    @Override
    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }
}
