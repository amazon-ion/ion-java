// Copyright (c) 2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.Decimal;
import com.amazon.ion.IonDecimal;
import com.amazon.ion.IonNumber;
import com.amazon.ion.IonType;
import com.amazon.ion.NullValueException;
import com.amazon.ion.ValueVisitor;
import java.math.BigDecimal;

/**
 *
 */
public class IonDecimalLite
extends IonValueLite
implements IonDecimal
{
    private static final int HASH_SIGNATURE =
        IonType.DECIMAL.toString().hashCode();

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


    /**
     * makes a copy of this IonDecimal including a copy
     * of the BigDecimal value which is "naturally" immutable.
     * This calls IonValueImpl to copy the annotations and the
     * field name if appropriate.  The symbol table is not
     * copied as the value is fully materialized and the symbol
     * table is unnecessary.
     */
    @Override
    public IonDecimalLite clone()
    {
        IonDecimalLite clone = new IonDecimalLite(this._context.getSystemLite(), false);

        clone.copyValueContentFrom(this);
        clone.setValue(this._decimal_value);

        return clone;
    }

    /**
     * Calculate Ion Decimal hash code as hash code of double value,
     * XOR'ed with IonType hash code. This is required because
     * {@link IonDecimal#equals(Object)} is not consistent
     * with {@link BigDecimal#equals(Object)}, but rather with
     * {@link BigDecimal#compareTo(BigDecimal)}.
     * @return hash code
     */
    @Override
    public int hashCode()
    {
        int hash = HASH_SIGNATURE;
        if (!isNullValue())  {
            long bits = Double.doubleToLongBits(doubleValue());
            hash ^= (int) ((bits >>> 32) ^ bits);
        }
        return hash;
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

    @Deprecated
    public void setValueSpecial(IonNumber.Classification valueClassification)
    {
        setValue(BigDecimal.ZERO, valueClassification);
    }

    @Deprecated
    public void setValue(BigDecimal value, IonNumber.Classification valueClassification)
    {
        checkForLock();
        switch (valueClassification)
        {
        case NORMAL:
                _decimal_value = value;
                _isNullValue(value == null);
                break;
        case NEGATIVE_ZERO:
                if (value.signum() != 0) throw new IllegalArgumentException("to be a negative zero the value must be zero");
                _decimal_value = Decimal.negativeZero(value.scale());
                _isNullValue(false);
                break;
        default:
                throw new IllegalArgumentException("IonDecimal values may only be NORMAL or NEGATIVE_ZERO");
        }
    }

    /**
     * Gets the number classification of this value. IonDecimal
     * values may only be NORMAL or NEGATIVE_ZERO.
     */
    public IonNumber.Classification getClassification()
    {
        return (isNullValue() || ! Decimal.isNegativeZero(_decimal_value)
                    ? Classification.NORMAL
                    : Classification.NEGATIVE_ZERO);
    }

    @Override
    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }
}
