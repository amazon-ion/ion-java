// Copyright (c) 2010-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.IonException;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.NullValueException;
import com.amazon.ion.ValueVisitor;
import java.math.BigDecimal;
import java.math.BigInteger;


/**
 *
 */
final class IonIntLite
    extends IonValueLite
    implements IonInt
{
    static private final BigInteger LONG_MIN_VALUE =
        BigInteger.valueOf(Long.MIN_VALUE);

    static private final BigInteger LONG_MAX_VALUE =
        BigInteger.valueOf(Long.MAX_VALUE);

    private static final int HASH_SIGNATURE =
        IonType.INT.toString().hashCode();

    private long _long_value;
    private BigInteger _big_int_value;


    /**
     * Constructs a <code>null.int</code> element.
     */
    public IonIntLite(IonSystemLite system, boolean isNull)
    {
        super(system, isNull);
    }

    /**
     * makes a copy of this IonInt including a copy
     * of the Long value which is "naturally" immutable.
     * This calls IonValueImpl to copy the annotations and the
     * field name if appropriate.  The symbol table is not
     * copied as the value is fully materialized and the symbol
     * table is unnecessary.
     */
    @Override
    public IonIntLite clone()
    {
        IonIntLite clone = new IonIntLite(this._context.getSystem(), false);

        clone.copyValueContentFrom(this);
        if (this._big_int_value != null)
        {
            clone.doSetValue(this._big_int_value);
        }
        else
        {
            clone.doSetValue(this._long_value, this._isNullValue());
        }

        return clone;
    }

    @Override
    public int hashCode()
    {
        int result = HASH_SIGNATURE;

        if (!isNullValue())  {
            if (_big_int_value == null)
            {
                long lv = longValue();
                // jonker memorial bug:  throw away top 32 bits if they're not
                // interesting.  Otherwise n and -(n+1) get the same hash code.
                result ^= (int) lv;
                int hi_word = (int) (lv >>> 32);
                if (hi_word != 0 && hi_word != -1)  {
                    result ^= hi_word;
                }
            }
            else
            {
                result = _big_int_value.hashCode();
            }
        }

        return hashTypeAnnotations(result);
    }

    @Override
    public IonType getType()
    {
        return IonType.INT;
    }

    public int intValue()
        throws NullValueException
    {
        validateThisNotNull();
        if (_big_int_value == null)
        {
            return (int)_long_value;
        }
        return _big_int_value.intValue();
    }

    public long longValue()
        throws NullValueException
    {
        validateThisNotNull();
        if (_big_int_value == null)
        {
            return _long_value;
        }
        return _big_int_value.longValue();
    }

    public BigInteger bigIntegerValue()
        throws NullValueException
    {
        if (isNullValue()) {
            return null;
        }
        if (_big_int_value == null)
        {
            return BigInteger.valueOf(_long_value);
        }
        return _big_int_value;
    }

    @Deprecated
    public BigInteger toBigInteger()
        throws NullValueException
    {
        return bigIntegerValue();
    }

    public void setValue(int value)
    {
        checkForLock();
        doSetValue(Long.valueOf(value), false);
    }

    public void setValue(long value)
    {
        checkForLock();
        doSetValue(Long.valueOf(value), false);
    }

    public void setValue(Number value)
    {
        checkForLock();
        if (value == null)
        {
            doSetValue(0, true);
        }
        else
        {
            if (value instanceof BigInteger)
            {
                BigInteger big = (BigInteger) value;
                doSetValue(big);
            }
            else if (value instanceof BigDecimal)
            {
                BigDecimal bd = (BigDecimal) value;
                doSetValue(bd.toBigInteger());
            }
            else
            {
                // XXX this is essentially a narrowing conversion
                // for some types of numbers
                doSetValue(value.longValue(), false);
            }
        }
    }

    public final void writeTo(IonWriter writer) {
        try {
            writer.setTypeAnnotationSymbols(getTypeAnnotationSymbols());
            if (isNullValue()) {
                writer.writeNull(IonType.INT);
            } else {
                if (_big_int_value != null) {
                    writer.writeInt(_big_int_value);
                } else {
                    writer.writeInt(_long_value);
                }
            }
        } catch (Exception e) {
            throw new IonException(e);
        }
    }

    private void doSetValue(long value, boolean isNull)
    {
        _long_value = value;
        _big_int_value = null;
        _isNullValue(isNull);
    }

    private void doSetValue(BigInteger value) {
        if ((value.compareTo(LONG_MIN_VALUE) < 0) ||
            (value.compareTo(LONG_MAX_VALUE) > 0))
        {
            _long_value = 0L;
            _big_int_value = value;
            _isNullValue(false);
        }
        else
        {
            // fits in long
            doSetValue(value.longValue(), false);
        }
    }

    @Override
    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }

}
