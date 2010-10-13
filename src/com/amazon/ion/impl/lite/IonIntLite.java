// Copyright (c) 2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.IonException;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonType;
import com.amazon.ion.NullValueException;
import com.amazon.ion.ValueVisitor;
import java.math.BigInteger;


/**
 *
 */
public class IonIntLite
    extends IonValueLite
    implements IonInt
{
    // FIXME We can't handle Long.MIN_VALUE at encoding time.
    static private final BigInteger MIN_VALUE =
        BigInteger.valueOf(Long.MIN_VALUE + 1);

    static private final BigInteger MAX_VALUE =
        BigInteger.valueOf(Long.MAX_VALUE);

    private static final int HASH_SIGNATURE =
        IonType.INT.toString().hashCode();

    private long _long_value;


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
        IonIntLite clone = new IonIntLite(this._context.getSystemLite(), false);

        clone.copyValueContentFrom(this);
        clone.doSetValue(this._long_value, this._isNullValue());

        return clone;
    }

    /**
     * Calculate Ion Int hash code by returning long hash value XOR'ed
     * with IonType hash code.
     * @return hash code
     */
    @Override
    public int hashCode()
    {
        int hash = HASH_SIGNATURE;
        if (!isNullValue())  {
            // FIXME if/when IonIntImpl is extended to support values bigger
            // than a long,
            long lv = longValue();
            // jonker memorial bug:  throw away top 32 bits if they're not
            // interesting.  Other n and -(n+1) get the same hash code.
            hash ^= (int) lv;
            int hi_word = (int) (lv >>> 32);
            if (hi_word != 0 && hi_word != -1)  {
                hash ^= hi_word;
            }
        }
        return hash;
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
        return (int)_long_value;
    }

    public long longValue()
        throws NullValueException
    {
        validateThisNotNull();
        return _long_value;
    }

    public BigInteger bigIntegerValue()
        throws NullValueException
    {
        if (isNullValue()) {
            return null;
        }
        return BigInteger.valueOf(_long_value);
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
                if ((big.compareTo(MIN_VALUE) < 0) ||
                    (big.compareTo(MAX_VALUE) > 0))
                {
                    String message =
                        "int too large for this implementation: " + big;
                    throw new IonException(message);
                }
            }
            doSetValue(value.longValue(), false);
        }
    }

    private void doSetValue(long value, boolean isNull)
    {
        _long_value = value;
        _isNullValue(isNull);
    }

    @Override
    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }

}
