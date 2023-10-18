/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion.impl.lite;

import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.NullValueException;
import com.amazon.ion.ValueVisitor;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;



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


    // This mask combines the IS_BOOL_TRUE (0x08) and IS_IVM (0x10)
    // masks from IonValueLite. Those flags are never relevant for
    // IonInts, which makes them safe for reuse here.
    private static final int INT_SIZE_MASK  = 0x18;
    private static final int INT_SIZE_SHIFT = 0x03;

    private static final IntegerSize[] SIZES = IntegerSize.values();

    private long _long_value;
    private BigInteger _big_int_value;

    /**
     * Constructs a <code>null.int</code> element.
     */
    IonIntLite(ContainerlessContext context, boolean isNull)
    {
        super(context, isNull);
    }

    IonIntLite(IonIntLite existing, IonContext context)
    {
        super(existing, context);
        this._long_value    = existing._long_value;
        this._big_int_value = existing._big_int_value;
    }

    @Override
    IonValueLite shallowClone(IonContext context)
    {
        return new IonIntLite(this, context);
    }

    @Override
    public IonIntLite clone()
    {
        return (IonIntLite) shallowClone(ContainerlessContext.wrap(getSystem()));
    }

    @Override
    int hashSignature() {
        return HASH_SIGNATURE;
    }

    @Override
    int scalarHashCode()
    {
        int result = HASH_SIGNATURE;
        if (_big_int_value == null)
        {
            // Throw away top 32 bits if they're not interesting.
            // Otherwise n and -(n+1) get the same hash code.
            result ^= (int) _long_value;
            int hi_word = (int) (_long_value >>> 32);
            if (hi_word != 0 && hi_word != -1)  {
                result ^= hi_word;
            }
        }
        else
        {
            result = _big_int_value.hashCode();
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

    @Override
    public BigDecimal bigDecimalValue() {
        if (isNullValue()) {
            return null;
        } else if (_big_int_value == null) {
            return BigDecimal.valueOf(_long_value);
        } else {
            return new BigDecimal(_big_int_value);
        }
    }

    @Override
    public boolean isNumericValue() {
        return !isNullValue();
    }

    public void setValue(int value)
    {
        setValue((long)value);
    }

    public void setValue(long value)
    {
        checkForLock();
        doSetValue(value, false);
    }

    public void setValue(Number value)
    {
        checkForLock();
        if (value == null)
        {
            doSetValue(0, true);
        }
        else if (value instanceof BigInteger)
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

    @Override
    final void writeBodyTo(IonWriter writer, SymbolTableProvider symbolTableProvider)
        throws IOException
    {
        if (_big_int_value != null)
        {
            writer.writeInt(_big_int_value);
        }
        else
        {
            writer.writeInt(_long_value);
        }
    }

    private void doSetValue(long value, boolean isNull)
    {
        _long_value = value;
        _big_int_value = null;
        _isNullValue(isNull);
        if (!isNull)
        {
            if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE)
            {
                // fits in long
                setSize(IntegerSize.LONG);
            }
            else
            {
                // fits in int
                setSize(IntegerSize.INT);
            }
        }
    }

    private void doSetValue(BigInteger value) {
        if ((value.compareTo(LONG_MIN_VALUE) < 0) ||
            (value.compareTo(LONG_MAX_VALUE) > 0))
        {
            setSize(IntegerSize.BIG_INTEGER);
            _long_value = 0L;
            _big_int_value = value;
            _isNullValue(false);
        }
        else {
            doSetValue(value.longValue(), false);
        }
    }

    private void setSize(IntegerSize size)
    {
        _setMetadata(size.ordinal(), INT_SIZE_MASK, INT_SIZE_SHIFT);
    }

    @Override
    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }

    public IntegerSize getIntegerSize()
    {
        if (isNullValue())
        {
            return null;
        }
        return SIZES[_getMetadata(INT_SIZE_MASK, INT_SIZE_SHIFT)];
    }

}
