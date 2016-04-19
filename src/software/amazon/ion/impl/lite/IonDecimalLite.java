/*
 * Copyright 2010-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion.impl.lite;

import java.io.IOException;
import java.math.BigDecimal;
import software.amazon.ion.Decimal;
import software.amazon.ion.IonDecimal;
import software.amazon.ion.IonType;
import software.amazon.ion.IonWriter;
import software.amazon.ion.NullValueException;
import software.amazon.ion.ValueVisitor;

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
    IonDecimalLite(ContainerlessContext context, boolean isNull)
    {
        super(context, isNull);
    }

    IonDecimalLite(IonDecimalLite existing, IonContext context)
    {
        super(existing, context);
        // we can shallow copy as BigDecimal is immutable
        this._decimal_value = existing._decimal_value;
    }

    @Override
    IonDecimalLite clone(IonContext parentContext)
    {
        return new IonDecimalLite(this, parentContext);
    }

    @Override
    public IonDecimalLite clone()
    {
        return clone(ContainerlessContext.wrap(getSystem()));
    }

    @Override
    int hashCode(SymbolTableProvider symbolTableProvider)
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

        return hashTypeAnnotations(result, symbolTableProvider);
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
    final void writeBodyTo(IonWriter writer, SymbolTableProvider symbolTableProvider)
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
