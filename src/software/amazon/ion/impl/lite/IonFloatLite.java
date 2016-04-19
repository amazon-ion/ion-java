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
import software.amazon.ion.IonFloat;
import software.amazon.ion.IonType;
import software.amazon.ion.IonWriter;
import software.amazon.ion.NullValueException;
import software.amazon.ion.ValueVisitor;

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
    IonFloatLite(ContainerlessContext context, boolean isNull)
    {
        super(context, isNull);
    }

    IonFloatLite(IonFloatLite existing, IonContext context)
    {
        super(existing, context);
        // shallow copy as Double is immutable
        this._float_value = existing._float_value;
    }

    @Override
    IonFloatLite clone(IonContext context)
    {
        return new IonFloatLite(this, context);
    }

    @Override
    public IonFloatLite clone()
    {
        return clone(ContainerlessContext.wrap(getSystem()));
    }

    @Override
    int hashCode(SymbolTableProvider symbolTableProvider)
    {
        int result = HASH_SIGNATURE;

        if (!isNullValue())  {
            long bits = Double.doubleToLongBits(doubleValue());
            result ^= (int) ((bits >>> 32) ^ bits);
        }

        return hashTypeAnnotations(result, symbolTableProvider);
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
    final void writeBodyTo(IonWriter writer, SymbolTableProvider symbolTableProvider)
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

    public boolean isNumericValue()
    {
        return !(isNullValue() || _float_value.isNaN() || _float_value.isInfinite());
    }

    @Override
    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }
}
