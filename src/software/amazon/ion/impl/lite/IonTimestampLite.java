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
import java.util.Date;
import software.amazon.ion.IonTimestamp;
import software.amazon.ion.IonType;
import software.amazon.ion.IonWriter;
import software.amazon.ion.NullValueException;
import software.amazon.ion.Timestamp;
import software.amazon.ion.ValueVisitor;

final class IonTimestampLite
    extends IonValueLite
    implements IonTimestamp
{
    public final static Integer UTC_OFFSET = Timestamp.UTC_OFFSET;

    private static final int BIT_FLAG_YEAR      = 0x01;
    private static final int BIT_FLAG_MONTH     = 0x02;
    private static final int BIT_FLAG_DAY       = 0x04;
    private static final int BIT_FLAG_MINUTE    = 0x08;
    private static final int BIT_FLAG_SECOND    = 0x10;
    private static final int BIT_FLAG_FRACTION  = 0x20;
    private static final int HASH_SIGNATURE =
        IonType.TIMESTAMP.toString().hashCode();


    private Timestamp _timestamp_value;

    /**
     * Constructs a <code>null.timestamp</code> value.
     */
    IonTimestampLite(ContainerlessContext context, boolean isNull)
    {
        super(context, isNull);
    }

    IonTimestampLite(IonTimestampLite existing, IonContext context)
    {
        super(existing, context);
        // Timestamp contract is immutable; so can simply pass the reference
        this._timestamp_value = existing._timestamp_value;
    }

    @Override
    IonTimestampLite clone(IonContext context)
    {
        return new IonTimestampLite(this, context);
    }

    @Override
    public IonTimestampLite clone()
    {
        return clone(ContainerlessContext.wrap(getSystem()));
    }

    @Override
    int hashCode(SymbolTableProvider symbolTableProvider) {
        int result = HASH_SIGNATURE;

        if (!isNullValue())  {
            result ^= timestampValue().hashCode();
        }

        return hashTypeAnnotations(result, symbolTableProvider);
    }

    @Override
    public IonType getType()
    {
        return IonType.TIMESTAMP;
    }

    public Timestamp timestampValue()
    {
        if (isNullValue()) {
            return null;
        }
        return _timestamp_value;
    }

    public Date dateValue()
    {
        if (_isNullValue()) {
            return null;
        }
        return _timestamp_value.dateValue();
    }


    public Integer getLocalOffset() throws NullValueException
    {
        if (_isNullValue()) {
            throw new NullValueException();
        }
        return _timestamp_value.getLocalOffset();
    }


    /**
     * Returns null if this is null.timestamp.
     */
    private Integer getInternalLocalOffset()
    {
        if (_isNullValue()) {
            return null;
        }
        return _timestamp_value.getLocalOffset();
    }

    public void setValue(Timestamp timestamp)
    {
        checkForLock();
        _timestamp_value = timestamp;
        _isNullValue(timestamp == null);
    }

    public void setValue(BigDecimal millis, Integer localOffset)
    {
        setValue(Timestamp.forMillis(millis, localOffset));
    }

    public void setValue(long millis, Integer localOffset)
    {
        setValue(Timestamp.forMillis(millis, localOffset));
    }

    public void setTime(Date value)
    {
        if (value == null)
        {
            makeNull();
        }
        else
        {
            // setMillis(long) will check for the lock
            setMillis(value.getTime());
        }
    }


    public BigDecimal getDecimalMillis()
    {
        if (_isNullValue()) {
            return null;
        }
        return _timestamp_value.getDecimalMillis();
    }

    public void setDecimalMillis(BigDecimal millis)
    {
        // setValue() calls checkForLock()
        Integer offset = getInternalLocalOffset();
        setValue(millis, offset);
    }


    public long getMillis()
    {
        if (_isNullValue()) {
            throw new NullValueException();
        }
        return _timestamp_value.getMillis();
    }


    public void setMillis(long millis)
    {
        // setValue() calls checkForLock()
        Integer offset = getInternalLocalOffset();
        setValue(millis, offset);
    }


    public void setMillisUtc(long millis)
    {
        // setValue() calls checkForLock()
        setValue(millis, UTC_OFFSET);
    }


    public void setCurrentTime()
    {
        long millis = System.currentTimeMillis();
        setMillis(millis);
    }

    public void setCurrentTimeUtc()
    {
        long millis = System.currentTimeMillis();
        setMillisUtc(millis);
    }

    public void setLocalOffset(int minutes)
        throws NullValueException
    {
        // setLocalOffset(Integer) will check for the lock
        setLocalOffset(new Integer(minutes));
    }


    public void setLocalOffset(Integer minutes)
        throws NullValueException
    {
        validateThisNotNull();
        assert (_timestamp_value != null);

        setValue(_timestamp_value.getDecimalMillis(), minutes);
    }

    public void makeNull()
    {
        checkForLock();
        _timestamp_value = null;
        _isNullValue(true);
    }

    @Override
    final void writeBodyTo(IonWriter writer, SymbolTableProvider symbolTableProvider)
        throws IOException
    {
        writer.writeTimestamp(_timestamp_value);
    }

    @Override
    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }
}
