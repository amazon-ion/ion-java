// Copyright (c) 2010-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.IonException;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.NullValueException;
import com.amazon.ion.Timestamp;
import com.amazon.ion.Timestamp.Precision;
import com.amazon.ion.ValueVisitor;
import java.math.BigDecimal;
import java.util.Date;

/**
 *
 */
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

    static public int getPrecisionAsBitFlags(Precision p) {
        int precision_flags = 0;

        // fall through each case - by design - to accumulate all necessary bits
        switch (p) {
        default:        throw new IllegalStateException("unrecognized precision"+p);
        case FRACTION:  precision_flags |= BIT_FLAG_FRACTION;
        case SECOND:    precision_flags |= BIT_FLAG_SECOND;
        case MINUTE:    precision_flags |= BIT_FLAG_MINUTE;
        case DAY:       precision_flags |= BIT_FLAG_DAY;
        case MONTH:     precision_flags |= BIT_FLAG_MONTH;
        case YEAR:      precision_flags |= BIT_FLAG_YEAR;
        }

        return precision_flags;
    }

    static public boolean precisionIncludes(int precision_flags,
                                            Precision isIncluded)
    {
        switch (isIncluded) {
        case FRACTION:  return (precision_flags & BIT_FLAG_FRACTION) != 0;
        case SECOND:    return (precision_flags & BIT_FLAG_SECOND) != 0;
        case MINUTE:    return (precision_flags & BIT_FLAG_MINUTE) != 0;
        case DAY:       return (precision_flags & BIT_FLAG_DAY) != 0;
        case MONTH:     return (precision_flags & BIT_FLAG_MONTH) != 0;
        case YEAR:      return (precision_flags & BIT_FLAG_YEAR) != 0;
        default:        break;
        }
        throw new IllegalStateException("unrecognized precision"+isIncluded);
    }


    private Timestamp _timestamp_value;

    /**
     * Constructs a <code>null.timestamp</code> value.
     */
    public IonTimestampLite(IonSystemLite system, boolean isNull)
    {
        super(system, isNull);
    }


    /**
     * makes a copy of this IonTimestamp. This calls up to
     * IonValueImpl to copy
     * the annotations and the field name if appropriate.
     * It then copies the time stamp value itself.
     */
    @Override
    public IonTimestampLite clone()
    {
        IonTimestampLite clone = new IonTimestampLite(this._context.getSystem(), false);

        clone.copyValueContentFrom(this);
        clone._timestamp_value = this._timestamp_value;

        return clone;
    }

    /**
     * Implements {@link Object#hashCode()} consistent with equals. This
     * implementation uses the hash of the underlying timestamp value XOR'ed
     * with a constant.
     *
     * @return  An int, consistent with the contracts for
     *          {@link Object#hashCode()} and {@link Object#equals(Object)}.
     */
    @Override
    public int hashCode() {
        int hash = HASH_SIGNATURE;
        if (!isNullValue())  {
            hash ^= timestampValue().hashCode();
        }
        return hash;
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
        setValue(new Timestamp(millis, localOffset));
    }

    public void setValue(long millis, Integer localOffset)
    {
        setValue(new Timestamp(millis, localOffset));
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

    public final void writeTo(IonWriter writer) {
        try {
            writer.setTypeAnnotationSymbols(getTypeAnnotationSymbols());
            if (isNullValue()) {
                writer.writeNull(IonType.TIMESTAMP);
            } else {
                writer.writeTimestamp(_timestamp_value);
            }
        } catch (Exception e) {
            throw new IonException(e);
        }
    }

    @Override
    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }
}
