/*
 * Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.NullValueException;
import com.amazon.ion.TtTimestamp;
import com.amazon.ion.ValueVisitor;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;

/**
 * Implements the Ion <code>timestamp</code> type.
 */
public final class IonTimestampImpl
    extends IonValueImpl
    implements IonTimestamp
{
    public final static Integer UTC_OFFSET = TtTimestamp.UTC_OFFSET;

    static final int NULL_TIMESTAMP_TYPEDESC =
        IonConstants.makeTypeDescriptor(IonConstants.tidTimestamp,
                                        IonConstants.lnIsNullAtom);

    private TtTimestamp _timestamp_value;

    /**
     * Constructs a <code>null.timestamp</code> value.
     */
    public IonTimestampImpl(IonSystemImpl system)
    {
        super(system, NULL_TIMESTAMP_TYPEDESC);
    }


    /**
     * Constructs a binary-backed value.
     */
    public IonTimestampImpl(IonSystemImpl system, int typeDesc)
    {
        super(system, typeDesc);
        assert pos_getType() == IonConstants.tidTimestamp;
    }

    /**
     * makes a copy of this IonTimestamp. This calls up to
     * IonValueImpl to copy
     * the annotations and the field name if appropriate.
     * It then copies the time stamp value itself.
     */
    @Override
    public IonTimestampImpl clone()
    {
        IonTimestampImpl clone = new IonTimestampImpl(_system);

        clone.copyAnnotationsFrom(this);  // Calls makeReady()
        clone._timestamp_value = this._timestamp_value;
        clone._hasNativeValue = true;
        return clone;
    }


    public IonType getType()
    {
        return IonType.TIMESTAMP;
    }


    public TtTimestamp timestampValue()
    {
        makeReady();
        return _timestamp_value;
    }

    public Date dateValue()
    {
        makeReady();
        if (_timestamp_value == null) return null;
        return _timestamp_value.dateValue();
    }


    public Integer getLocalOffset() throws NullValueException
    {
        makeReady();
        if (_timestamp_value == null) throw new NullValueException();
        return _timestamp_value.getLocalOffset();
    }


    /**
     * Returns null if this is null.timestamp.
     */
    private Integer getInternalLocalOffset()
    {
        makeReady();
        if (_timestamp_value == null) return null;
        return _timestamp_value.getLocalOffset();
    }

    public void setValue(TtTimestamp timestamp)
    {
        checkForLock();
        _timestamp_value = timestamp;
        _hasNativeValue = true;
        setDirty();
    }

    public void setValue(BigDecimal millis, Integer localOffset)
    {
        setValue(new TtTimestamp(millis, localOffset));
    }

    public void setValue(long millis, Integer localOffset)
    {
        setValue(new TtTimestamp(millis, localOffset));
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
        makeReady();
        if (_timestamp_value == null) return null;
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
        makeReady();

        if (_timestamp_value == null) {
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
        makeReady();
        assert (_timestamp_value != null);

        setValue(_timestamp_value.getDecimalMillis(), minutes);
    }

    public void makeNull()
    {
        checkForLock();
        _timestamp_value = null;
        _hasNativeValue = true;
        setDirty();
    }


    @Override
    public synchronized boolean isNullValue()
    {
        if (!_hasNativeValue) return super.isNullValue();
        return (_timestamp_value == null);
    }

    @Override
    protected int getNativeValueLength()
    {
        assert _hasNativeValue == true;
        return IonBinary.lenIonTimestamp(_timestamp_value);
    }


    @Override
    protected int computeLowNibble(int valuelen)
    {
        assert _hasNativeValue == true;

        int ln = 0;
        if (_timestamp_value == null) {
            ln = IonConstants.lnIsNullAtom;
        }
        else {
            ln = getNativeValueLength();
            if (ln > IonConstants.lnIsVarLen) {
                ln = IonConstants.lnIsVarLen;
            }
        }
        return ln;
    }

    @Override
    protected void doMaterializeValue(IonBinary.Reader reader) throws IOException
    {
        assert this._isPositionLoaded == true && this._buffer != null;

        // a native value trumps a buffered value
        if (_hasNativeValue) return;

        // the reader will have been positioned for us
        assert reader.position() == this.pos_getOffsetAtValueTD();

//      we need to skip over the td to get to the good stuff
        int td = reader.read();
        assert (byte)(0xff & td) == this.pos_getTypeDescriptorByte();

        int type = this.pos_getType();
        if (type != IonConstants.tidTimestamp) {
            throw new IonException("invalid type desc encountered for value");
        }

        int ln = this.pos_getLowNibble();
        switch ((0xf & ln)) {   // TODO is the mask necessary?
        case IonConstants.lnIsNullAtom:
            _timestamp_value = null;
            break;
        case 0:
            _timestamp_value = new TtTimestamp(0, null);
            break;
        case IonConstants.lnIsVarLen:
            ln = reader.readVarUInt7IntValue();
            // fall through to default:
        default:
            _timestamp_value = reader.readTimestampValue(ln);
            break;
        }

        _hasNativeValue = true;
    }



    @Override
    protected void doWriteNakedValue(IonBinary.Writer writer, int valueLen) throws IOException
    {
        assert valueLen == this.getNakedValueLength();
        assert valueLen > 0;

        int wlen = writer.writeTimestamp(_timestamp_value);
        assert wlen == valueLen;

        return;
    }


    public void accept(ValueVisitor visitor) throws Exception
    {
        makeReady();
        visitor.visit(this);
    }
}
