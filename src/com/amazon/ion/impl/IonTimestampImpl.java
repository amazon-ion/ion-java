/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import java.io.IOException;
import java.util.Date;

import com.amazon.ion.IonException;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.NullValueException;
import com.amazon.ion.ValueVisitor;
import com.amazon.ion.impl.IonTokenReader.Type.timeinfo;

/**
 * Implements the Ion <code>timestamp</code> type.
 */
public final class IonTimestampImpl
    extends IonValueImpl
    implements IonTimestamp
{
    public final static Integer UTC_OFFSET = new Integer(0);

    static final int NULL_TIMESTAMP_TYPEDESC =
        IonConstants.makeTypeDescriptor(IonConstants.tidTimestamp,
                                        IonConstants.lnIsNullAtom);

    private timeinfo _timestamp_value;

    /**
     * Constructs a <code>null.timestamp</code> value.
     */
    public IonTimestampImpl()
    {
        super(NULL_TIMESTAMP_TYPEDESC);
    }


    /**
     * Constructs a binary-backed value.
     */
    public IonTimestampImpl(int typeDesc)
    {
        super(typeDesc);
        assert pos_getType() == IonConstants.tidTimestamp;
    }


    public Date dateValue()
    {
        makeReady();

        if (_timestamp_value == null) return null;

        // Make a copy because the user might change it!
        return new Date(_timestamp_value.d.getTime());
    }


    public Integer getLocalOffset() throws NullValueException
    {
        makeReady();
        if (_timestamp_value == null) throw new NullValueException();
        return (_timestamp_value.localOffset == null ? null : _timestamp_value.localOffset);
    }


    public void setTime(Date value)
    {
        /* I assume that if setTime is called, most of the time a change will
         * occur, and most of the time it won't be called again.  Thus we
         * should assume that we should be optimized for encoding.
         */
        if (value == null)
        {
            _timestamp_value = null;
        }
        else
        {
            Date date = (value == null ? null : new Date(value.getTime()));
            if (_timestamp_value == null)
            {
                _timestamp_value = new timeinfo(date, null);
            }
            else
            {
                _timestamp_value.d = date;
            }
        }
        _hasNativeValue = true;
        setDirty();
    }


    public long getMillis()
    {
        makeReady();

        if (_timestamp_value == null) {
            throw new NullValueException();
        }

        return _timestamp_value.d.getTime();
    }


    public void setMillis(long millis)
    {
        setTime(new Date(millis));
    }


    public void setMillisUtc(long millis)
    {
        setTime(new Date(millis));
        _timestamp_value.localOffset = UTC_OFFSET;
    }


    public void setCurrentTime()
    {
        Date date = new Date();
        if (_timestamp_value == null)
        {
            _timestamp_value = new timeinfo(date, null);
        }
        else
        {
            _timestamp_value.d = date;
        }
        _hasNativeValue = true;
        setDirty();
    }

    public void setCurrentTimeUtc()
    {
        setCurrentTime();
        _timestamp_value.localOffset = UTC_OFFSET;
    }

    public void setLocalOffset(int minutes)
        throws NullValueException
    {
        setLocalOffset(new Integer(minutes));
    }


    public void setLocalOffset(Integer minutes)
        throws NullValueException
    {
        validateThisNotNull();
        assert (_timestamp_value != null);

        _timestamp_value.localOffset = minutes;
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
            _timestamp_value = new timeinfo();
            _timestamp_value.d = new Date(0L);
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
