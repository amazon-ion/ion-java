// Copyright (c) 2007-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl._Private_IonConstants.lnIsNullAtom;
import static com.amazon.ion.impl._Private_IonConstants.lnNumericZero;
import static com.amazon.ion.impl._Private_IonConstants.makeTypeDescriptor;
import static com.amazon.ion.impl._Private_IonConstants.tidPosInt;

import com.amazon.ion.IonException;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonType;
import com.amazon.ion.NullValueException;
import com.amazon.ion.ValueVisitor;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;


/**
 * Implements the Ion <code>int</code> type.
 *
 * TODO: we don't properly handle values larger than Java long.
 */
public final class IonIntImpl
    extends IonValueImpl
    implements IonInt
{
    static private final BigInteger LONG_MIN_VALUE =
        BigInteger.valueOf(Long.MIN_VALUE);

    static private final BigInteger LONG_MAX_VALUE =
        BigInteger.valueOf(Long.MAX_VALUE);

    static private final BigInteger LONG_ABS_MIN_VALUE =
        LONG_MIN_VALUE.abs();

    static final int NULL_INT_TYPEDESC =
        makeTypeDescriptor(tidPosInt, lnIsNullAtom);
    static final int ZERO_INT_TYPEDESC =
        makeTypeDescriptor(tidPosInt, lnNumericZero);

    private static final int HASH_SIGNATURE =
        IonType.INT.toString().hashCode();

    private long _long_value;
    private BigInteger _big_int_value;


    /**
     * Constructs a <code>null.int</code> element.
     */
    public IonIntImpl(IonSystemImpl system)
    {
        super(system, NULL_INT_TYPEDESC);
        // _isNullValue(true);
        _hasNativeValue(true); // Since this is null
    }

    /**
     * Constructs a binary-backed element.
     */
    public IonIntImpl(IonSystemImpl system, int typeDesc)
    {
        super(system, typeDesc);
        // _isNullValue(true);
        assert pos_getType() == _Private_IonConstants.tidPosInt
            || pos_getType() == _Private_IonConstants.tidNegInt
        ;
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
    public IonIntImpl clone()
    {
        IonIntImpl clone = new IonIntImpl(_system);

        makeReady();
        clone.copyAnnotationsFrom(this);
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
            makeReady();
            if (_big_int_value == null)
            {
                long lv = longValue();
                // jonker memorial bug:  throw away top 32 bits if they're not
                // interesting.  Other n and -(n+1) get the same hash code.
                hash ^= (int) lv;
                int hi_word = (int) (lv >>> 32);
                if (hi_word != 0 && hi_word != -1)  {
                    hash ^= hi_word;
                }
            }
            else
            {
                hash = _big_int_value.hashCode();
            }
        }
        return hash;
    }

    public IonType getType()
    {
        return IonType.INT;
    }


    public int intValue()
        throws NullValueException
    {
        makeReady();
        if (_isNullValue()) throw new NullValueException();
        if (_big_int_value == null)
        {
            return (int)_long_value;
        }
        return _big_int_value.intValue();
    }

    public long longValue()
        throws NullValueException
    {
        makeReady();
        if (_isNullValue()) throw new NullValueException();
        if (_big_int_value == null)
        {
            return _long_value;
        }
        return _big_int_value.longValue();
    }

    public BigInteger bigIntegerValue()
        throws NullValueException
    {
        makeReady();
        if (_isNullValue()) return null;
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

    private void doSetValue(long value, boolean isNull)
    {
        _long_value = value;
        _big_int_value = null;
        _isNullValue(isNull);
        _hasNativeValue(true);
        setDirty();
    }

    private void doSetValue(BigInteger value) {
        if ((value.compareTo(LONG_MIN_VALUE) < 0) ||
            (value.compareTo(LONG_MAX_VALUE) > 0))
        {
            _long_value = 0L;
            _big_int_value = value;
            _isNullValue(false);
            _hasNativeValue(true);
            setDirty();
        }
        else
        {
            // fits in long
            doSetValue(value.longValue(), false);
        }
    }

    //public boolean oldisNullValue()
    //{
    //    if (!_hasNativeValue()) return super.oldisNullValue();
    //    return _isNullValue();
    //}

    @Override
    protected int getNativeValueLength()
    {
        assert _hasNativeValue() == true;
        if (_isNullValue()) return 0;
        // TODO streamline following; this is only call site.
        if (_big_int_value != null)
        {
            return IonBinary.lenIonInt(_big_int_value);
        }
        return IonBinary.lenIonInt(_long_value);
    }

    @Override
    protected int computeTypeDesc(int valuelen)
    {
        assert _hasNativeValue() == true;

        if (_isNullValue())
        {
            return NULL_INT_TYPEDESC;
        }

        int hn = 0;
        int ln = valuelen;
        if (_big_int_value != null)
        {
            BigInteger content = _big_int_value;
            switch (content.signum())
            {
            case 0:
                return ZERO_INT_TYPEDESC;
            case -1:
                hn = _Private_IonConstants.tidNegInt;
                break;
            case 1:
                hn = _Private_IonConstants.tidPosInt;
                break;
            default:
                // should never happen
                throw new IllegalStateException("Bad signum");
            }

        }
        else
        {
            long content = _long_value;
            if (content == 0)
            {
                return ZERO_INT_TYPEDESC;
            }
            hn = (content > 0 ? _Private_IonConstants.tidPosInt : _Private_IonConstants.tidNegInt);
        }

        if (ln > _Private_IonConstants.lnIsVarLen) {
            ln = _Private_IonConstants.lnIsVarLen;
        }
        assert hn == _Private_IonConstants.tidPosInt || hn == _Private_IonConstants.tidNegInt;
        return _Private_IonConstants.makeTypeDescriptor(hn, ln);
    }

    /**
     * Never called, since we override {@link #computeTypeDesc}.
     */
    @Override
    protected int computeLowNibble(int valuelen)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void doMaterializeValue(IonBinary.Reader reader)
        throws IOException
    {
        assert this._isPositionLoaded() == true && this._buffer != null;

        // a native value trumps a buffered value
        if (_hasNativeValue()) return;

        // the reader will have been positioned for us
        assert reader.position() == this.pos_getOffsetAtValueTD();

        // we need to skip over the td to get to the good stuff
        int td = reader.read();
        assert (byte)(0xff & td) == this.pos_getTypeDescriptorByte();

        int type = this.pos_getType();
        switch (type) {
        case _Private_IonConstants.tidPosInt:
        case _Private_IonConstants.tidNegInt:
            break;
        default:
            throw new IonException("invalid type desc encountered for int");
        }

        // reset internal reified state to zero
        _big_int_value = null;
        _long_value = 0;
        _isNullValue(false);

        int ln = this.pos_getLowNibble();
        switch ((0xf & ln)) {
        case _Private_IonConstants.lnIsNullAtom:
            _isNullValue(true);
            break;
        case 0:
            // no-op, we're already zeroed
            break;
        case _Private_IonConstants.lnIsVarLen:
            ln = reader.readVarUIntAsInt();
            // fall through to default:
        default:
            int signum = type == _Private_IonConstants.tidNegInt ? -1 : 1;
            if (ln <= 8)
            {
                long val = reader.readUIntAsLong(ln);
                if (val < 0)
                {
                    // we really can't fit this magnitude properly into a Java long
                    _big_int_value = IonBinary.unsignedLongToBigInteger(signum, val);
                }
                else
                {
                    if (type == _Private_IonConstants.tidNegInt) {
                        val = -val;
                    }
                    _long_value = val;
                }
            }
            else
            {
                _big_int_value = reader.readUIntAsBigInteger(ln, signum);

            }
            break;
        }
        _hasNativeValue(true);
    }


    @Override
    protected void doWriteNakedValue(IonBinary.Writer writer, int valueLen)
        throws IOException
    {
        assert valueLen == this.getNakedValueLength();
        assert valueLen > 0;

        int wlen = 0;
        if (_big_int_value != null)
        {
            BigInteger big = _big_int_value;
            if (big.signum() < 0)
            {
                big = _big_int_value.negate();
            }
            wlen = writer.writeUIntValue(big, valueLen);
        }
        else if (_long_value == Long.MIN_VALUE)
        {
            // Long.MIN_VALUE is a bit special since we can't negate it (overflow truncation)
            wlen = writer.writeUIntValue(LONG_ABS_MIN_VALUE, valueLen);
        }
        else
        {
            // emit the native long
            long l = (_long_value < 0) ? -_long_value : _long_value;
            wlen = writer.writeUIntValue(l, valueLen);
        }
        assert wlen == valueLen;
    }


    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }
}
