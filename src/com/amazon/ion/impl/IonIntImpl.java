/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonType;
import com.amazon.ion.NullValueException;
import com.amazon.ion.ValueVisitor;
import java.io.IOException;
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

    static final int NULL_INT_TYPEDESC =
        IonConstants.makeTypeDescriptor(IonConstants.tidPosInt,
                                        IonConstants.lnIsNullAtom);
    static final int ZERO_INT_TYPEDESC =
        IonConstants.makeTypeDescriptor(IonConstants.tidPosInt,
                                        IonConstants.lnNumericZero);

    static private final Long ZERO_LONG = Long.valueOf(0);

    // FIXME We can't handle Long.MIN_VALUE at encoding time.
    static private final BigInteger MIN_VALUE =
        BigInteger.valueOf(Long.MIN_VALUE + 1);

    static private final BigInteger MAX_VALUE =
        BigInteger.valueOf(Long.MAX_VALUE);

    private static final int HASH_SIGNATURE =
        IonType.INT.toString().hashCode();

    //private Long _int_value;
    private long _long_value;



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
        assert pos_getType() == IonConstants.tidPosInt
            || pos_getType() == IonConstants.tidNegInt
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

    public IonType getType()
    {
        return IonType.INT;
    }


    public int intValue()
        throws NullValueException
    {
        makeReady();
        if (_isNullValue()) throw new NullValueException();
        return (int)_long_value;
    }

    public long longValue()
        throws NullValueException
    {
        makeReady();
        if (_isNullValue()) throw new NullValueException();
        return _long_value;
    }

    public BigInteger bigIntegerValue()
        throws NullValueException
    {
        makeReady();
        if (_isNullValue()) return null;
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
        _hasNativeValue(true);
        setDirty();
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
        return IonBinary.lenIonInt(_long_value);
    }

    @Override
    protected int computeTypeDesc(int valuelen)
    {
        assert _hasNativeValue() == true;

        if (_isNullValue()) {
            return NULL_INT_TYPEDESC;
        }

        long content = _long_value;
        if (content == 0) {
            return ZERO_INT_TYPEDESC;
        }

        int hn =
            (content > 0 ? IonConstants.tidPosInt : IonConstants.tidNegInt);

        int ln = valuelen;
        if (ln > IonConstants.lnIsVarLen) {
            ln = IonConstants.lnIsVarLen;
        }

        return IonConstants.makeTypeDescriptor(hn, ln);
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
        case IonConstants.tidPosInt:
        case IonConstants.tidNegInt:
            break;
        default:
            throw new IonException("invalid type desc encountered for int");
        }

        int ln = this.pos_getLowNibble();
        switch ((0xf & ln)) {
        case IonConstants.lnIsNullAtom:
            _long_value = 0;
            _isNullValue(true);
            break;
        case 0:
            //_int_value = ZERO_LONG;
            _long_value = 0;
            _isNullValue(false);
            break;
        case IonConstants.lnIsVarLen:
            ln = reader.readVarUInt7IntValue();
            // fall through to default:
        default:
            long l = reader.readVarUInt8LongValue(ln);
            if (type == IonConstants.tidNegInt) {
                l = - l;
            }
            //_int_value = Long.valueOf(l);
            _long_value = l;
            _isNullValue(false);
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

        long l = (_long_value < 0) ? -_long_value : _long_value;

        int wlen = writer.writeVarUInt8Value(l, valueLen);
        assert wlen == valueLen;

        return;
    }


    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }
}
