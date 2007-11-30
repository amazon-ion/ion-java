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

    static private final Long ZERO_LONG = new Long(0);

    private Long _int_value;


    /**
     * Constructs a <code>null.int</code> element.
     */
    public IonIntImpl()
    {
        super(NULL_INT_TYPEDESC);
    }

    /**
     * Constructs a binary-backed element.
     */
    public IonIntImpl(int typeDesc)
    {
        super(typeDesc);
        assert pos_getType() == IonConstants.tidPosInt
            || pos_getType() == IonConstants.tidNegInt
        ;
    }


    public IonType getType()
    {
        return IonType.INT;
    }


    public int intValue()
        throws NullValueException
    {
        makeReady();
        if (_int_value == null) throw new NullValueException();
        return _int_value.intValue();
    }

    public long longValue()
        throws NullValueException
    {
        makeReady();
        if (_int_value == null) throw new NullValueException();
        return _int_value.longValue();
    }

    public BigInteger toBigInteger()
        throws NullValueException
    {
        makeReady();
        if (_int_value == null) return null;
        return new BigInteger(_int_value.toString());
    }

    public void setValue(int value)
    {
        setValue(new Long(value));
    }

    public void setValue(long value)
    {
        setValue(new Long(value));
    }

    public void setValue(Number value)
    {
        if (value == null)
        {
            _int_value = null;
            _hasNativeValue = true;
            setDirty();
        }
        else
        {
            if (value instanceof BigInteger)
            {
                BigInteger big = (BigInteger) value;
                if (big.shiftRight(64).compareTo(BigInteger.ZERO) != 0) {
                    String message =
                        "int too large for this implementation: " + big;
                    throw new IonException(message);
                }
            }
            setValue(value.longValue());
        }
    }

    public void setValue(Long value)
    {
        _int_value = value;
        _hasNativeValue = true;
        setDirty();
    }

    @Override
    public synchronized boolean isNullValue()
    {
        if (!_hasNativeValue) return super.isNullValue();
        return (_int_value == null);
    }


    @Override
    protected int getNativeValueLength()
    {
        assert _hasNativeValue == true;
        // TODO streamline following; this is only call site.
        return IonBinary.lenIonInt(_int_value);
    }

    @Override
    protected int computeTypeDesc(int valuelen)
    {
        assert _hasNativeValue == true;

        if (_int_value == null) {
            return NULL_INT_TYPEDESC;
        }

        long content = _int_value.longValue();
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
    protected void doMaterializeValue(IonBinary.Reader reader) throws IOException
    {
        assert this._isPositionLoaded == true && this._buffer != null;

        // a native value trumps a buffered value
        if (_hasNativeValue) return;

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
            _int_value = null;
            break;
        case 0:
            _int_value = ZERO_LONG;
            break;
        case IonConstants.lnIsVarLen:
            ln = reader.readVarUInt7IntValue();
            // fall through to default:
        default:
            long l = reader.readVarUInt8LongValue(ln);
            if (type == IonConstants.tidNegInt) {
                l = - l;
            }
            _int_value = new Long(l);break;
        }

        _hasNativeValue = true;
    }


    @Override
    protected void doWriteNakedValue(IonBinary.Writer writer, int valueLen) throws IOException
    {
        assert valueLen == this.getNakedValueLength();
        assert valueLen > 0;

        long l = (_int_value < 0) ? -_int_value : _int_value;

        int wlen = writer.writeVarUInt8Value(l, valueLen);
        assert wlen == valueLen;

        return;
    }


    public void accept(ValueVisitor visitor) throws Exception
    {
        makeReady();
        visitor.visit(this);
    }
}
