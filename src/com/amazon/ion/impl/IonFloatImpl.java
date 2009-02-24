/*
 * Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonType;
import com.amazon.ion.NullValueException;
import com.amazon.ion.ValueVisitor;
import java.io.IOException;
import java.math.BigDecimal;


/**
 * Implements the Ion <code>float</code> type.
 */
public final class IonFloatImpl
    extends IonValueImpl
    implements IonFloat
{
    static final int NULL_FLOAT_TYPEDESC =
        IonConstants.makeTypeDescriptor(IonConstants.tidFloat,
                                        IonConstants.lnIsNullAtom);

    static private final Double ZERO_DOUBLE = new Double(0);

    // not needed: static private final int SIZE_OF_IEEE_754_64_BITS = 8;


    private Double _float_value;

    /**
     * Constructs a <code>null.float</code> element.
     */
    public IonFloatImpl(IonSystemImpl system)
    {
        super(system, NULL_FLOAT_TYPEDESC);
        _hasNativeValue = true; // Since this is null
    }

    public IonFloatImpl(IonSystemImpl system, Double value)
    {
        super(system, NULL_FLOAT_TYPEDESC);
        _float_value = value;
        _hasNativeValue = true;
        assert isDirty();
    }

    /**
     * Constructs a binary-backed element.
     */
    public IonFloatImpl(IonSystemImpl system, int typeDesc)
    {
        super(system, typeDesc);
        assert pos_getType() == IonConstants.tidFloat;
//        assert pos_getLowNibble() == IonConstants.lnIsNullAtom
//            || pos_getLowNibble() == SIZE_OF_IEEE_754_64_BITS;
    }

    /**
     * makes a copy of this IonFloat including a copy
     * of the double value which is "naturally" immutable.
     * This calls IonValueImpl to copy the annotations and the
     * field name if appropriate.  The symbol table is not
     * copied as the value is fully materialized and the symbol
     * table is unnecessary.
     */
    @Override
    public IonFloatImpl clone()
    {
        IonFloatImpl clone = new IonFloatImpl(_system);

        makeReady();
        clone.copyAnnotationsFrom(this);
        clone.setValue(this._float_value);

        return clone;
    }

    public IonType getType()
    {
        return IonType.FLOAT;
    }


    public float floatValue()
        throws NullValueException
    {
        makeReady();
        if (_float_value == null) throw new NullValueException();
        return _float_value.floatValue();
    }

    public double doubleValue()
        throws NullValueException
    {
        makeReady();
        if (_float_value == null) throw new NullValueException();
        return _float_value.doubleValue();
    }

    @Deprecated
    public BigDecimal toBigDecimal()
        throws NullValueException
    {
        return bigDecimalValue();
    }

    public BigDecimal bigDecimalValue()
        throws NullValueException
    {
        makeReady();
        if (_float_value == null) return null;
        return new BigDecimal(_float_value.doubleValue());
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
            _hasNativeValue = true;
            setDirty();
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
        _hasNativeValue = true;
        setDirty();
    }

    @Override
    public synchronized boolean isNullValue()
    {
        if (!_hasNativeValue) return super.isNullValue();
        return (_float_value == null);
    }

    @Override
    protected int getNativeValueLength()
    {
        assert _hasNativeValue == true;
        if (_float_value == null) return 0;
        return IonBinary.lenIonFloat(_float_value);
    }

    @Override
    protected int computeLowNibble(int valuelen)
    {
        assert _hasNativeValue == true;

        int ln = 0;
        if (_float_value == null) {
            ln = IonConstants.lnIsNullAtom;
        }
        else if (_float_value.equals(0)) {
            ln = IonConstants.lnNumericZero;
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

        // we need to skip over the td to get to the good stuff
        int td = reader.read();
        assert (byte)(0xff & td) == this.pos_getTypeDescriptorByte();

        int type = this.pos_getType();
        if (type != IonConstants.tidFloat) {
            throw new IonException("invalid type desc encountered for float");
        }
        int ln = this.pos_getLowNibble();
        switch ((0xf & ln)) {
        case IonConstants.lnIsNullAtom:
            _float_value = null;
            break;
        case 0:
            _float_value = ZERO_DOUBLE;
            break;
        case IonConstants.lnIsVarLen:
            ln = reader.readVarUInt7IntValue();
            // fall through to default:
        default:
            _float_value = new Double(reader.readFloatValue(ln));
            break;
        }
        _hasNativeValue = true;
    }

    @Override
    protected void doWriteNakedValue(IonBinary.Writer writer, int valueLen) throws IOException
    {
        assert valueLen == this.getNakedValueLength();
        assert valueLen > 0;

        int wlen = writer.writeFloatValue(_float_value);
        assert wlen == valueLen;
    }


    public void accept(ValueVisitor visitor) throws Exception
    {
        makeReady();
        visitor.visit(this);
    }
}
