/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonDecimal;
import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import com.amazon.ion.NullValueException;
import com.amazon.ion.ValueVisitor;
import java.io.IOException;
import java.math.BigDecimal;


/**
 * Implements the Ion <code>decimal</code> type.
 */
public final class IonDecimalImpl
    extends IonValueImpl
    implements IonDecimal
{

    static final int NULL_DECIMAL_TYPEDESC =
        IonConstants.makeTypeDescriptor(IonConstants.tidDecimal,
                                        IonConstants.lnIsNullAtom);
    static final int ZERO_DECIMAL_TYPEDESC =
        IonConstants.makeTypeDescriptor(IonConstants.tidDecimal,
                                        IonConstants.lnNumericZero);

    private BigDecimal _decimal_value;

    /**
     * Constructs a <code>null.decimal</code> element.
     */
    public IonDecimalImpl()
    {
        super(NULL_DECIMAL_TYPEDESC);
    }

    /**
     * Constructs a binary-backed element.
     */
    public IonDecimalImpl(int typeDesc)
    {
        super(typeDesc);
        assert pos_getType() == IonConstants.tidDecimal;
    }

    /**
     * makes a copy of this IonDecimal including a copy
     * of the BigDecimal value which is "naturally" immutable.
     * This calls IonValueImpl to copy the annotations and the
     * field name if appropriate.  The symbol table is not
     * copied as the value is fully materialized and the symbol
     * table is unnecessary.
     */
    @Override
    public IonDecimalImpl clone()
    {
        IonDecimalImpl clone = new IonDecimalImpl();

        makeReady();
        clone.copyAnnotationsFrom(this);
        clone.setValue(this._decimal_value);

        return clone;
    }



    public IonType getType()
    {
        return IonType.DECIMAL;
    }


    public float floatValue()
        throws NullValueException
    {
        makeReady();
        if (_decimal_value == null) throw new NullValueException();
        return _decimal_value.floatValue();
    }

    public double doubleValue()
        throws NullValueException
    {
        makeReady();
        if (_decimal_value == null) throw new NullValueException();
        return _decimal_value.doubleValue();
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
        if (_decimal_value == null) return null;
        return _decimal_value;
    }

    public void setValue(float value)
    {
        // base setValue will check for the lock
        setValue(new BigDecimal(value));
    }

    public void setValue(double value)
    {
        // base setValue will check for the lock
        setValue(new BigDecimal(value));
    }

    public void setValue(BigDecimal value)
    {
        checkForLock();
        _decimal_value = value;
        _hasNativeValue = true;
        setDirty();
    }

    @Override
    public synchronized boolean isNullValue()
    {
        if (!_hasNativeValue) return super.isNullValue();
        return (_decimal_value == null);
    }

    @Override
    protected int getNativeValueLength()
    {
        assert _hasNativeValue == true;
        return IonBinary.lenIonDecimal(_decimal_value);
    }


    @Override
    protected int computeLowNibble(int valuelen)
    {
        assert _hasNativeValue == true;

        int ln = 0;
        if (_decimal_value == null) {
            ln = IonConstants.lnIsNullAtom;
        }
        else if (_decimal_value.equals(BigDecimal.ZERO)) {
            ln = IonConstants.lnNumericZero;
        }
        else {
            ln = getNativeValueLength();
            if (ln > IonConstants.lnIsVarLen) {
                // and we get == for free, that is when len is 14
                // then it will fill in the VarLen anyway, but for
                // greater ... we have to be explicit
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
        if (type != IonConstants.tidDecimal) {
            throw new IonException("invalid type desc encountered for value");
        }

        int ln = this.pos_getLowNibble();
        switch ((0xf & ln)) {
        case IonConstants.lnIsNullAtom:
            _decimal_value = null;
            break;
        case 0:
            _decimal_value = BigDecimal.ZERO;
            break;
        case IonConstants.lnIsVarLen:
            ln = reader.readVarUInt7IntValue();
            // fall through to default:
        default:
            _decimal_value = reader.readDecimalValue(ln);
            break;
        }

        _hasNativeValue = true;
    }

    @Override
    protected void doWriteNakedValue(IonBinary.Writer writer, int valueLen) throws IOException
    {
        assert valueLen == this.getNakedValueLength();
        assert valueLen > 0;

        int wlen = writer.writeDecimalContent(_decimal_value);
        assert wlen == valueLen;

        return;
    }


    public void accept(ValueVisitor visitor) throws Exception
    {
        makeReady();
        visitor.visit(this);
    }
}
