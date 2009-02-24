// Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonBool;
import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import com.amazon.ion.NullValueException;
import com.amazon.ion.ValueVisitor;
import java.io.IOException;


/**
 * Implements the Ion <code>bool</code> type.
 */
public final class IonBoolImpl
    extends IonValueImpl
    implements IonBool
{
    static final int NULL_BOOL_TYPEDESC =
        IonConstants.makeTypeDescriptor(IonConstants.tidBoolean,
                                        IonConstants.lnIsNullAtom);

    // TODO we can probably take less space by using two booleans
    // private boolean isNull, value;
    private Boolean _bool_value;

    /**
     * Constructs a null bool value.
     */
    public IonBoolImpl(IonSystemImpl system)
    {
        super(system, NULL_BOOL_TYPEDESC);
        _hasNativeValue = true; // Since this is null
    }

    /**
     * Constructs a binary-backed symbol value.
     */
    public IonBoolImpl(IonSystemImpl system, int typeDesc)
    {
        super(system, typeDesc);
        assert pos_getType() == IonConstants.tidBoolean;
    }

    /**
     * makes a copy of this IonBool including a copy
     * of the Boolean value which is "naturally" immutable.
     * This calls IonValueImpl to copy the annotations and the
     * field name if appropriate.  The symbol table is not
     * copied as the value is fully materialized and the symbol
     * table is unnecessary.
     */
    @Override
    public IonBoolImpl clone()
    {
        IonBoolImpl clone = new IonBoolImpl(_system);

        makeReady();
        clone.copyAnnotationsFrom(this);
        clone.setValue(this._bool_value);

        return clone;
    }

    public IonType getType()
    {
        return IonType.BOOL;
    }


    public boolean booleanValue()
        throws NullValueException
    {
        makeReady();
        if (_bool_value == null) throw new NullValueException();
        return _bool_value;
    }

    public void setValue(boolean b)
    {
        // the called setValue will check if this is locked
        setValue(Boolean.valueOf(b));
    }

    public void setValue(Boolean b)
    {
        checkForLock();
        _bool_value = b;
        _hasNativeValue = true;
        setDirty();
    }

    @Override
    protected int getNativeValueLength()
    {
        return 0;
    }

    @Override
    protected int computeLowNibble(int valuelen)
    {
        assert _hasNativeValue == true;

        int ln = 0;
        if (_bool_value == null) {
            ln = IonConstants.lnIsNullAtom;
        }
        else if (_bool_value.equals(true)) {
            ln = IonConstants.lnBooleanTrue;
        }
        else {
            ln = IonConstants.lnBooleanFalse;
        }
        return ln;
    }


    @Override
    public synchronized boolean isNullValue()
    {
        if (!_hasNativeValue) return super.isNullValue();
        return (_bool_value == null);
    }

    @Override
    protected void doMaterializeValue(IonBinary.Reader reader)
    {
        assert this._isPositionLoaded == true && this._buffer != null;

        // a native value trumps a buffered value
        if (_hasNativeValue) return;

        // the reader will have been positioned for us
        assert reader.position() == this.pos_getOffsetAtValueTD();

        // decode the low nibble to get the boolean value
        int ln = this.pos_getLowNibble();
        switch (ln) {
        case IonConstants.lnIsNullAtom:
            _bool_value = null;
            break;
        case IonConstants.lnBooleanFalse:
            _bool_value = Boolean.FALSE;
            break;
        case IonConstants.lnBooleanTrue:
            _bool_value = Boolean.TRUE;
            break;
        default:
            throw new IonException("malformed binary boolean value");
        }

        _hasNativeValue = true;
    }


    @Override
    protected void doWriteNakedValue(IonBinary.Writer writer, int valueLen) throws IOException
    {
        throw new IonException("call not needed!");
    }


    public void accept(ValueVisitor visitor)
        throws Exception
    {
        makeReady();
        visitor.visit(this);
    }
}
