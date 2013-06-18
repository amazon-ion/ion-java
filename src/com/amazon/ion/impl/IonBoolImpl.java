// Copyright (c) 2007-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl._Private_IonConstants.lnIsNullAtom;
import static com.amazon.ion.impl._Private_IonConstants.makeTypeDescriptor;
import static com.amazon.ion.impl._Private_IonConstants.tidBoolean;

import com.amazon.ion.IonBool;
import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import com.amazon.ion.NullValueException;
import com.amazon.ion.ValueVisitor;
import java.io.IOException;


/**
 * Implements the Ion <code>bool</code> type.
 */
final class IonBoolImpl
    extends IonValueImpl
    implements IonBool
{
    static final int NULL_BOOL_TYPEDESC =
        makeTypeDescriptor(tidBoolean, lnIsNullAtom);

    private static final int HASH_SIGNATURE =
        IonType.BOOL.toString().hashCode();

    /**
     * Optimizes out a function call for a const result
     */
    protected static final int TRUE_HASH
            = HASH_SIGNATURE ^ (16777619 * Boolean.TRUE.hashCode());

    /**
     * Optimizes out a function call for a const result
     */
    protected static final int FALSE_HASH
            = HASH_SIGNATURE ^ (16777619 * Boolean.FALSE.hashCode());

    // TODO we can probably take less space by using two booleans
    // private boolean isNull, value;
    // private Boolean _bool_value;

    /**
     * Constructs a null bool value.
     */
    public IonBoolImpl(IonSystemImpl system)
    {
        super(system, NULL_BOOL_TYPEDESC);
        _hasNativeValue(true); // Since this is null
    }

    /**
     * Constructs a binary-backed symbol value.
     */
    public IonBoolImpl(IonSystemImpl system, int typeDesc)
    {
        super(system, typeDesc);
        assert pos_getType() == _Private_IonConstants.tidBoolean;
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
        clone.setValue(this._isBoolTrue());
        clone._isNullValue(this._isNullValue());

        return clone;
    }

    public IonType getType()
    {
        return IonType.BOOL;
    }

    @Override
    public int hashCode()
    {
        int result = HASH_SIGNATURE;

        if (!isNullValue())
        {
            result = booleanValue() ? TRUE_HASH : FALSE_HASH;
        }

        return hashTypeAnnotations(result);
    }

    public boolean booleanValue()
        throws NullValueException
    {
        makeReady();
        if (_isNullValue()) { // if (_bool_value == null) {
            throw new NullValueException();
        }
        return _isBoolTrue();
    }

    public void setValue(boolean b)
    {
        // the called setValue will check if this is locked
        setValue(Boolean.valueOf(b));
    }

    public void setValue(Boolean b)
    {
        checkForLock();
        if (b == null) {
            _isBoolTrue(false);
            _isNullValue(true);
        }
        else {
            _isBoolTrue(b.booleanValue());
            _isNullValue(false);
        }
        //_bool_value = b;
        //_isNullValue(b == null);
        _hasNativeValue(true);
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
        assert _hasNativeValue() == true;

        int ln = 0;
        if (_isNullValue()) {
            ln = _Private_IonConstants.lnIsNullAtom;
        }
        else if (_isBoolTrue()) {
            ln = _Private_IonConstants.lnBooleanTrue;
        }
        else {
            ln = _Private_IonConstants.lnBooleanFalse;
        }
        return ln;
    }

    //public boolean oldisNullValue()
    //{
    //    if (!_hasNativeValue()) return super.oldisNullValue();
    //    return (_bool_value == null);
    //}

    @Override
    protected void doMaterializeValue(IonBinary.Reader reader)
    {
        assert this._isPositionLoaded() == true && this._buffer != null;

        // a native value trumps a buffered value
        if (_hasNativeValue()) return;

        // the reader will have been positioned for us
        assert reader.position() == this.pos_getOffsetAtValueTD();

        // decode the low nibble to get the boolean value
        int ln = this.pos_getLowNibble();
        switch (ln) {
        case _Private_IonConstants.lnIsNullAtom:
            //_bool_value = null;
            _isNullValue(true);
            _isBoolTrue(false);
            break;
        case _Private_IonConstants.lnBooleanFalse:
            //_bool_value = Boolean.FALSE;
            _isNullValue(false);
            _isBoolTrue(false);
            break;
        case _Private_IonConstants.lnBooleanTrue:
            //_bool_value = Boolean.TRUE;
            _isNullValue(false);
            _isBoolTrue(true);
            break;
        default:
            throw new IonException("malformed binary boolean value");
        }

        _hasNativeValue(true);
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
