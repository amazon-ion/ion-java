/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.EmptySymbolException;
import com.amazon.ion.IonException;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonType;
import com.amazon.ion.LocalSymbolTable;
import com.amazon.ion.NullValueException;
import com.amazon.ion.ValueVisitor;
import java.io.IOException;


/**
 * Implements the Ion <code>symbol</code> type.
 */
public final class IonSymbolImpl
    extends IonTextImpl
    implements IonSymbol
{
    static final int NULL_SYMBOL_TYPEDESC =
        IonConstants.makeTypeDescriptor(IonConstants.tidSymbol,
                                        IonConstants.lnIsNullAtom);

    private int mySid = UNKNOWN_SYMBOL_ID;

    /**
     * Constructs a <code>null.symbol</code> value.
     */
    public IonSymbolImpl()
    {
        this(NULL_SYMBOL_TYPEDESC);
    }

    public IonSymbolImpl(String name)
    {
        this(NULL_SYMBOL_TYPEDESC);
        setValue(name);
    }

    /**
     * Constructs a binary-backed symbol value.
     */
    public IonSymbolImpl(int typeDesc)
    {
        super(typeDesc);
        assert pos_getType() == IonConstants.tidSymbol;
    }


    public IonType getType()
    {
        return IonType.SYMBOL;
    }


    public String stringValue()
    {
        if (this.isNullValue()) return null;

        makeReady();
        if (this._hasNativeValue) {
            return _get_value();
        }
        return this.getSymbolTable().findSymbol(this.intValue());
    }

    public int intValue()
        throws NullValueException
    {
        validateThisNotNull();

        makeReady();

        if (mySid == UNKNOWN_SYMBOL_ID) {
            assert _hasNativeValue == true && isDirty();
            LocalSymbolTable symtab = getSymbolTable();
            if (symtab != null) {
                mySid = symtab.addSymbol(_get_value());
            }
        }

        return mySid;
    }

    @Override
    public void setValue(String value)
    {
        if ("".equals(value)) {
            throw new EmptySymbolException();
        }

        super.setValue(value);
        if (value != null) {
            mySid = UNKNOWN_SYMBOL_ID;
        }
        else {
            mySid = 0;
        }
    }

    @Override
    protected int getNativeValueLength()
    {
        assert _hasNativeValue == true;

        // We only get here if not null, and after going thru updateSymbolTable
        assert mySid > 0;
        return IonBinary.lenVarUInt8(mySid);
    }


    @Override
    protected int computeLowNibble(int valuelen)
    {
        assert _hasNativeValue == true;

        int ln = 0;
        if (mySid == UNKNOWN_SYMBOL_ID) {
            assert _hasNativeValue == true && isDirty();
            mySid = getSymbolTable().addSymbol(_get_value());
        }
        if (mySid < 1) {
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
    public void updateSymbolTable(LocalSymbolTable symtab)
    {
        // TODO do we really need to materialize?
        makeReady();

        super.updateSymbolTable(symtab);

        if (mySid < 1 && this.isNullValue() == false) {
            mySid = symtab.addSymbol(this._get_value());
        }
    }

    @Override
    protected void doMaterializeValue(IonBinary.Reader reader)
        throws IOException
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
        if (type != IonConstants.tidSymbol) {
            throw new IonException("invalid type desc encountered for value");
        }

        int ln = this.pos_getLowNibble();
        switch ((0xf & ln)) {
        case IonConstants.lnIsNullAtom:
            mySid = 0;
            _set_value(null);
            break;
        case 0:
            throw new IonException("invalid symbol id for value, must be > 0");
        case IonConstants.lnIsVarLen:
            ln = reader.readVarUInt7IntValue();
            // fall through to default:
        default:
            mySid = reader.readVarUInt8IntValue(ln);
            _set_value(getSymbolTable().findSymbol(mySid));
            break;
        }

        _hasNativeValue = true;
    }


    @Override
    protected void doWriteNakedValue(IonBinary.Writer writer, int valueLen)
        throws IOException
    {
        assert valueLen == this.getNakedValueLength();
        assert valueLen > 0;

        // We've already been through updateSymbolTable().
        assert mySid > 0;

        int wlen = writer.writeVarUInt8Value(mySid, true);
        assert wlen == valueLen;
    }


    public void accept(ValueVisitor visitor) throws Exception
    {
        makeReady();
        visitor.visit(this);
    }
}
