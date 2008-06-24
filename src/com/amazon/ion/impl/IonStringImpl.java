/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonString;
import com.amazon.ion.IonType;
import com.amazon.ion.ValueVisitor;
import java.io.IOException;


/**
 * Implements the Ion <code>string</code> type.
 */
public final class IonStringImpl
    extends IonTextImpl
    implements IonString
{

    static final int NULL_STRING_TYPEDESC =
        IonConstants.makeTypeDescriptor(IonConstants.tidString,
                                        IonConstants.lnIsNullAtom);


    /**
     * Constructs a <code>null.string</code> value.
     */
    public IonStringImpl()
    {
        super(NULL_STRING_TYPEDESC);
    }


    /**
     * Constructs a binary-backed string value.
     */
    public IonStringImpl(int typeDesc)
    {
        super(typeDesc);
        assert pos_getType() == IonConstants.tidString;
    }

    /**
     * makes a copy of this IonString. This calls up to
     * IonTextImpl to copy the string itself and that in
     * turn calls IonValueImpl to copy 
     * the annotations and the field name if appropriate.  
     * The symbol table is not copied as the value is fully 
     * materialized and the symbol table is unnecessary.
     */
    public IonStringImpl clone() throws CloneNotSupportedException
    {
    	IonStringImpl clone = new IonStringImpl();
    	
    	clone.copyFrom(this);
    	
    	return clone;
    }

    public IonType getType()
    {
        return IonType.STRING;
    }


    public String stringValue()
    {
        if (this.isNullValue()) return null;

        makeReady();
        return _get_value();
    }


    @Override
    protected int getNativeValueLength()
    {
        assert _hasNativeValue == true;
        return IonBinary.lenIonString(_get_value());
    }


    @Override
    protected int computeLowNibble(int valuelen)
    {
        assert _hasNativeValue == true;

        int ln = 0;
        if (_get_value() == null) {
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

        // we need to skip over the td to get to the good stuff
        int td = reader.read();
        assert (byte)(0xff & td) == this.pos_getTypeDescriptorByte();

        int type = this.pos_getType();
        if (type != IonConstants.tidString) {
            throw new IonException("invalid type desc encountered for value");
        }

        int ln = this.pos_getLowNibble();
        switch ((0xf & ln)) {
        case IonConstants.lnIsNullAtom:
            _set_value(null);
            break;
        case 0:
            _set_value("");
            break;
        case IonConstants.lnIsVarLen:
            ln = reader.readVarUInt7IntValue();
            // fall through to default:
        default:
            _set_value(reader.readString(ln));
            break;
        }

        _hasNativeValue = true;
    }


    @Override
    protected void doWriteNakedValue(IonBinary.Writer writer, int valueLen) throws IOException
    {
        assert valueLen == this.getNakedValueLength();
        assert valueLen > 0;
        String s = _get_value();

        int wlen = writer.writeStringData(s);
        assert wlen == valueLen;
        return;
    }

    public void accept(ValueVisitor visitor) throws Exception
    {
        makeReady();
        visitor.visit(this);
    }

}
