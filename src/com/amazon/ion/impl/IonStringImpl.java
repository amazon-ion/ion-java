// Copyright (c) 2007-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl._Private_IonConstants.lnIsNullAtom;
import static com.amazon.ion.impl._Private_IonConstants.makeTypeDescriptor;
import static com.amazon.ion.impl._Private_IonConstants.tidString;

import com.amazon.ion.IonException;
import com.amazon.ion.IonString;
import com.amazon.ion.IonType;
import com.amazon.ion.ValueVisitor;
import java.io.IOException;


/**
 * Implements the Ion <code>string</code> type.
 */
final class IonStringImpl
    extends IonTextImpl
    implements IonString
{

    static final int NULL_STRING_TYPEDESC =
        makeTypeDescriptor(tidString, lnIsNullAtom);

    private static final int HASH_SIGNATURE =
        IonType.STRING.toString().hashCode();

    /**
     * Constructs a <code>null.string</code> value.
     */
    public IonStringImpl(IonSystemImpl system)
    {
        super(system, NULL_STRING_TYPEDESC);
        _hasNativeValue(true); // Since this is null
    }


    /**
     * Constructs a binary-backed string value.
     */
    public IonStringImpl(IonSystemImpl system, int typeDesc)
    {
        super(system, typeDesc);
        assert pos_getType() == _Private_IonConstants.tidString;
    }

    /**
     * makes a copy of this IonString. This calls up to
     * IonTextImpl to copy the string itself and that in
     * turn calls IonValueImpl to copy
     * the annotations and the field name if appropriate.
     * The symbol table is not copied as the value is fully
     * materialized and the symbol table is unnecessary.
     */
    @Override
    public IonStringImpl clone()
    {
        IonStringImpl clone = new IonStringImpl(_system);

        clone.copyFrom(this);

        return clone;
    }

    /**
     * Implements {@link Object#hashCode()} consistent with equals. This
     * implementation uses the hash of the string value XOR'ed with a constant.
     *
     * @return  An int, consistent with the contracts for
     *          {@link Object#hashCode()} and {@link Object#equals(Object)}.
     */
    @Override
    public int hashCode() {
        int hash = HASH_SIGNATURE;
        if (!isNullValue())  {
            hash ^= stringValue().hashCode();
        }
        return hash;
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
        assert _hasNativeValue() == true;
        return IonBinary.lenIonString(_get_value());
    }


    @Override
    protected int computeLowNibble(int valuelen)
    {
        assert _hasNativeValue() == true;

        int ln = 0;
        if (_get_value() == null) {
            ln = _Private_IonConstants.lnIsNullAtom;
        }
        else {
            ln = getNativeValueLength();
            if (ln > _Private_IonConstants.lnIsVarLen) {
                ln = _Private_IonConstants.lnIsVarLen;
            }
        }
        return ln;
    }

    @Override
    protected void doMaterializeValue(IonBinary.Reader reader) throws IOException
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
        if (type != _Private_IonConstants.tidString) {
            throw new IonException("invalid type desc encountered for value");
        }

        int ln = this.pos_getLowNibble();
        switch ((0xf & ln)) {
        case _Private_IonConstants.lnIsNullAtom:
            _set_value(null);
            break;
        case 0:
            _set_value("");
            break;
        case _Private_IonConstants.lnIsVarLen:
            ln = reader.readVarUIntAsInt();
            // fall through to default:
        default:
            _set_value(reader.readString(ln));
            break;
        }

        _hasNativeValue(true);
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
