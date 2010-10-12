// Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static com.amazon.ion.impl.IonConstants.BB_TOKEN_LEN;

import com.amazon.ion.EmptySymbolException;
import com.amazon.ion.IonException;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonType;
import com.amazon.ion.NullValueException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SystemSymbolTable;
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

    private static final int NULL_SYMBOL_ID = 0;

    private static final int HASH_SIGNATURE =
        IonType.SYMBOL.toString().hashCode();

    /**
     * SID is zero when this is null.symbol
     */
    private int     mySid = UNKNOWN_SYMBOL_ID;
    private boolean _is_IonVersionMarker = false;

    /**
     * Constructs a <code>null.symbol</code> value.
     */
    public IonSymbolImpl(IonSystemImpl system)
    {
        this(system, NULL_SYMBOL_TYPEDESC);
        _hasNativeValue(true); // Since this is null
    }

    public IonSymbolImpl(IonSystemImpl system, String name)
    {
        this(system, NULL_SYMBOL_TYPEDESC);
        setValue(name);
    }

    /**
     * Constructs a binary-backed symbol value.
     */
    public IonSymbolImpl(IonSystemImpl system, int typeDesc)
    {
        super(system, typeDesc);
        assert pos_getType() == IonConstants.tidSymbol;
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
    public IonSymbolImpl clone()
    {
        IonSymbolImpl clone = new IonSymbolImpl(_system);

        clone.copyFrom(this);
        clone.mySid = UNKNOWN_SYMBOL_ID;

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
        return IonType.SYMBOL;
    }


    public String stringValue()
    {
        if (this.isNullValue()) return null;

        makeReady();

        String value = _get_value();

        // this only decodes a valid value if the symbol
        // is of the form $<digits>
        int sid = UnifiedSymbolTable.decodeIntegerSymbol(value);
        if (this._hasNativeValue()) {
            if (sid != UnifiedSymbolTable.UNKNOWN_SYMBOL_ID) {
                SymbolTable symbols = this.getSymbolTable();
                if (symbols != null) {
                     value = symbols.findSymbol(sid);
                }
            }
        }
        else {
            // we have to look up the string for this
            sid = this.getSymbolId();
            SymbolTable symbols = this.getSymbolTable();
            if (symbols != null) {
                 value = symbols.findSymbol(sid);
            }
        }
        return value;
    }

    @Deprecated
    public int intValue()
        throws NullValueException
    {
        return getSymbolId();
    }

    public int getSymbolId()
        throws NullValueException
    {
        validateThisNotNull();

        makeReady();

        // TODO this can be streamlined
        if (mySid == UNKNOWN_SYMBOL_ID) {
            assert _hasNativeValue() == true && isDirty();
            SymbolTable symtab = getSymbolTable();
            if (symtab == null) {
                symtab = materializeSymbolTable();
            }
            if (symtab != null) {
                String name = _get_value();
                if (symtab.isLocalTable())
                {
                    mySid = symtab.addSymbol(name);
                }
                else
                {
                    mySid = symtab.findSymbol(name);
                }
            }
        }

        assert mySid != 0;
        return mySid;
    }

    @Override
    public void setValue(String value)
    {
        if ("".equals(value)) {
            throw new EmptySymbolException();
        }

        super.setValue(value);  // Calls checkForLock
        if (value != null) {
            mySid = UNKNOWN_SYMBOL_ID;
        }
        else {
            mySid = NULL_SYMBOL_ID;
        }
    }

    @Override
    protected int getNativeValueLength()
    {
        assert _hasNativeValue() == true;

        if (this.isIonVersionMarker()) {
            return IonConstants.BINARY_VERSION_MARKER_SIZE;
        }

        assert mySid >= 0;
        return IonBinary.lenVarUInt8(mySid);
    }

    protected boolean isIonVersionMarker() {
        return _is_IonVersionMarker;
    }
    protected void setIsIonVersionMarker(boolean isIVM)
    {
        assert SystemSymbolTable.ION_1_0.equals(this._get_value());

        _is_IonVersionMarker = isIVM;
        _isSystemValue(true);
        _hasNativeValue(true);
        _isMaterialized(true);

        mySid = SystemSymbolTable.ION_1_0_SID;
    }

    @Override
    protected int computeLowNibble(int valuelen)
    {
        assert _hasNativeValue() == true;

        if (mySid == UNKNOWN_SYMBOL_ID) {
            assert _hasNativeValue() == true && isDirty();
            String name = _get_value();
            mySid = (name == null
                         ? NULL_SYMBOL_ID
                         : getSymbolTable().addSymbol(name));
        }
        assert mySid >= 0;

        int ln;
        if (isIonVersionMarker()) {
            // the low nibble of the ion version marker is always 0
            ln = 0;
        }
        else if (mySid == NULL_SYMBOL_ID) {
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
    public SymbolTable populateSymbolValues(SymbolTable symtab)
    {
        // TODO do we really need to materialize?
        makeReady();

        // the super method will check for the lock
        symtab = super.populateSymbolValues(symtab);

        if (mySid < 1 && this.isNullValue() == false) {
            assert _hasNativeValue() == true && isDirty();

            String s = this._get_value();
            mySid = this.resolveSymbol(s);
            if (mySid < 1) {
                symtab = this.addSymbol(s, symtab);
                mySid = this.resolveSymbol(s);
            }
        }

        return symtab;
    }

    @Override
    protected int getNakedValueLength() throws IOException
    {
        int len;

        if (this._is_IonVersionMarker) {  // FIXME: WHAT IF THE ivm HAS ANNOTATIONS ??
            len = IonConstants.BINARY_VERSION_MARKER_SIZE - IonConstants.BB_TOKEN_LEN;
        }
        else {
            len = super.getNakedValueLength();
        }
        return len;
    }

    /**
     * Length of the core header.  We handle version markers as a one-byte TD
     * plus a three-byte value.
     *
     * @param contentLength length of the core value.
     * @return at least one.
     */
    @Override
    public int getTypeDescriptorAndLengthOverhead(int contentLength) {
        int len = BB_TOKEN_LEN;
        if (! this._is_IonVersionMarker) {
            len += IonBinary.lenLenFieldWithOptionalNibble(contentLength);
        }
        return len;
    }

    @Override
    void detachFromSymbolTable()
    {
        String name = stringValue();  // Force materialization
        this.mySid = (name == null ? 0 : UNKNOWN_SYMBOL_ID);
        super.detachFromSymbolTable();
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

        int tdb = this.pos_getTypeDescriptorByte();
        if ((tdb & 0xff) == (IonConstants.BINARY_VERSION_MARKER_1_0[0] & 0xff)) {
            mySid = SystemSymbolTable.ION_1_0_SID;
            _set_value(SystemSymbolTable.ION_1_0);
            // we need to skip over the binary marker, we've read the first
            // byte and we checked the contents long before we got here so ...
            reader.skip(IonConstants.BINARY_VERSION_MARKER_SIZE - 1);
        }
        else {
            int type = this.pos_getType();
            if (type != IonConstants.tidSymbol) {
                throw new IonException("invalid type desc encountered for value");
            }

            int ln = this.pos_getLowNibble();
            switch ((0xf & ln)) {
            case IonConstants.lnIsNullAtom:
                mySid = NULL_SYMBOL_ID;
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
        }

        _hasNativeValue(true);
    }


    @Override
    protected void doWriteNakedValue(IonBinary.Writer writer, int valueLen)
        throws IOException
    {
        assert valueLen == this.getNakedValueLength();
        assert valueLen > 0;

        if (this.isIonVersionMarker()) {
            writer.write(IonConstants.BINARY_VERSION_MARKER_1_0);
            assert valueLen == IonConstants.BINARY_VERSION_MARKER_SIZE;
        }
        else {
            // We've already been through updateSymbolTable().
            assert mySid > 0;

            int wlen = writer.writeVarUInt8Value(mySid, valueLen);
            assert wlen == valueLen;
        }
    }

    /**
     * Precondition: the token is up to date, the buffer is positioned properly,
     * and enough space is available.
     *
     * @return the cumulative position delta at the end of this value.
     * @throws IOException
     */
    @Override
    protected int writeValue(IonBinary.Writer writer,
                             int cumulativePositionDelta)
        throws IOException
    {
        // we look for the IonVersionMarker stamp for special
        // treatment (the 4 byte value) otherwise we delegate
        if (this._is_IonVersionMarker) {
            writer.write(IonConstants.BINARY_VERSION_MARKER_1_0);
        }
        else {
            cumulativePositionDelta = super.writeValue(writer, cumulativePositionDelta);
        }
        return cumulativePositionDelta;
    }


    public void accept(ValueVisitor visitor) throws Exception
    {
        makeReady();
        visitor.visit(this);
    }
}
