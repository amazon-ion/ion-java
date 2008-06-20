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

    private int     mySid = UNKNOWN_SYMBOL_ID;
    private boolean _is_IonVersionMarker = false;

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

    /**
     * makes a copy of this IonString. This calls up to
     * IonTextImpl to copy the string itself and that in
     * turn calls IonValueImpl to copy 
     * the annotations and the field name if appropriate.  
     * The symbol table is not copied as the value is fully 
     * materialized and the symbol table is unnecessary.
     */
    public IonSymbolImpl clone() throws CloneNotSupportedException
    {
    	IonSymbolImpl clone = new IonSymbolImpl();
    	
    	clone.copyFrom(this);
    	clone.mySid = 0;

    	return clone;
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

        if (mySid == UNKNOWN_SYMBOL_ID) {
            assert _hasNativeValue == true && isDirty();
            LocalSymbolTable symtab = getSymbolTable();
            if (symtab == null) {
            	symtab = materializeSymbolTable();
            }
            if (symtab != null) {
                mySid = symtab.addSymbol(_get_value());
            }
        }

        return mySid;
    }

    @Override
    public void setValue(String value)
    {
    	checkForLock();

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

        if (this.isIonVersionMarker()) {
        	return IonConstants.BINARY_VERSION_MARKER_SIZE;
        }

        // We only get here if not null, and after going thru updateSymbolTable
        assert mySid > 0;
        return IonBinary.lenVarUInt8(mySid);
    }

    protected boolean isIonVersionMarker() {
    	return _is_IonVersionMarker;
    }
    protected void setIsIonVersionMarker(boolean isIVM) 
    {
    	assert SystemSymbolTable.ION_1_0.equals(this._get_value());

    	_is_IonVersionMarker = isIVM;
    	_isSystemValue = true;
        _hasNativeValue = true;
        _isMaterialized = true;

    	mySid = SystemSymbolTable.ION_1_0_SID;
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

        // the low nibble of the ion version marker is 0
        if (!isIonVersionMarker()) {
	        if (mySid < 1) {
	            ln = IonConstants.lnIsNullAtom;
	        }
	        else {
	            ln = getNativeValueLength();
	            if (ln > IonConstants.lnIsVarLen) {
	                ln = IonConstants.lnIsVarLen;
	            }
	        }
        }
        return ln;
    }

    @Override
    public void updateSymbolTable(LocalSymbolTable symtab)
    {
        // TODO do we really need to materialize?
        makeReady();

        // the super method will check for the lock
        super.updateSymbolTable(symtab);

        if (mySid < 1 && this.isNullValue() == false) {
            mySid = symtab.addSymbol(this._get_value());
        }
    }
    
    // TODO rename to getContentLength (from IonValueImpl)
    protected int getNakedValueLength() throws IOException
    {
        int len;

        if (this._is_IonVersionMarker) {
        	len = IonConstants.BINARY_VERSION_MARKER_SIZE;
        }
        else {
        	len = super.getNakedValueLength();
        }
        return len;
    }
    
    /**
     * Length of the core header.
     * @param contentLength length of the core value.
     * @return at least one.
     */
    public int getTypeDescriptorAndLengthOverhead(int contentLength) {
    	int len;
        if (this._is_IonVersionMarker || isNullValue()) {
        	len = 0;
    	}
    	else {
    		len = IonConstants.BB_TOKEN_LEN + IonBinary.lenLenFieldWithOptionalNibble(contentLength);
    	}
        return len;
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
        }
        
        _hasNativeValue = true;
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
