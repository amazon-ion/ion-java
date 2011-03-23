// Copyright (c) 2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;

import com.amazon.ion.EmptySymbolException;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonType;
import com.amazon.ion.NullValueException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SystemSymbolTable;
import com.amazon.ion.ValueVisitor;

/**
 *
 */
public class IonSymbolLite
    extends IonTextLite
    implements IonSymbol
{
    private static final int NULL_SYMBOL_ID = 0;

    private static final int HASH_SIGNATURE =
        IonType.SYMBOL.toString().hashCode();

    /**
     * SID is zero when this is null.symbol
     */
    private int     _sid = UNKNOWN_SYMBOL_ID;

    /**
     * Constructs a <code>null.symbol</code> value.
     */
    public IonSymbolLite(IonSystemLite system, boolean isNull)
    {
        super(system, isNull);
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
    public IonSymbolLite clone()
    {
        IonSymbolLite clone = new IonSymbolLite(this._context.getSystemLite(), false);

        clone.copyFrom(this);
        clone._sid = UNKNOWN_SYMBOL_ID;

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

    @Override
    public IonType getType()
    {
        return IonType.SYMBOL;
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

        if (_sid != UNKNOWN_SYMBOL_ID) {
            return _sid;
        }

        SymbolTable symtab = getSymbolTable();
        if (symtab == null) {
            symtab = getSystem().getSystemSymbolTable();
        }
        assert(symtab != null);

        String name = _get_value();
        if (!symtab.isLocalTable())
        {
            _sid = symtab.findSymbol(name);
            if (_sid > 0 || isReadOnly()) {
                return _sid;
            }
            symtab = _context.getLocalSymbolTable(this);
        }
        _sid = symtab.addSymbol(name);
        assert _sid > 0;
        return _sid;
    }

    @Override
    public void setValue(String value)
    {
        if ("".equals(value)) {
            throw new EmptySymbolException();
        }

        // check for a $<digits> form of the symbol name
        // really this should be done down in the parser
        boolean is_encoded_sid = false;
        int sid = UNKNOWN_SYMBOL_ID;
        if (value != null && value.length() > 1 && value.charAt(0) == '$') {
            if (Character.isDigit(value.charAt(1))) {
                is_encoded_sid = true;
                for (int ii=2; ii<value.length(); ii++) {
                    if (!Character.isDigit(value.charAt(ii))) {
                        is_encoded_sid = false;
                        break;
                    }
                }
                if (is_encoded_sid) {
                    sid = Integer.parseInt(value.substring(1));
                    if (sid < 1) {
                        // we reject $0 (as an encoded sid)
                        is_encoded_sid = false;
                    }
                    else {
                        value = null;
                    }
                }
            }
        }

        super.setValue(value);  // Calls checkForLock
        if (is_encoded_sid) {
            _sid = sid;
            _isNullValue(false);
        }
        else if (value != null) {
            _isNullValue(false);
            _sid = UNKNOWN_SYMBOL_ID;
        }
        else {
            _isNullValue(true);
            _sid = NULL_SYMBOL_ID;
        }
    }

    protected boolean isIonVersionMarker() {
        return _isIVM();
    }

    @Override
    public SymbolTable populateSymbolValues(SymbolTable symbols)
    {
        if (_isLocked()) {
            // we can't, and don't need to, update symbol id's
            // for a locked value - there are none - so do nothing here
        }
        else {
            // this will check whether or not we're locked
            // it also looks up the symbol table if it is
            // null
            symbols = super.populateSymbolValues(symbols);

            if (!isNullValue()) {
                String name = _get_value();
                if (name != null) {
                    symbols = resolve_symbol(symbols, name);
                    // seems a bit lame, but we can't return two values
                    // from resolve and there's no point looking it up
                    // now since we'll look it up if anyone asks for it
                    // and if no one asks we don't need it
                    _sid = UNKNOWN_SYMBOL_ID;
                }
            }
        }
        return symbols;
    }

    @Override
    void clearSymbolIDValues()
    {
        super.clearSymbolIDValues();
        _sid = UNKNOWN_SYMBOL_ID;
    }

    protected void setIsIonVersionMarker(boolean isIVM)
    {
        assert (SystemSymbolTable.ION_1_0.equals(this._get_value()) == isIVM);

        //_is_IonVersionMarker = isIVM;
        _isIVM(isIVM);
        _isSystemValue(isIVM);

        _sid = SystemSymbolTable.ION_1_0_SID;
    }

    @Override
    public String stringValue()
    {
        if (isNullValue()) {
            return null;
        }
        String name = _get_value();
        if (name == null) {
            assert(_sid > 0);
            SymbolTable symbols = getSymbolTable();
            name = symbols.findKnownSymbol(_sid);
            if (name == null) {
                name = symbols.findSymbol(_sid);
            }
            else {
                // if this is a mutable value we'll hang onto
                // our know known symbol table so we don't have
                // to look it up again.
                // If the value is immutable, honor that contract.
                if (_isLocked() == false) {
                    _set_value(name);
                }
            }
        }
        return name;
    }

    @Override
    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }
}
