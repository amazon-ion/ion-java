// Copyright (c) 2010-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static com.amazon.ion.SystemSymbols.ION_1_0;
import static com.amazon.ion.SystemSymbols.ION_1_0_SID;

import com.amazon.ion.EmptySymbolException;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonType;
import com.amazon.ion.NullValueException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.UnknownSymbolException;
import com.amazon.ion.ValueVisitor;
import com.amazon.ion.impl._Private_Utils;

/**
 *
 */
public class IonSymbolLite
    extends IonTextLite
    implements IonSymbol
{
    private static final int HASH_SIGNATURE =
        IonType.SYMBOL.toString().hashCode();

    private int _sid = UNKNOWN_SYMBOL_ID;

    /**
     * Constructs a <code>null.symbol</code> value.
     */
    public IonSymbolLite(IonSystemLite system, boolean isNull)
    {
        super(system, isNull);
    }

    IonSymbolLite(IonSystemLite system, SymbolToken sym)
    {
        super(system, sym == null);
        if (sym != null)
        {
            String text = sym.getText();
            int sid = sym.getSid();
            assert text != null || sid > 0;

            if (text != null)
            {
                if ("".equals(text)) {
                    throw new EmptySymbolException();
                }
                super.setValue(text);
            }
            else
            {
                _sid = sid;
            }
        }
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
        IonSymbolLite clone = new IonSymbolLite(this._context.getSystem(), false);

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
        if (!isNullValue())
        {
            SymbolToken token = symbolValue();
            String text = token.getText();
            if (text != null)
            {
                hash ^= text.hashCode();
            }
            else
            {
                int sid = token.getSid();
                hash ^= Integer.valueOf(sid).hashCode();
            }
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

    @Deprecated
    public int getSymbolId()
        throws NullValueException
    {
        validateThisNotNull();

        if (_sid != UNKNOWN_SYMBOL_ID || isReadOnly()) {
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
        }
        SymbolToken tok = symtab.find(name);
        if (tok != null)
        {
            _sid = tok.getSid();
            _set_value(tok.getText()); // Use the interned instance of the text
        }
        return _sid;
    }


    /**
     * Get's the text of this NON-NULL symbol, finding it from our symbol
     * table if it's not yet known (and caching the result if possible).
     * <p>
     * Caller must check {@link #isNullValue()}
     *
     * @return null if symbol text is unknown.
     */
    private String _stringValue()
    {
        String name = _get_value();
        if (name == null)
        {
            assert _sid > 0;

            SymbolTable symbols = getSymbolTable();
            name = symbols.findKnownSymbol(_sid);
            if (name != null)
            {
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

    public SymbolToken symbolValue()
    {
        if (isNullValue()) return null;

        int sid = getSymbolId();
        String text = _stringValue();
        return _Private_Utils.newSymbolToken(text, sid);
    }


    @Override
    public void setValue(String value)
    {
        if ("".equals(value)) {
            throw new EmptySymbolException();
        }

        super.setValue(value);  // Calls checkForLock and _isNullValue
        _sid = UNKNOWN_SYMBOL_ID;
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

        // Don't lose the sid if that's all we have!
        if (! isNullValue() && _stringValue() != null)
        {
            _sid = UNKNOWN_SYMBOL_ID;
        }
    }

    protected void setIsIonVersionMarker(boolean isIVM)
    {
        assert (ION_1_0.equals(this._get_value()) == isIVM);

        //_is_IonVersionMarker = isIVM;
        _isIVM(isIVM);
        _isSystemValue(isIVM);

        _sid = ION_1_0_SID;
    }

    @Override
    public String stringValue()
    {
        if (isNullValue()) {
            return null;
        }
        String name = _stringValue();
        if (name == null) {
            assert(_sid > 0);
            throw new UnknownSymbolException(_sid);
        }
        return name;
    }

    @Override
    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }
}
