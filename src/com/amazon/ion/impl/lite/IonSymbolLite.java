// Copyright (c) 2010-2015 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static com.amazon.ion.SystemSymbols.ION_1_0;
import static com.amazon.ion.SystemSymbols.ION_1_0_SID;

import com.amazon.ion.EmptySymbolException;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.NullValueException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.UnknownSymbolException;
import com.amazon.ion.ValueVisitor;
import com.amazon.ion.impl._Private_Utils;
import java.io.IOException;

/**
 *
 */
final class IonSymbolLite
    extends IonTextLite
    implements IonSymbol
{
    private static final int HASH_SIGNATURE =
        IonType.SYMBOL.toString().hashCode();

    private int _sid = UNKNOWN_SYMBOL_ID;

    /**
     * @param isNull if {@code true}, constructs a {@code null.symbol} value.
     */
    public IonSymbolLite(IonContext context, boolean isNull)
    {
        super(context, isNull);
    }

    IonSymbolLite(IonSymbolLite existing, IonContext context) throws UnknownSymbolException
    {
        super(existing, context);
    }

    IonSymbolLite(IonContext context, SymbolToken sym)
    {
        super(context, sym == null);
        if (sym != null)
        {
            String text = sym.getText();
            int sid = sym.getSid();
            assert text != null || sid > 0;

            if (text != null)
            {
                if (text.length() == 0) {
                    throw new EmptySymbolException();
                }
                super.setValue(text);

                // TODO why is the sid ignored in this case?
                // Probably because we don't trust our ability to change the
                // sid if this value gets recontextualized.
                // BUT: we retain the sid on field names and annotations,
                // so one or the other is buggy.
            }
            else
            {
                _sid = sid;
            }
        }
    }

    @Override
    IonSymbolLite clone(IonContext context)
    {
        return new IonSymbolLite(this, context);
    }

    @Override
    public IonSymbolLite clone() throws UnknownSymbolException
    {
        // If this symbol has unknown text but known Sid, this symbol has no
        // semantic meaning, as such cloning should throw an exception.
        if (!isNullValue()
            && _sid != UNKNOWN_SYMBOL_ID
            && _stringValue() == null) {
            throw new UnknownSymbolException(_sid);
        }
        return clone(StubContext.wrap(getSystem()));
    }

    @Override
    int hashCode(SymbolTableProvider symbolTableProvider)
    {
        final int sidHashSalt   = 127;      // prime to salt sid
        final int textHashSalt  = 31;       // prime to salt text
        int result = HASH_SIGNATURE;

        if (!isNullValue())
        {
            SymbolToken token = symbolValue();
            String text = token.getText();

            int tokenHashCode = text == null
                ? token.getSid()  * sidHashSalt
                : text.hashCode() * textHashSalt;

            // mixing to account for small text and sid deltas
            tokenHashCode ^= (tokenHashCode << 29) ^ (tokenHashCode >> 3);

            result ^= tokenHashCode;
        }

        return hashTypeAnnotations(result, symbolTableProvider);
    }

    @Override
    public IonType getType()
    {
        return IonType.SYMBOL;
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
                // our now known symbol table so we don't have
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
    final void writeBodyTo(IonWriter writer, SymbolTableProvider symbolTableProvider)
        throws IOException
    {
        // TODO ION-320 Fix symbol handling
        // A million-dollar question is - if text is missing, do
        // we throw (cannot serialize) or do we pass the sid thru???

        // NB! This will throw if symbol is not set
        writer.writeSymbol(stringValue());
    }

    @Override
    public String stringValue()
        throws UnknownSymbolException
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

    @Override
    protected void detachFromSymbolTable()
    {
        super.detachFromSymbolTable();
        String text = _stringValue();
        if (text != null)
        {
            _sid = UNKNOWN_SYMBOL_ID;
        }
        else
        {
            assert _sid > 0;
        }
    }
}
