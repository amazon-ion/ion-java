/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion.impl.lite;

import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static com.amazon.ion.SystemSymbols.ION_1_0;
import static com.amazon.ion.SystemSymbols.ION_1_0_SID;

import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.NullValueException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.UnknownSymbolException;
import com.amazon.ion.ValueVisitor;
import com.amazon.ion.impl._Private_IonSymbol;
import com.amazon.ion.impl._Private_Utils;
import java.io.IOException;


final class IonSymbolLite
    extends IonTextLite
    implements _Private_IonSymbol
{
    private static final int HASH_SIGNATURE =
        IonType.SYMBOL.toString().hashCode();

    private int _sid = UNKNOWN_SYMBOL_ID;

    /**
     * @param isNull if {@code true}, constructs a {@code null.symbol} value.
     */
    IonSymbolLite(ContainerlessContext context, boolean isNull)
    {
        super(context, isNull);
    }

    IonSymbolLite(IonSymbolLite existing, IonContext context) throws UnknownSymbolException
    {
        super(existing, context);
    }

    IonSymbolLite(ContainerlessContext context, SymbolToken sym)
    {
        super(context, sym == null);
        if (sym != null)
        {
            String text = sym.getText();
            int sid = sym.getSid();
            assert text != null || sid >= 0;

            if (text != null)
            {
                super.setValue(text);
                // TODO [amazon-ion/ion-java/issues/27] - needs consistent handling, when to retain SID's vs ignore
            }
            else
            {
                // TODO [amazon-ion/ion-java/issues/223] - needs consistent handling, resolution against context symbol table
                _sid = sid;
                // there *is* an encoding present so we must update
                _isSymbolIdPresent(true);
            }
        }
    }

    @Override
    IonValueLite shallowClone(IonContext context)
    {
        IonSymbolLite clone = new IonSymbolLite(this, context);
        if(this._sid == 0) {
            clone._sid = 0;
        }
        return clone;
    }

    @Override
    public IonSymbolLite clone() throws UnknownSymbolException
    {
        // If this symbol has unknown text but known Sid, this symbol has no
        // semantic meaning, as such cloning should throw an exception.
        if (!isNullValue()
            && _sid != UNKNOWN_SYMBOL_ID && _sid != 0
            && _stringValue() == null) {
            throw new UnknownSymbolException(_sid);
        }
        return (IonSymbolLite) shallowClone(ContainerlessContext.wrap(getSystem()));
    }

    @Override
    int hashSignature() {
        return HASH_SIGNATURE;
    }

    @Override
    int scalarHashCode()
    {
        final int sidHashSalt   = 127;      // prime to salt sid
        final int textHashSalt  = 31;       // prime to salt text
        int result = HASH_SIGNATURE;
        int tokenHashCode = _text_value == null
            ? _sid  * sidHashSalt
            : _text_value.hashCode() * textHashSalt;

        // mixing to account for small text and sid deltas
        tokenHashCode ^= (tokenHashCode << 29) ^ (tokenHashCode >> 3);

        result ^= tokenHashCode;

        return hashTypeAnnotations(result);
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
        return getSymbolId(null);
    }

    private int getSymbolId(SymbolTableProvider symbolTableProvider)
        throws NullValueException
    {
        validateThisNotNull();

        if (_sid != UNKNOWN_SYMBOL_ID || isReadOnly()) {
            return _sid;
        }

        SymbolTable symtab =
                symbolTableProvider != null ? symbolTableProvider.getSymbolTable()
                                            : getSymbolTable();
        if (symtab == null) {
            symtab = getSystem().getSystemSymbolTable();
        }
        assert(symtab != null);

        String name = _get_value();
        // TODO [amazon-ion/ion-java/issues/27] - needs consistent handling, when to retain SID's vs ignore (here memoizing SID on read)
        if (!symtab.isLocalTable())
        {
            setSID(symtab.findSymbol(name));
            if (_sid > 0 || isReadOnly()) {
                return _sid;
            }
        }
        SymbolToken tok = symtab.find(name);
        if (tok != null)
        {
            setSID(tok.getSid());
            _set_value(tok.getText()); // Use the interned instance of the text
        }
        return _sid;
    }

    private void setSID(int sid)
    {
        _sid = sid;
        if (_sid > 0)
        {
            cascadeSIDPresentToContextRoot();
        }
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
        return _stringValue(new LazySymbolTableProvider(this));
    }

    private String _stringValue(SymbolTableProvider symbolTableProvider)
    {
        String name = _get_value();
        if (name == null)
        {
            assert _sid >= 0;

            SymbolTable symbols = symbolTableProvider.getSymbolTable();
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
        return symbolValue(new LazySymbolTableProvider(this));
    }

    public SymbolToken symbolValue(SymbolTableProvider symbolTableProvider)
    {
        if (isNullValue()) return null;

        int sid = getSymbolId(symbolTableProvider);
        String text = _stringValue(symbolTableProvider);
        return _Private_Utils.newSymbolToken(text, sid);
    }


    @Override
    public void setValue(String value)
    {
        super.setValue(value);  // Calls checkForLock and _isNullValue
        _sid = UNKNOWN_SYMBOL_ID;
    }

    protected boolean isIonVersionMarker() {
        return _isIVM();
    }

    @Override
    boolean attemptClearSymbolIDValues()
    {
        boolean allSymbolIDsCleared = super.attemptClearSymbolIDValues();

        // if there is no value, or there is already no SID - there is no clear action
        if (isNullValue() || _sid == UNKNOWN_SYMBOL_ID)
        {
            // no behavior required - value has no SID value to clear
        }
        // if there is text, clear the SID
        else if (_stringValue() != null)
        {
            _sid = UNKNOWN_SYMBOL_ID;
        }
        else
        {
            // TODO [amazon-ion/ion-java/issues/223] - needs consistent handling, resolution against context symbol table
            // there is not text, so we can't clear the SID.
            allSymbolIDsCleared = false;
        }

        return allSymbolIDsCleared;
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
        // TODO amazon-ion/ion-java/issues/27 Fix symbol handling
        // A million-dollar question is - if text is missing, do
        // we throw (cannot serialize) or do we pass the sid thru???

        // NB! This will throw if symbol is not set
        String text = _stringValue(symbolTableProvider);
        if (text != null) {
            writer.writeSymbol(text);
        } else {
            writer.writeSymbolToken(_Private_Utils.newSymbolToken(getSymbolId(symbolTableProvider)));
        }

    }

    @Override
    public String stringValue()
        throws UnknownSymbolException
    {
        return stringValue(new LazySymbolTableProvider(this));
    }

    private String stringValue(SymbolTableProvider symbolTableProvider)
        throws UnknownSymbolException
    {
        if (isNullValue()) {
            return null;
        }
        String name = _stringValue(symbolTableProvider);
        if (name == null) {
            assert(_sid >= 0);
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
