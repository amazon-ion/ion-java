/*
 * Copyright 2010-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion.impl.lite;

import static software.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static software.amazon.ion.SystemSymbols.ION_1_0;
import static software.amazon.ion.SystemSymbols.ION_1_0_SID;

import java.io.IOException;
import software.amazon.ion.EmptySymbolException;
import software.amazon.ion.IonType;
import software.amazon.ion.IonWriter;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.UnknownSymbolException;
import software.amazon.ion.ValueVisitor;
import software.amazon.ion.impl.PrivateIonSymbol;
import software.amazon.ion.impl.PrivateUtils;

final class IonSymbolLite
    extends IonTextLite
    implements PrivateIonSymbol
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
        return clone(ContainerlessContext.wrap(getSystem()));
    }

    @Override
    int hashCode(SymbolTableProvider symbolTableProvider)
    {
        final int sidHashSalt   = 127;      // prime to salt sid
        final int textHashSalt  = 31;       // prime to salt text
        int result = HASH_SIGNATURE;

        if (!isNullValue())
        {
            SymbolToken token = symbolValue(symbolTableProvider);
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
            assert _sid > 0;

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

    private int resolveSymbolId()
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

    public SymbolToken symbolValue(SymbolTableProvider symbolTableProvider)
    {
        if (isNullValue()) return null;

        int sid = resolveSymbolId();
        String text = _stringValue(symbolTableProvider);
        return PrivateUtils.newSymbolToken(text, sid);
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
        // TODO amznlabs/ion-java#27 Fix symbol handling
        // A million-dollar question is - if text is missing, do
        // we throw (cannot serialize) or do we pass the sid thru???

        SymbolToken symbol = symbolValue(symbolTableProvider);
        writer.writeSymbolToken(symbol);
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
