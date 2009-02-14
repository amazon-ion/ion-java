/*
 * Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonList;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SystemSymbolTable;
import com.amazon.ion.util.Printer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public abstract class AbstractSymbolTable
    implements SymbolTable
{
    protected Map<Integer, String> _byId     = new HashMap<Integer, String>();
    protected Map<String, Integer> _byString = new HashMap<String, Integer>();

    protected int _maxId;

    /**
     * The Ion representation of this table.
     */
    protected IonStruct _symtabElement;

    /**
     * The <code>symbols</code> field of the {@link #_symtabElement}
     * when the symbols are provided as a struct with manually assigned
     * symbol ids for the fieldid values.
     */
    protected IonStruct _symbolsStruct;

    /**
     * The <code>symbols</code> field of the {@link #_symtabElement} when
     * the symbols are provided as a list of strings.
     */
    protected IonList _symbolsList;

    /**
     *
     */
    public AbstractSymbolTable()
    {

    }

    /**
     *
     */
    public AbstractSymbolTable(IonStruct representation)
    {
        _symtabElement = representation;
    }



    public boolean isSharedTable()
    {
        return false;
    }


    public boolean isSystemTable()
    {
        return false;
    }


    public String getName()
    {
        return null;
    }


    public int getVersion()
    {
        return 0;
    }


    public String getSystemId()
    {
        // FIXME I'm not sure this is correct...
        return SystemSymbolTable.ION_1_0;
    }

    public synchronized int getMaxId()
    {
        return _maxId;
    }


    public synchronized int size()
    {
        return _byString.size();
    }


    public void writeTo(IonWriter writer)
        throws IOException
    {
        writer.writeValue(_symtabElement);
    }


    public synchronized IonStruct getIonRepresentation()
    {
        return _symtabElement;
    }


    @Override
    public String toString()
    {
        StringBuilder buf = new StringBuilder();
        buf.append('[');
        buf.append(getClass().getName());
        buf.append(' ');
        try
        {
            Printer p = new Printer();
            synchronized (this)
            {
                p.print(_symtabElement, buf);
            }
        }
        catch (IOException e)
        {
            // Can't do much.
            buf.append("/* IOException!! */");
        }
        buf.append(']');
        return buf.toString();
    }


    //=========================================================================
    // Helpers

    /**
     * Defines a NEW symbol, asserts that it doesn't exist.
     * @param name
     * @param id
     */
    protected void doDefineSymbol(String name, int id)
    {
        assert ! _byString.containsKey(name);
        assert ! _byId.containsKey(id);

        // TODO throw on attempt to redefine $ion_ symbol ???

        Integer idObj = new Integer(id);
        _byString.put(name, idObj);
        _byId.put(idObj, name);

        if (_symbolsStruct != null)  // null while constructing
        {
            IonString textElement = _symbolsStruct.getSystem().newString(name);
            String fieldName = SystemSymbolTableImpl.unknownSymbolName(id);
            _symbolsStruct.put(fieldName, textElement);
        }
    }


    /**
     * Initializes {@link #_symbolsStruct} from {@link #_symtabElement}.
     * Does not create a symbols field if it doesn't exist!
     */
    protected void loadSymbols(StringBuilder errors)
    {
        IonValue symbolsElt = _symtabElement.get("symbols");
        if (symbolsElt instanceof IonStruct)
        {
            // Keep this._symbolsStruct == null for now so defineSymbol doesn't
            // attempt to add the symbol back into the struct.

            IonStruct symbolsStruct = (IonStruct) symbolsElt;
            if (! symbolsStruct.isNullValue())
            {
                Printer printer = new Printer();

                for (IonValue v : symbolsStruct)
                {
                    // TODO check for ill-formed field name.

                    String symbolName = null;
                    if (v instanceof IonString) {
                        symbolName = ((IonString)v).stringValue();
                    }
                    if (symbolName == null || symbolName.length() == 0) {
                        errors.append(" Bad symbol name ");
                        try
                        {
                            printer.print(v, errors);
                        }
                        catch (IOException e) { }
                        errors.append('.');
                        continue;
                    }

                    int sid = ((IonValueImpl)v).getFieldId();

                    doDefineSymbol(symbolName, sid);

                    if (sid > _maxId) _maxId = sid;
                }
            }
            this._symbolsStruct = symbolsStruct;
        }
        else if (symbolsElt instanceof IonList) {
            Printer printer = new Printer();
            IonList symbolsList = (IonList) symbolsElt;
            if (_maxId == 0) {
                _maxId = symbolsElt.getSymbolTable().getSystemSymbolTable().getMaxId();
            }

            for (IonValue v : symbolsList)
            {
                // TODO check for ill-formed field name.
                String symbolName = null;
                if (v instanceof IonString) {
                    symbolName = ((IonString)v).stringValue();
                }
                if (symbolName == null || symbolName.length() == 0) {
                    errors.append(" Bad symbol name ");
                    try {
                        printer.print(v, errors);
                    }
                    catch (IOException e) { }
                    errors.append('.');
                    continue;
                }

                int sid = _maxId + 1;

                doDefineSymbol(symbolName, sid);

                if (sid > _maxId) _maxId = sid;
            }
            this._symbolsList = symbolsList;
        }
        else if (symbolsElt != null) {
            errors.append(" Field 'symbols' must be a struct or list.");
        }
    }
}
