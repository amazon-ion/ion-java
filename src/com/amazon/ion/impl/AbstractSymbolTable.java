/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonList;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonText;
import com.amazon.ion.IonValue;
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



    public synchronized int getMaxId()
    {
        return _maxId;
    }


    public synchronized int size()
    {
        return _byString.size();
    }


    public synchronized IonStruct getIonRepresentation()
    {
        return _symtabElement;
    }


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
            IonString textElement = new IonStringImpl();
            textElement.setValue(name);
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

    /**
     * Examines the IonValue candidateTable and checks if it is a
     * viable symbol table.  If it is this returns which type of
     * symbol table it might be.  This does not validate the full
     * contents, it examines the value type (struct), the annotations
     * and just enough contents to distiguish between the possible
     * types.
     * @param candidateTable value to be check
     * @return (@link SymbolTableType) possible type of table, or {@link SymbolTableType#INVALID}
     */
    public static SymbolTableType getSymbolTableType(IonValue candidateTable)
    {
    	SymbolTableType type = SymbolTableType.INVALID;

    	if ((candidateTable instanceof IonStruct)
    	 && (candidateTable.hasTypeAnnotation(SystemSymbolTable.ION_SYMBOL_TABLE))
    	) {
			IonStruct struct = (IonStruct)candidateTable;
			IonValue  ionname = struct.get(SystemSymbolTable.NAME);
			IonValue version = struct.get(SystemSymbolTable.VERSION);
			if (ionname == null) {
				if (version != null) {
					throw new IonException("invalid local symbol table, version is not allowed");
				}
				type = SymbolTableType.LOCAL;
			}
			else {
				if (!(ionname instanceof IonString)
				 || (ionname.isNullValue())
				 || (version != null && !(version instanceof IonInt))
				) {
					throw new IonException("invalid symbol table, the name field must be a string value and version must be an int");
				}
				String name = ((IonText)ionname).stringValue();
				if (name.equals(SystemSymbolTable.ION_1_0)) {
					type = SymbolTableType.SYSTEM;
				}
				else {
					type = SymbolTableType.SHARED;
				}
			}
    	}

    	return type;
    }
}
