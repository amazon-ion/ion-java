/* Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved.
 */

 package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SystemSymbolTable;


/**
 * Implementation of a static symbol table.
 * <p>
 * Instances of this class are safe for use from multiple threads.
 */
@Deprecated
public class StaticSymbolTableImpl
    extends AbstractSymbolTable
{
    private final String _name;
    private final int    _version;


    public StaticSymbolTableImpl(IonSystem system,
                                 IonStruct symtabElt)
    {
        super(symtabElt);

        assert system != null;
        assert symtabElt.hasTypeAnnotation(SystemSymbolTable.ION_SYMBOL_TABLE);


        StringBuilder errors = new StringBuilder();

        IonValue nameElt = symtabElt.get("name");
        if (nameElt instanceof IonString)
        {
            _name = ((IonString)nameElt).stringValue();
        }
        else
        {
            _name = null;
        }

        if (_name == null || _name.length() == 0)
        {
            errors.append(" Field 'name' must be a non-empty string.");
        }

        IonValue versionElt = symtabElt.get("version");
        if (versionElt instanceof IonInt)
        {
            _version = ((IonInt)versionElt).intValue();
        }
        else if (versionElt == null)
        {
            _version = 1;
        }
        else
        {
            _version = 0;
        }

        if (_version < 1) {
            errors.append(" Field 'version' must be a positive int.");
        }

        loadSymbols(errors);

        if (errors.length() != 0) {
            errors.insert(0,
                          "Error in " + SystemSymbolTable.ION_SYMBOL_TABLE
                            + ":");
            throw new IonException(errors.toString());
        }
    }


    public boolean isLocalTable()
    {
        return false;
    }

    @Override
    public boolean isSharedTable()
    {
        return true;
    }


    @Override
    public String getName()
    {
        return this._name;
    }


    @Override
    public int getVersion()
    {
        return this._version;
    }


    public SymbolTable[] getImportedTables()
    {
        return null;
    }



    public SymbolTable getSystemSymbolTable()
    {
        return null;  // FIXME this is incorrect WRT docs in SymbolTable
    }



    public int findSymbol(String name)
    {
        Integer id;
        synchronized (this)
        {
            id = _byString.get(name);
        }

        if (id != null) return id.intValue();
        return -1;
    }


    public String findSymbol(int id)
    {
        String name = findKnownSymbol(id);
        if (name == null)
        {
            name = SystemSymbolTableImpl.unknownSymbolName(id);
        }

        return name;
    }

    public synchronized String findKnownSymbol(int id)
    {
        Integer idObj = new Integer(id);
        synchronized (this)
        {
            return _byId.get(idObj);
        }
    }


    public int addSymbol(String name)
    {
        String message = "Cannot call addSymbol on shared symbol table";
        throw new UnsupportedOperationException(message);
    }

    public void defineSymbol(String name, int id)
    {
        String message = "Cannot call defineSymbol on shared symbol table";
        throw new UnsupportedOperationException(message);
    }


    public boolean isTrivial()
    {
        return size() == 0;
    }
}
