/* Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved.
 */

 package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonList;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.LocalSymbolTable;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SystemSymbolTable;


/**
 * TODO define thread-safety.
 */
public class LocalSymbolTableImpl
    extends AbstractSymbolTable
    implements LocalSymbolTable
{
    private static class ImportedTable
    {
        /** Can be null if the imported table isn't available. */
        SymbolTable _table;
        int               _localOffset;
        int               _localMaxId;
    }

    private final SymbolTable _systemSymbols;
    private final ImportedTable[]   _importedTables;


    /**
     * Constructs an empty symbol table.
     *
     * @param systemSymbolTable must be a system symbol table as per
     *   {@link SymbolTable#isSystemTable()}.
     */
    public LocalSymbolTableImpl(SymbolTable systemSymbolTable)
    {
        assert systemSymbolTable != null;
        assert systemSymbolTable.isSystemTable();

        _systemSymbols = systemSymbolTable;
        _importedTables = null;
        _maxId = systemSymbolTable.getMaxId();

        _symtabElement = new IonStructImpl();

        ((IonStructImpl)_symtabElement).setSymbolTable(this);
        _symtabElement.addTypeAnnotation(SystemSymbolTable.ION_SYMBOL_TABLE); // cas 25 apr 2008 was: systemSymbolTable.getSystemId());

        _symbolsStruct = new IonStructImpl();
        _symtabElement.put(SystemSymbolTable.SYMBOLS, _symbolsStruct);
    }


    /**
     * Constructs an empty, anonymous symbol table.
     */
    public LocalSymbolTableImpl(IonSystem system)
    {
    	// assert system != null;
        this(system.getSystemSymbolTable());
    }

    /**
     * Constructs an empty symbol table from its Ion representation.
     *
     * @param systemSymbolTable must be a system symbol table as per
     *   {@link SymbolTable#isSystemTable()}.
     */
    public LocalSymbolTableImpl(IonSystem system,
                                IonCatalog catalog,
                                IonStruct asymboltable,
                                SymbolTable systemSymbolTable)
    {
        assert system != null;
        assert systemSymbolTable != null;
        assert systemSymbolTable.isSystemTable();
        assert asymboltable.hasTypeAnnotation(SystemSymbolTable.ION_SYMBOL_TABLE);  // was: ION_1_0

        _systemSymbols = systemSymbolTable;
        _symtabElement = asymboltable;
        _maxId = _systemSymbols.getMaxId();

        StringBuilder errors = new StringBuilder();
        _importedTables = loadImports(system, catalog, errors);
        loadSymbols(errors);

        if (errors.length() != 0) {
            errors.insert(0, "Error in local symbol table:");
            throw new IonException(errors.toString());
        }

        if (_symbolsStruct == null) {
            _symbolsStruct = system.newEmptyStruct();
            _symtabElement.put(SystemSymbolTable.SYMBOLS, _symbolsStruct);
        }
    }

    public final boolean isLocalTable()
    {
        return true;
    }

    // Not synchronized since member is final.
    public SymbolTable getSystemSymbolTable()
    {
        return _systemSymbols;
    }


    public synchronized int addSymbol(String name) {
        int id = findSymbol(name);
        if (id > 0) return id;

        id = ++_maxId;
        doDefineSymbol(name, id);
        return id;
    }

    public synchronized void defineSymbol(String name, int id)
    {
        Integer existingId = _byString.get(name);
        if (existingId != null)
        {
            if (existingId.intValue() == id)
            {
                return;
            }
            throw new IonException("Cannot redefine symbol '" + name + "'");
        }

        // Now we know that name isn't defined, but check the id
        if (_byId.containsKey(id))
        {
            throw new IonException("Cannot redefine symbol " + id);
        }

        doDefineSymbol(name, id);
    }


    @Override
    protected void doDefineSymbol(String name, int id)
    {
        String systemSymbol = _systemSymbols.findKnownSymbol(id);
        if (systemSymbol != null)
        {
            String message =
                "Cannot hide system symbol '" + systemSymbol + "' with id " +
                id;
            throw new IonException(message);
        }

        super.doDefineSymbol(name, id);
    }

    public synchronized int findSymbol(String name) {
        int sid = _systemSymbols.findSymbol(name);
        if (sid != IonSymbol.UNKNOWN_SYMBOL_ID) return sid;

        synchronized (this)
        {
            Integer id = _byString.get(name);
            if (id != null) return id.intValue();
        }

        if (_importedTables != null)
        {
            for (int i = 0; i < _importedTables.length; i++)
            {
                ImportedTable imported = _importedTables[i];
                SymbolTable table = imported._table;
                if (table != null)
                {
                    sid = table.findSymbol(name);
                    if (sid > 0)
                    {
                        sid = sid + imported._localOffset;

                        // Make sure the imported sid isn't shadowed by a local
                        // mapping, in which case this name is hidden.
                        synchronized (this)
                        {
                            if (_byId.get(sid) == null) {
                                return sid;
                            }
                        }
                    }
                }
            }
        }

        return IonSymbol.UNKNOWN_SYMBOL_ID;
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

    public String findKnownSymbol(int id)
    {
        String name = _systemSymbols.findKnownSymbol(id);
        if (name != null) return name;

        synchronized (this)
        {
            name = _byId.get(new Integer(id));
        }
        if (name != null) return name;

        if (_importedTables != null)
        {
            for (int i = 0; i < _importedTables.length; i++)
            {
                ImportedTable imported = _importedTables[i];

                if (id <= imported._localMaxId) {
                    if (imported._table != null) {
                        int importedSid = id - imported._localOffset;
                        name = imported._table.findKnownSymbol(importedSid);
                    }
                    return name;
                }
            }
        }

        return null;
    }


    public boolean hasImports()
    {
        assert _importedTables == null || _importedTables.length > 0;

        return (_importedTables != null);
    }


    public SymbolTable getImportedTable(String name)
    {
        if (_importedTables == null) return null;

        for (int i = 0; i < _importedTables.length; i++)
        {
            SymbolTable current = _importedTables[i]._table;
            if (current != null && name.equals(current.getName()))
            {
                return current;
            }
        }
        return null;
    }

    public SymbolTable[] getImportedTables()
    {
        if (_importedTables == null) return new SymbolTable[0];

        SymbolTable[] symtabs = new SymbolTable[_importedTables.length];

        for (int i = 0; i < _importedTables.length; i++)
        {
            symtabs[i] = _importedTables[i]._table;
        }
        return symtabs;
    }


    public boolean isCompatible(SymbolTable other)
    {
        // FIXME implement LocalSymbolTable.isCompatible()
        return false;
    }


    //=========================================================================
    // Helpers

    private static String stringValue(IonValue value)
    {
        if (value instanceof IonString)
        {
            return ((IonString)value).stringValue();
        }
        return null;
    }


    private ImportedTable[] loadImports(IonSystem system,
                                        IonCatalog catalog,
                                        StringBuilder errors)
    {
        assert _maxId > 1;

        ImportedTable[] imports = null;

        IonValue importsElement =
            _symtabElement.get(SystemSymbolTable.IMPORTS);
        if (importsElement instanceof IonList) {
            IonList importsList = (IonList) importsElement;
            if (! importsList.isNullValue() && importsList.size() != 0)
            {
                int count = importsList.size();
                imports = new ImportedTable[count];
                for (int i = 0; i < count; i++)
                {
                    IonValue importElement = importsList.get(i);

                    if (!(importElement instanceof IonStruct)
                        || importElement.isNullValue())
                    {
                        errors.append(" Field 'imports' contains bad entry: " +
                                      importElement);
                    }
                    else
                    {
                        IonStruct importStruct = (IonStruct) importElement;
                        ImportedTable imported =
                            loadImport(system, catalog, errors, importStruct);
                        imports[i] = imported;
                    }
                }
            }
        }
        else if (importsElement != null) {
            errors.append(" Field 'imports' must be a list of structs.");
        }

        return imports;
    }


    private ImportedTable loadImport(IonSystem system,
                                     IonCatalog catalog,
                                     StringBuilder errors,
                                     IonStruct importStruct)
    {
        IonValue nameElement = importStruct.get(SystemSymbolTable.NAME);
        String name = stringValue(nameElement);
        if (name == null || name.length() == 0)
        {
            errors.append(" Imported tables must have non-empty name.");
            return null;
        }

        IonValue versionElement = importStruct.get(SystemSymbolTable.VERSION);
        if (! (versionElement instanceof IonInt)
                 || versionElement.isNullValue())
        {
            errors.append(" Imported tables must have version int.");
            return null;
        }
        int version = ((IonInt)versionElement).intValue();

        SymbolTable importedTable = catalog.getTable(name, version);


        IonValue maxIdElement = importStruct.get(SystemSymbolTable.MAX_ID);
        int maxId = 0;
        if (maxIdElement == null || maxIdElement.isNullValue())
        {
            if (importedTable == null)
            {
                // Couldn't find the table, must know max_id
                String message =
                    " Missing max_id on import of " + nameElement +
                    "; catalog has no version of it.";
                errors.append(message);
                return null;
            }

            if (version != importedTable.getVersion())
            {
                String message =
                    " Missing max_id on import of " + nameElement +
                    " version " + version + "; catalog returned version " +
                    importedTable.getVersion() + '.';
                errors.append(message);
                return null;
            }

            maxId = importedTable.getMaxId();
            assert maxId > 0;

            IonInt maxInt = system.newInt(maxId);
            importStruct.put(SystemSymbolTable.MAX_ID, maxInt);
        }
        else
        {
            if (maxIdElement instanceof IonInt)
            {
                maxId = ((IonInt)maxIdElement).intValue();
            }

            if (maxId < 1)
            {
                errors.append(" max_id on import of " + nameElement +
                              " is not a positive int");
                return null;
            }
        }

        ImportedTable imported = new ImportedTable();
        imported._table       = importedTable;
        imported._localOffset = _maxId;

        _maxId += maxId;
        imported._localMaxId = _maxId;
        return imported;
    }
}
