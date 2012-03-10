// Copyright (c) 2009-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;

import com.amazon.ion.ReadOnlyValueException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import java.util.Arrays;
import java.util.Iterator;

/**
 * This class managed the referenced system symbol table and
 * the list of any other imported symbol tables used by a
 * UnifiedSymbolTable.  It includes find methods to find either
 * sids or names in the imported tables it manages.
 */
final class UnifiedSymbolTableImports
{
    static final int DEFAULT_IMPORT_LENGTH = 4;

    private int           _max_id;
    private boolean       _is_read_only;

    private int           _import_count;
    private SymbolTable[] _imports;
    private int[]         _import_base_sid;

    static final UnifiedSymbolTableImports emptyImportList =
        new UnifiedSymbolTableImports();

    private UnifiedSymbolTableImports()
    {
        makeReadOnly();
    }

    UnifiedSymbolTableImports(SymbolTable systemSymbols) {
        if (systemSymbols != null) {
            add_import_helper(systemSymbols);
        }
    }

    /**
     * checks the _is_read_only flag and if the flag is set
     * this throws an error.  This is used by the various
     * methods that may modify a value.
     */
    private void verify_not_read_only() {
        if (_is_read_only) {
            throw new ReadOnlyValueException(SymbolTable.class);
        }
    }

    synchronized void makeReadOnly() {
        _is_read_only = true;
    }


    /**
     * @throws IllegalArgumentException if the table is local or system.
     * @throws NullPointerException if the table is null.
     */
    void addImport(SymbolTable symtab)
    {
        if (symtab.isLocalTable() || symtab.isSystemTable()) {
            throw new IllegalArgumentException("only non-system shared tables can be imported");
        }
        add_import_helper(symtab);
    }

    private final void add_import_helper(SymbolTable symtab)
    {
        assert symtab.isReadOnly();

        verify_not_read_only();

        // (_import_count+1) so we have room for the base_sid sentinel
        if (_imports == null || (_import_count+1) >= _imports.length) {
            do {
                grow_imports();
            } while ((_import_count+1) >= _imports.length);
        }

        int idx = _import_count++;
        _imports[idx] = symtab;
        _import_base_sid[idx] = _max_id;  // 0 based

        _max_id += symtab.getMaxId();
        _import_base_sid[idx+1] = _max_id;  // sentinel for max id loops
    }

    void grow_imports() {
        int oldlen = _imports == null ? 0 : _imports.length;
        int newlen = oldlen * 2;
        if (newlen < DEFAULT_IMPORT_LENGTH) {
            newlen = DEFAULT_IMPORT_LENGTH;
        }

        SymbolTable[] temp1 = new SymbolTable[newlen];
        int[]         temp2 = new int[newlen];

        if (oldlen > 0) {
            System.arraycopy(_imports, 0, temp1, 0, oldlen);
            System.arraycopy(_import_base_sid, 0, temp2, 0, oldlen);

        }
        _imports         = temp1;
        _import_base_sid = temp2;
    }

    String findKnownSymbol(int sid)
    {
        String name = null;

        if (sid > _max_id) {
            // do nothing it's not found so name will be null
        }
        else {
            // the sid is in one of the imported tables, find out
            // which one by checking the base values
            int ii, previous_base = 0;
            for (ii=0; ii<_import_count; ii++) {
                int base = _import_base_sid[ii];
                if (sid <= base) {
                    break;
                }
                previous_base = base;
            }
            // if we run over _import_count the sid is in the last table
            int idx = sid - previous_base;
            if (idx <= getMaxIdForExport(ii-1)) {
                name = _imports[ii-1].findKnownSymbol(idx);
            }
        }

        return name;
    }

    int findSymbol(String name)
    {
        SymbolToken tok = find(name);
        return (tok == null ? UNKNOWN_SYMBOL_ID : tok.getSid());
    }

    /**
     * Finds a symbol already interned by an import, returning the lowest
     * known SID.
     * <p>
     * This method will not necessarily return the same instance given the
     * same input.
     *
     * @param text the symbol text to find.
     *
     * @return the interned symbol (with both text and SID),
     * or {@code null} if it's not defined by an imported table.
     */
    SymbolToken find(String text)
    {
        for (int ii=0; ii<_import_count; ii++) {
            SymbolToken tok = _imports[ii].find(text);
            if (tok != null)
            {
                int local_sid = tok.getSid();
                int local_max = getMaxIdForExport(ii);
                if (local_sid <= local_max) {
                    int this_base = _import_base_sid[ii];
                    int sid = local_sid + this_base;
                    text = tok.getText(); // Use interned instance
                    assert text != null;
                    return new SymbolTokenImpl(text, sid);
                }
            }
        }
        return null;
    }


    int getMaxId() {
        return _max_id;
    }
    int getMaxIdForExportAdjusted(int idx) {
        int adjusted_idx = idx;
        if (this.hasSystemSymbolsImported()) {
            adjusted_idx++;
        }
        if (idx < 0 || adjusted_idx >= _import_count) {
            throw new ArrayIndexOutOfBoundsException();
        }
        int max_id = _imports[adjusted_idx].getMaxId();
        return max_id;
    }
    int getMaxIdForExport(int idx) {
        if (idx < 0 || idx >= _import_count) {
            throw new ArrayIndexOutOfBoundsException();
        }
        int max_id = _imports[idx].getMaxId();
        return max_id;
    }

    private final boolean hasSystemSymbolsImported() {
        if (_import_count >= 1 && _imports[0].isSystemTable()) {
            return true;
        }
        return false;
    }
    SymbolTable getSystemSymbolTable() {
        if (hasSystemSymbolsImported()) {
            assert _imports[0].isSystemTable();
            return _imports[0];
        }
        return null;
    }
    int getImportCount() {
        int non_system_count = _import_count;
        if (hasSystemSymbolsImported()) {
            non_system_count--;
        }
        return non_system_count;
    }
    void getImports(SymbolTable[] imports, int count)
    {
        int non_system_base_offset = 0;
        if (hasSystemSymbolsImported()) {
            non_system_base_offset++;
        }
        System.arraycopy(_imports, non_system_base_offset, imports, 0, count);
    }


    @Override
    public String toString()
    {
        return Arrays.toString(_imports);
    }

    Iterator<SymbolTable> getImportIterator() {
        return new ImportIterator();
    }

    final class ImportIterator implements Iterator<SymbolTable>
    {
        int _idx;

        ImportIterator() {
            _idx = 0;
            if (hasSystemSymbolsImported()) {
                _idx++;
                assert _imports[0].isSharedTable();
            }
        }

        public boolean hasNext()
        {
            if (_idx < _import_count) {
                return true;
            }
            return false;
        }

        public SymbolTable next()
        {
            while (_idx < _import_count) {
                SymbolTable obj = _imports[_idx];
                _idx++;
                assert ! obj.isSystemTable();
                return obj;
            }
            return null;
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }
}
