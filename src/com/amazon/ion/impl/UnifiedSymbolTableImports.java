// Copyright (c) 2009-2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.SymbolTable;

/**
 * This class managed the referenced system symbol table and
 * the list of any other imported symbol tables used by a
 * UnifiedSymbolTable.  It includes find methods to find either
 * sids or names in the imported tables it manages.
 */
public class UnifiedSymbolTableImports
{
    static final int DEFAULT_IMPORT_LENGTH = 4;

    private int                  _max_id;

    private int                  _import_count;
    private UnifiedSymbolTable[] _imports;
    private int[]                _import_base_sid;
    private int[]                _imports_max_id;

    static final UnifiedSymbolTableImports emptyImportList = new UnifiedSymbolTableImports(null);

    UnifiedSymbolTableImports(SymbolTable systemSymbols) {
        if (systemSymbols != null) {
            if (!(systemSymbols instanceof UnifiedSymbolTable)) {
                throw new UnsupportedOperationException("all symbol tables must be instances of UnifiedSymbolTable, currently");
            }
            add_import_helper((UnifiedSymbolTable)systemSymbols, -1);
        }
    }

    /**
     * @throws IllegalArgumentException if the table is local or system.
     * @throws NullPointerException if the table is null.
     */
    void addImport(UnifiedSymbolTable symtab, int maxId)
    {
        if (symtab.isLocalTable() || symtab.isSystemTable()) {
            throw new IllegalArgumentException("only non-system shared tables can be imported");
        }
        add_import_helper(symtab, maxId);
    }

    private final void add_import_helper(UnifiedSymbolTable symtab, int maxId)
    {
        // (_import_count+1) so we have room for the base_sid sentinel
        if (_imports == null || (_import_count+1) >= _imports.length) {
            do {
                grow_imports();
            } while ((_import_count+1) >= _imports.length);
        }

        int idx = _import_count++;

        _imports[idx] = symtab;
        _import_base_sid[idx] = _max_id;  // 0 based
        _imports_max_id[idx]  =  maxId;   // may be -1 or 0 for none

        if (maxId < 0) {
            maxId = symtab.getMaxId();
        }

        _max_id += maxId;
        _import_base_sid[idx+1] = _max_id;  // sentinel for max id loops
    }

    void grow_imports() {
        int oldlen = _imports == null ? 0 : _imports.length;
        int newlen = oldlen * 2;
        if (newlen < DEFAULT_IMPORT_LENGTH) {
            newlen = DEFAULT_IMPORT_LENGTH;
        }

        UnifiedSymbolTable[] temp1 = new UnifiedSymbolTable[newlen];
        int[]                temp2 = new int[newlen];
        int[]                temp3 = new int[newlen];

        if (oldlen > 0) {
            System.arraycopy(_imports, 0, temp1, 0, oldlen);
            System.arraycopy(_import_base_sid, 0, temp2, 0, oldlen);
            System.arraycopy(_imports_max_id, 0, temp3, 0, oldlen);

        }
        _imports         = temp1;
        _import_base_sid = temp2;
        _imports_max_id  = temp3;
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
            if (idx <= getMaxIdForIdChecking(ii-1)) {
                name = _imports[ii-1].findKnownSymbol(idx);
            }
        }

        return name;
    }

    int findSymbol(String name)
    {
        int ii, sid = -1;

        for (ii=0; ii<_import_count; ii++) {
            int local_sid = _imports[ii].findSymbol(name);
            if (local_sid > 0) {
                int this_base = _import_base_sid[ii];
                // int next_base = (ii+1 >= _import_count) ? _max_id : _import_base_sid[ii+1];
                //int local_max = next_base - this_base;
                int local_max = getMaxIdForIdChecking(ii); // this_base + _imports_max_id[ii] - 1;
                if (local_sid <= local_max) {
                    sid = local_sid + this_base;
                    break;
                }
            }
        }
        return sid;
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
        int max_id = _imports_max_id[adjusted_idx];
        return max_id;
    }
    int getMaxIdForExport(int idx) {
        if (idx < 0 || idx >= _import_count) {
            throw new ArrayIndexOutOfBoundsException();
        }
        int max_id = _imports_max_id[idx];
        return max_id;
    }
    int getMaxIdForIdChecking(int idx) {
        int max_id = getMaxIdForExport(idx);

        if (max_id < 1) {
            // is this the last import table?
            if (idx == _import_count - 1) {
                max_id = _max_id; // then it gets the global max
            }
            else {
                // otherwise it gets the delta
                max_id = _import_base_sid[idx+1];
            }
        }
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
    void getImports(UnifiedSymbolTable[] imports, int count)
    {
        int non_system_base_offset = 0;
        if (hasSystemSymbolsImported()) {
            non_system_base_offset++;
        }
        System.arraycopy(_imports, non_system_base_offset, imports, 0, count);
    }
}
