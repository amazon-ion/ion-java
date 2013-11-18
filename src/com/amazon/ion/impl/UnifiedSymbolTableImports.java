// Copyright (c) 2009-2013 Amazon.com, Inc.  All rights reserved.

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
    static final int      DEFAULT_IMPORT_LENGTH = 4;

    private int           myMaxId;
    private boolean       isReadOnly;

    private int           myImportsCount;
    private SymbolTable[] myImports;
    private int[]         myImportsBaseSid;

    static final UnifiedSymbolTableImports EMPTY_IMPORTS =
        new UnifiedSymbolTableImports();

    private UnifiedSymbolTableImports()
    {
        makeReadOnly();
    }

    /**
     * Constructor, uses a system symtab to bootstrap the instance.
     *
     * @param systemSymtab must be a system symbol table
     */
    UnifiedSymbolTableImports(SymbolTable systemSymtab)
    {
        if (systemSymtab != null)
        {
            add_import_helper(systemSymtab);
        }
    }

    synchronized void makeReadOnly()
    {
        isReadOnly = true;
    }


    /**
     * @throws IllegalArgumentException if the table is local or system.
     * @throws NullPointerException if the table is null.
     */
    void addImport(SymbolTable symtab)
    {
        if (symtab.isLocalTable() || symtab.isSystemTable())
        {
            throw new IllegalArgumentException("only non-system shared tables can be imported");
        }
        add_import_helper(symtab);
    }

    private final void add_import_helper(SymbolTable symtab)
    {
        if (isReadOnly)
        {
            throw new ReadOnlyValueException(SymbolTable.class);
        }

        // (_import_count+1) so we have room for the base_sid sentinel
        if (myImports == null || (myImportsCount+1) >= myImports.length)
        {
            do
            {
                grow_imports();
            } while ((myImportsCount+1) >= myImports.length);
        }

        int idx = myImportsCount++;
        myImports[idx] = symtab;
        myImportsBaseSid[idx] = myMaxId;  // 0 based

        myMaxId += symtab.getMaxId();
        myImportsBaseSid[idx+1] = myMaxId;  // sentinel for max id loops
    }

    void grow_imports()
    {
        int oldlen = myImports == null ? 0 : myImports.length;
        int newlen = oldlen * 2;
        if (newlen < DEFAULT_IMPORT_LENGTH)
        {
            newlen = DEFAULT_IMPORT_LENGTH;
        }

        SymbolTable[] temp1 = new SymbolTable[newlen];
        int[]         temp2 = new int[newlen];

        if (oldlen > 0)
        {
            System.arraycopy(myImports, 0, temp1, 0, oldlen);
            System.arraycopy(myImportsBaseSid, 0, temp2, 0, oldlen);
        }
        myImports         = temp1;
        myImportsBaseSid = temp2;
    }

    String findKnownSymbol(int sid)
    {
        String name = null;

        if (sid > myMaxId)
        {
            // do nothing it's not found so name will be null
        }
        else
        {
            // the sid is in one of the imported tables, find out
            // which one by checking the base values
            int ii, previous_base = 0;
            for (ii=0; ii<myImportsCount; ii++)
            {
                int base = myImportsBaseSid[ii];
                if (sid <= base)
                {
                    break;
                }
                previous_base = base;
            }
            // if we run over _import_count the sid is in the last table
            int idx = sid - previous_base;
            if (idx <= getMaxIdForExport(ii-1))
            {
                name = myImports[ii-1].findKnownSymbol(idx);
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
        for (int ii=0; ii<myImportsCount; ii++)
        {
            SymbolToken tok = myImports[ii].find(text);
            if (tok != null)
            {
                int local_sid = tok.getSid();
                int local_max = getMaxIdForExport(ii);
                if (local_sid <= local_max)
                {
                    int this_base = myImportsBaseSid[ii];
                    int sid = local_sid + this_base;
                    text = tok.getText(); // Use interned instance
                    assert text != null;
                    return new SymbolTokenImpl(text, sid);
                }
            }
        }
        return null;
    }


    int getMaxId()
    {
        return myMaxId;
    }
    int getMaxIdForExportAdjusted(int idx)
    {
        int adjusted_idx = idx;
        if (this.hasSystemSymbolsImported())
        {
            adjusted_idx++;
        }
        if (idx < 0 || adjusted_idx >= myImportsCount)
        {
            throw new ArrayIndexOutOfBoundsException();
        }
        int max_id = myImports[adjusted_idx].getMaxId();
        return max_id;
    }
    int getMaxIdForExport(int idx)
    {
        if (idx < 0 || idx >= myImportsCount)
        {
            throw new ArrayIndexOutOfBoundsException();
        }
        int max_id = myImports[idx].getMaxId();
        return max_id;
    }

    private final boolean hasSystemSymbolsImported()
    {
        if (myImportsCount >= 1 && myImports[0].isSystemTable())
        {
            return true;
        }
        return false;
    }
    SymbolTable getSystemSymbolTable()
    {
        if (hasSystemSymbolsImported())
        {
            assert myImports[0].isSystemTable();
            return myImports[0];
        }
        return null;
    }
    int getImportCount()
    {
        int non_system_count = myImportsCount;
        if (hasSystemSymbolsImported())
        {
            non_system_count--;
        }
        return non_system_count;
    }
    void getImports(SymbolTable[] imports, int count)
    {
        int non_system_base_offset = 0;
        if (hasSystemSymbolsImported())
        {
            non_system_base_offset++;
        }
        System.arraycopy(myImports, non_system_base_offset, imports, 0, count);
    }


    @Override
    public String toString()
    {
        return Arrays.toString(myImports);
    }

    /**
     * Determines whether the passed-in instance has the same sequence of
     * symbol table imports as this instance. Note that equality of these
     * imports are checked using their reference, instead of their semantic
     * state.
     */
    boolean equalImports(UnifiedSymbolTableImports other)
    {
        return Arrays.equals(myImports, other.myImports);
    }

    Iterator<SymbolTable> getImportIterator()
    {
        return new ImportIterator();
    }

    final class ImportIterator implements Iterator<SymbolTable>
    {
        int _idx;

        ImportIterator()
        {
            _idx = 0;
            if (hasSystemSymbolsImported())
            {
                _idx++;
                assert myImports[0].isSharedTable();
            }
        }

        public boolean hasNext()
        {
            if (_idx < myImportsCount)
            {
                return true;
            }
            return false;
        }

        public SymbolTable next()
        {
            while (_idx < myImportsCount)
            {
                SymbolTable obj = myImports[_idx];
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
