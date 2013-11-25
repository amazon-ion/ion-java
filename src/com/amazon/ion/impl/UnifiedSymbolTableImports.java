// Copyright (c) 2009-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;

import com.amazon.ion.ReadOnlyValueException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

// TODO ION-385 This class should be immutable. It should only be constructed
//              during the construction of a local symbol table.

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

    // TODO ION-385 This can be optimized as we already know the size of
    //      myImports that we want to create (skipping growing of the array);
    //      we also have enough data for all other member fields.
    //      We can remove add_import_helper() and addImport() methods.
    UnifiedSymbolTableImports(List<SymbolTable> importTables)
    {
        assert importTables != null && importTables.size() >= 1
            : "there must be at least one SymbolTable for construction";

        assert importTables.get(0).isSystemTable()
            : "first imported table must be a system symtab";

        add_import_helper(importTables.get(0));

        ListIterator<SymbolTable> iter = importTables.listIterator(1);

        while (iter.hasNext())
        {
            SymbolTable importTable = iter.next();
            addImport(importTable);
        }
    }

    /**
     * @param defaultSystemSymtab
     *          the default system symtab, which will be used if the first
     *          import in {@code imports} isn't a system symtab, never null
     * @param imports
     *          the set of shared symbol tables to import; the first (and only
     *          the first) may be a system table, in which case the
     *          {@code defaultSystemSymtab is ignored}
     *
     * @throws IllegalArgumentException
     *          if any import is a local table, or if any but the first is a
     *          system table
     * @throws NullPointerException
     *          if any import is null
     */
    UnifiedSymbolTableImports(SymbolTable defaultSystemSymtab,
                              SymbolTable... imports)
    {
        assert defaultSystemSymtab.isSystemTable()
            : "defaultSystemSymtab isn't a system symtab";

        if (imports != null && imports.length > 0)
        {
            int importsIdx = 0;
            // Determine which system symtab to be used for first imported table
            if (imports[importsIdx].isSystemTable())
            {
                // Use the passed-in system symtab
                add_import_helper(imports[importsIdx]);
                importsIdx++;
            }
            else
            {
                // Use the default system symtab
                add_import_helper(defaultSystemSymtab);
            }

            // Append the rest of var-args imports
            while (importsIdx < imports.length)
            {
                addImport(imports[importsIdx]);
                importsIdx++;
            }
        }
        else
        {
            // Use the default system symtab, there are no imports
            add_import_helper(defaultSystemSymtab);
        }
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
    int getMaxIdForExport(int idx)
    {
        if (idx < 0 || idx >= myImportsCount)
        {
            throw new ArrayIndexOutOfBoundsException();
        }
        int max_id = myImports[idx].getMaxId();
        return max_id;
    }

    SymbolTable getSystemSymbolTable()
    {
        assert myImports[0].isSystemTable();
        return myImports[0];
    }
    SymbolTable[] getImportedTables()
    {
        int count = myImportsCount - 1;
        SymbolTable[] imports = new SymbolTable[count];
        if (count > 0)
        {
            System.arraycopy(myImports, 1, imports, 0, count);
        }
        return imports;
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
            _idx = 1;
            assert myImports[0].isSystemTable();
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
