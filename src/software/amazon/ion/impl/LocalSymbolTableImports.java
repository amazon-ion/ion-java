/*
 * Copyright 2009-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion.impl;

import static software.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;

import java.util.Arrays;
import java.util.List;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;

/**
 * This class manages the system symbol table and any shared symbol table(s)
 * imported by a local symbol table. It provides "find" methods to find
 * either symbol Ids or names in the imported tables.
 * <p>
 * This class is <b>immutable</b>, and hence safe for use by multiple threads.
 */
// TODO amznlabs/ion-java#37 Create specialized class to handle the common case where
//      there are zero or one imported non-system shared symtab(s).
final class LocalSymbolTableImports
{
    /**
     * The symtabs imported by a local symtab, never null or empty. The first
     * symtab must be a system symtab, the rest must be non-system shared
     * symtabs.
     */
    private final SymbolTable[] myImports;

    /**
     * The maxId of all imported tables, i.e., the sum of all maxIds declared
     * by symtabs in {@link #myImports}.
     */
    private final int           myMaxId;

    /**
     * The base Sid of each symtab in {@link #myImports} in parallel, i.e.,
     * {@link #myBaseSids}[0] references {@link #myImports}[0]. Must be
     * the same length as {@link #myImports}.
     */
    private final int[]         myBaseSids;

    //==========================================================================
    // Constructor(s) and static factory methods
    //==========================================================================

    /**
     * Constructor, takes the passed-in {@code importTables} containing the
     * imported symtabs.
     *
     * @param importTables
     *          the imported symtabs, must contain at least one element; the
     *          first element must be a system symtab, the rest must be
     *          non-system shared symtabs
     *
     * @throws IllegalArgumentException
     *          if any import is a local table, or if any but the first is a
     *          system table
     * @throws NullPointerException
     *          if any import is null
     */
    LocalSymbolTableImports(List<SymbolTable> importTables)
    {
        int importTablesSize = importTables.size();

        myImports = importTables.toArray(new SymbolTable[importTablesSize]);
        myBaseSids = new int[importTablesSize];
        myMaxId = prepBaseSids(myBaseSids, myImports);
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
    LocalSymbolTableImports(SymbolTable defaultSystemSymtab,
                            SymbolTable... imports)
    {
        assert defaultSystemSymtab.isSystemTable()
            : "defaultSystemSymtab isn't a system symtab";

        if (imports != null && imports.length > 0)
        {
            if (imports[0].isSystemTable())
            {
                // copy imports as-is
                myImports = new SymbolTable[imports.length];
                System.arraycopy(imports, 0, myImports, 0, imports.length);
            }
            else
            {
                // use defaultSystemSymtab and append imports
                myImports = new SymbolTable[imports.length + 1];
                myImports[0] = defaultSystemSymtab;
                System.arraycopy(imports, 0, myImports, 1, imports.length);
            }
        }
        else
        {
            // use defaultSystemSymtab only
            myImports = new SymbolTable[] { defaultSystemSymtab };
        }

        myBaseSids = new int[myImports.length];
        myMaxId = prepBaseSids(myBaseSids, myImports);
    }

    /**
     * Collects the necessary maxId info. from the passed-in {@code imports}
     * and populates the {@code baseSids} array.
     *
     * @return the sum of all imports' maxIds
     *
     * @throws IllegalArgumentException
     *          if any symtab beyond the first is a local or system symtab
     */
    private static int prepBaseSids(int[] baseSids, SymbolTable[] imports)
    {
        SymbolTable firstImport = imports[0];

        assert firstImport.isSystemTable()
            : "first symtab must be a system symtab";

        baseSids[0] = 0;
        int total = firstImport.getMaxId();

        for (int i = 1; i < imports.length; i++)
        {
            SymbolTable importedTable = imports[i];

            if (importedTable.isLocalTable() || importedTable.isSystemTable())
            {
                String message = "only non-system shared tables can be imported";
                throw new IllegalArgumentException(message);
            }

            baseSids[i] = total;
            total += imports[i].getMaxId();
        }

        return total;
    }

    //==========================================================================

    String findKnownSymbol(int sid)
    {
        String name = null;

        if (sid <= myMaxId)
        {
            int i, previousBaseSid = 0;
            for (i = 1; i < myImports.length; i++)
            {
                int baseSid = myBaseSids[i];
                if (sid <= baseSid)
                {
                    break;
                }
                previousBaseSid = baseSid;
            }

            // if we run over myImports.length, the sid is in the last symtab
            int importScopedSid = sid - previousBaseSid;
            name = myImports[i-1].findKnownSymbol(importScopedSid);
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
     * @param text the symbol text to find
     *
     * @return
     *          the interned symbol (with both text and SID), or {@code null}
     *          if it's not defined by an imported table
     */
    SymbolToken find(String text)
    {
        for (int i = 0; i < myImports.length; i++)
        {
            SymbolTable importedTable = myImports[i];
            SymbolToken tok = importedTable.find(text);

            if (tok != null)
            {
                int sid = tok.getSid() + myBaseSids[i];
                text = tok.getText(); // Use interned instance

                assert text != null;

                return new SymbolTokenImpl(text, sid);
            }
        }
        return null;
    }

    int getMaxId()
    {
        return myMaxId;
    }

    /**
     * Gets the sole system symtab.
     */
    SymbolTable getSystemSymbolTable()
    {
        assert myImports[0].isSystemTable();
        return myImports[0];
    }

    /**
     * Gets all non-system shared symtabs (if any).
     *
     * @return a newly allocated copy of the imported symtabs
     */
    SymbolTable[] getImportedTables()
    {
        int count = myImports.length - 1; // we don't include system symtab
        SymbolTable[] imports = new SymbolTable[count];
        if (count > 0)
        {
            // defensive copy
            System.arraycopy(myImports, 1, imports, 0, count);
        }
        return imports;
    }

    /**
     * Returns the {@link #myImports} member field without making a copy.
     * <p>
     * <b>Note:</b> Callers must not modify the resulting SymbolTable array!
     * This will violate the immutability property of this class.
     *
     * @return
     *          the backing array of imported symtabs, as-is; the first element
     *          is a system symtab, the rest are non-system shared symtabs
     *
     * @see #getImportedTables()
     */
    SymbolTable[] getImportedTablesNoCopy()
    {
        return myImports;
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
    boolean equalImports(LocalSymbolTableImports other)
    {
        return Arrays.equals(myImports, other.myImports);
    }
}
