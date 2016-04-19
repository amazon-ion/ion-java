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

package software.amazon.ion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static software.amazon.ion.SystemSymbols.ION_SHARED_SYMBOL_TABLE;
import static software.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;
import static software.amazon.ion.impl.PrivateUtils.newLocalSymtab;
import static software.amazon.ion.util.IonTextUtils.printString;

import java.io.IOException;
import java.util.Arrays;
import software.amazon.ion.IonList;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonSystem;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SystemSymbols;
import software.amazon.ion.ValueFactory;
import software.amazon.ion.impl.PrivateIonSystem;
import software.amazon.ion.system.IonSystemBuilder;
import software.amazon.ion.system.SimpleCatalog;

public class Symtabs
{

    public static final String LocalSymbolTablePrefix = ION_SYMBOL_TABLE + "::";
    public static final String SharedSymbolTablePrefix = ION_SHARED_SYMBOL_TABLE + "::";

    public static final String[] LOCAL_SYMBOLS_ABC = new String[] {"a", "b", "c"};

    public static final SimpleCatalog CATALOG = new SimpleCatalog();

    public static final String FRED_NAME = "fred";

    public static final String[] FRED_SERIALIZED = {
        null,

        // version: 1
        SharedSymbolTablePrefix +
        "{" +
        "  name:'''" + FRED_NAME + "''', version:1," +
        "  symbols:['''fred_1''', '''fred_2''']" +
        "}",

        // version: 2
        SharedSymbolTablePrefix +
        "{" +
        "  name:'''" + FRED_NAME + "''', version:2," +
        "  symbols:[" +
        "    '''fred_1'''," +
        "    '''fred_2'''," +
        "    '''fred_3'''," +
        "    '''fred_4'''," +
        "  ]" +
        "}",

        // version: 3
        SharedSymbolTablePrefix +
        "{" +
        "  name:'''" + FRED_NAME + "''', version:3," +
        "  symbols:[" +
        "    '''fred_1'''," +
        "    null, /* Removed fred_2 */" +
        "    '''fred_3'''," +
        "    '''fred_4'''," +
        "    '''fred_5'''," +
        "  ]" +
        "}"
    };

    public static final String GINGER_NAME = "ginger";

    public static final String[] GINGER_SERIALIZED = {
        null,

        // version: 1
        SharedSymbolTablePrefix +
        "{" +
        "  name:'''" + GINGER_NAME + "''', version:1," +
        "  symbols:['''g1''', '''g2''']" +
        "}",
    };


    public static final int[] FRED_MAX_IDS;
    public static final int[] GINGER_MAX_IDS;


    static
    {
        FRED_MAX_IDS   = registerTables(FRED_SERIALIZED);
        GINGER_MAX_IDS = registerTables(GINGER_SERIALIZED);
    }

    private static int[] registerTables(String[] serializedTables)
    {
        int[] maxIds = new int[serializedTables.length];

        PrivateIonSystem system = (PrivateIonSystem)
            IonSystemBuilder.standard().withCatalog(CATALOG).build();

        for (int i = 1; i < serializedTables.length; i++)
        {
            String serialized = serializedTables[i];

            IonStruct st = (IonStruct) system.singleValue(serialized);
            SymbolTable shared = system.newSharedSymbolTable(st);
            CATALOG.putTable(shared);

            maxIds[i] = shared.getMaxId();
        }

        return maxIds;
    }

    public static SymbolTable register(String name, int version,
                                       SimpleCatalog catalog)
    {
        SymbolTable table = CATALOG.getTable(name, version);
        catalog.putTable(table);
        return table;
    }

    public static void assertEqualSymtabs(SymbolTable s1, SymbolTable s2)
    {
        assertEquals(s1.isLocalTable(), s2.isLocalTable());
        assertEquals(s1.isSharedTable(), s2.isSharedTable());
        assertEquals(s1.isSystemTable(), s2.isSystemTable());
        assertEquals(s1.getName(),    s2.getName());
        assertEquals(s1.getVersion(), s2.getVersion());
        assertEquals(s1.getMaxId(),   s2.getMaxId());

        assertSame(s1.getSystemSymbolTable(), s2.getSystemSymbolTable());
        assertEquals(s1.getIonVersionId(), s2.getIonVersionId());
        assertTrue(Arrays.equals(s1.getImportedTables(),
                                 s2.getImportedTables()));
        assertEquals(s1.getImportedMaxId(), s2.getImportedMaxId());
        assertEquals(s1.getMaxId(), s2.getMaxId());
    }

    /**
     * Constructs the DOM for a shared symtab.
     *
     * @param syms If null, so symbols list will be added.
     */
    public static IonStruct sharedSymtabStruct(ValueFactory factory,
                                               String name, int version,
                                               String... syms)
    {
        IonStruct s = factory.newEmptyStruct();
        s.setTypeAnnotations(SystemSymbols.ION_SHARED_SYMBOL_TABLE);
        s.add(SystemSymbols.NAME).newString(name);
        s.add(SystemSymbols.VERSION).newInt(version);
        if (syms != null)
        {
            IonList l = s.add(SystemSymbols.SYMBOLS).newEmptyList();
            for (String sym : syms)
            {
                l.add().newString(sym);
            }
        }
        return s;
    }


    public static String printLocalSymtab(String... symbols)
        throws IOException
    {
        StringBuilder s = new StringBuilder(LocalSymbolTablePrefix);

        s.append("{symbols:[");

        for (String symbol : symbols)
        {
            // If symbol is the null ref., print it as null to indicate a gap
            if (symbol == null)
            {
                s.append("null");
            }
            else
            {
                printString(s, symbol);
            }
            s.append(',');
        }

        s.append("]} ");

        return s.toString();
    }


    /**
     * Creates a local symtab with local symbols but no imports.
     *
     * @param system
     * @param localSymbols
     *          the array (var-args) of local symbols that the resulting
     *          local symtab to contain; may be {@code null} to indicate no
     *          local symbols; elements may be {@code null} to indicate a gap
     */
    public static SymbolTable makeLocalSymtab(IonSystem system,
                                              String... localSymbols)
    {
        return newLocalSymtab(system, system.getSystemSymbolTable(),
                              Arrays.asList(localSymbols));
    }


    /**
     * Creates a local symtab with local symbols and imports.
     */
    public static SymbolTable makeLocalSymtab(IonSystem system,
                                              String[] localSymbols,
                                              SymbolTable... imports)
    {
        SymbolTable localSymtab = system.newLocalSymbolTable(imports);

        for (String localSymbol : localSymbols)
        {
            localSymtab.intern(localSymbol);
        }

        return localSymtab;
    }
}
