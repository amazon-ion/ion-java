// Copyright (c) 2009-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.SystemSymbols.ION_SHARED_SYMBOL_TABLE;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.amazon.ion.impl.IonSystemPrivate;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.SimpleCatalog;
import java.util.Arrays;

/**
 *
 */
public class Symtabs
{

    public static final String LocalSymbolTablePrefix = ION_SYMBOL_TABLE + "::";
    public static final String SharedSymbolTablePrefix = ION_SHARED_SYMBOL_TABLE + "::";



    public static final SimpleCatalog CATALOG = new SimpleCatalog();


    public final static String[] FRED_SERIALIZED = {
        null,

        // version: 1
        SharedSymbolTablePrefix +
        "{" +
        "  name:'''fred''', version:1," +
        "  symbols:['''fred_1''', '''fred_2''']" +
        "}",

        // version: 2
        SharedSymbolTablePrefix +
        "{" +
        "  name:'''fred''', version:2," +
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
        "  name:'''fred''', version:3," +
        "  symbols:[" +
        "    '''fred_1'''," +
        "    null, /* Removed fred_2 */" +
        "    '''fred_3'''," +
        "    '''fred_4'''," +
        "    '''fred_5'''," +
        "  ]" +
        "}"
    };

    public final static String[] GINGER_SERIALIZED = {
        null,

        // version: 1
        SharedSymbolTablePrefix +
        "{" +
        "  name:'''ginger''', version:1," +
        "  symbols:['''g1''', '''g2''']" +
        "}",
    };


    public final static int[] FRED_MAX_IDS;
    public final static int[] GINGER_MAX_IDS;


    static
    {
        FRED_MAX_IDS   = registerTables(FRED_SERIALIZED);
        GINGER_MAX_IDS = registerTables(GINGER_SERIALIZED);
    }

    private static int[] registerTables(String[] serializedTables)
    {
        int[] maxIds = new int[serializedTables.length];

        IonSystemPrivate system = (IonSystemPrivate)
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
}
