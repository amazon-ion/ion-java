/*
 * Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import static com.amazon.ion.SystemSymbolTable.ION_SYMBOL_TABLE;
import static com.amazon.ion.impl.UnifiedSymbolTable.ION_SHARED_SYMBOL_TABLE;

import com.amazon.ion.impl.IonSystemImpl;
import com.amazon.ion.system.SimpleCatalog;
import com.amazon.ion.system.SystemFactory;

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


    public final static int[] FRED_MAX_IDS;


    static
    {
        FRED_MAX_IDS = new int[FRED_SERIALIZED.length];

        IonSystemImpl system = (IonSystemImpl) SystemFactory.newSystem(CATALOG);

        for (int i = 1; i < FRED_SERIALIZED.length; i++)
        {
            String serialized = FRED_SERIALIZED[i];

            IonStruct st = (IonStruct) system.singleValue(serialized);
            SymbolTable shared = system.newSharedSymbolTable(st);
            CATALOG.putTable(shared);

            FRED_MAX_IDS[i] = shared.getMaxId();
        }
    }


    public static SymbolTable register(String name, int version,
                                       SimpleCatalog catalog)
    {
        SymbolTable table = CATALOG.getTable(name, version);
        catalog.putTable(table);
        return table;
    }
}
