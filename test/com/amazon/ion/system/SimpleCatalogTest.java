/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.system;

import com.amazon.ion.IonTestCase;
import com.amazon.ion.StaticSymbolTable;

/**
 *
 */
public class SimpleCatalogTest
    extends IonTestCase
{
    public void testGetMissingVersion()
    {
        SimpleCatalog cat = new SimpleCatalog();
        assertNull(cat.getTable("T"));
        assertNull(cat.getTable("T", 3));

        system().setCatalog(cat);


        String t1Text =
            "$ion_symbol_table::{" +
            "  name:'''T''', version:1," +
            "  symbols:{" +
            "    $1:'''yes'''," +
            "    $2:'''no'''," +
            "  }" +
            "}";
        loader().load(t1Text);

        StaticSymbolTable t1 = cat.getTable("T", 1);
        assertEquals("no", t1.findKnownSymbol(2));
        assertEquals(-1,   t1.findSymbol("maybe"));
        assertSame(t1, cat.getTable("T"));
        assertSame(t1, cat.getTable("T", 5));


        String t2Text =
            "$ion_symbol_table::{" +
            "  name:'''T''', version:2," +
            "  symbols:{" +
            "    $1:'''yes'''," +
            "    $2:'''no'''," +
            "    $3:'''maybe'''," +
            "  }" +
            "}";
        loader().load(t2Text);

        StaticSymbolTable t2 = cat.getTable("T", 2);
        assertEquals(3, t2.findSymbol("maybe"));
        assertSame(t2, cat.getTable("T"));
        assertSame(t1, cat.getTable("T", 1));
        assertSame(t2, cat.getTable("T", 5));

        assertSame(t1, cat.removeTable("T", 1));

        assertSame(t2, cat.getTable("T"));
        assertSame(t2, cat.getTable("T", 1));
        assertSame(t2, cat.getTable("T", 2));
        assertSame(t2, cat.getTable("T", 5));
    }
}
