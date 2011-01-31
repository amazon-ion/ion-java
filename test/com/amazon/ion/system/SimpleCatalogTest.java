// Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.system;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import org.junit.Test;

/**
 *
 */
public class SimpleCatalogTest
    extends IonTestCase
{
    @Test
    public void testGetMissingVersion()
    {
        SimpleCatalog cat = new SimpleCatalog();
        assertNull(cat.getTable("T"));
        assertNull(cat.getTable("T", 3));

        system().setCatalog(cat);


        String t1Text =
            "$ion_shared_symbol_table::{" +
            "  name:'''T''', version:1," +
            "  symbols:[ '''yes''', '''no''' ]" +
            "}";
        registerSharedSymtab(t1Text);

        SymbolTable t1 = cat.getTable("T", 1);
        assertEquals(1, t1.getVersion());
        assertEquals("no", t1.findKnownSymbol(2));
        assertEquals(-1,   t1.findSymbol("maybe"));
        assertSame(t1, cat.getTable("T"));
        assertSame(t1, cat.getTable("T", 5));


        String t2Text =
            "$ion_shared_symbol_table::{" +
            "  name:'''T''', version:2," +
            "  symbols:[ '''yes''', '''no''', '''maybe''' ]" +
            "}";
        registerSharedSymtab(t2Text);

        SymbolTable t2 = cat.getTable("T", 2);
        assertEquals(2, t2.getVersion());
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


    @SuppressWarnings("unchecked")
    @Test
    public void testBenchmark() {
        Map m = new HashMap();
        String s = "hello";

        m.put("This is a test String.", true);
        m.put(s, true);
        m.put("true", true);
        m.put("false", false);
        m.put("Something", null);
        m.put("12242.124598129", 12242.124598129);
        m.put("long",(long) 9326);
        m.put("12", 12);
        m.put("Almost Done.", true);
        m.put("Date",new Date(-10000));

        HashMap<String, String> hmap = new HashMap();
        for (int i = 0; i < 10; i++) {
            hmap.put("Key " + i, "value " + i);
        }
        TreeMap<String, String> tmap = new TreeMap();
        for (int i = 0; i < 10; i++) {
            tmap.put("Key " + i, "value " + i);
        }
        m.put("hmap", hmap);
        m.put("tmap", tmap);

        IonSystem sys = system();
        IonStruct i_tmap, i_hmap, i_map;
        Set<Entry<String, String>> set;

        i_tmap = sys.newEmptyStruct();
        set = tmap.entrySet();
        for (Entry<String, String> e : set) {
            IonString is = sys.newString(e.getValue());
            i_tmap.add(e.getKey(), is);
        }

        i_hmap = sys.newEmptyStruct();
        set = hmap.entrySet();
        for (Entry<String, String> e : set) {
            IonString is = sys.newString(e.getValue());
            i_hmap.add(e.getKey(), is);
        }

        i_map = sys.newEmptyStruct();
        set = tmap.entrySet();
        for (Entry<String, String> e : set) {
            Object val = e.getValue();
            IonValue ival;
            if (val instanceof String) {
                 ival = sys.newString((String)val);
            }
            else if (e.getKey().equals("tmap")) {
                ival = i_tmap;
            }
            else if (e.getKey().equals("hmap")) {
                ival = i_hmap;
            }
            else {
                throw new RuntimeException("ACK! there's something in this map I don't understand!");
            }

            i_map.add(e.getKey(), ival);
        }
        IonDatagram dg = sys.newDatagram(i_map);

        byte[] bytes = dg.getBytes();
        IonValue v2 = sys.singleValue(bytes);
        assertNotNull(v2);
    }
}
