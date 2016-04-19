/*
 * Copyright 2007-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion.system;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import org.junit.Test;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonString;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonTestCase;
import software.amazon.ion.IonValue;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.system.SimpleCatalog;

public class SimpleCatalogTest
    extends IonTestCase
{
    @Test
    public void testGetMissingVersion()
    {
        SimpleCatalog cat = myCatalog = new SimpleCatalog();
        assertNull(cat.getTable("T"));
        assertNull(cat.getTable("T", 3));


        String t1Text =
            "$ion_shared_symbol_table::{" +
            "  name:'''T''', version:1," +
            "  symbols:[ '''yes''', '''no''' ]" +
            "}";
        registerSharedSymtab(t1Text);

        SymbolTable t1 = cat.getTable("T", 1);
        assertEquals(1, t1.getVersion());
        checkSymbol("no", 2, t1);
        checkUnknownSymbol("maybe", t1);
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
        checkSymbol("maybe", 3, t2);
        assertSame(t2, cat.getTable("T"));
        assertSame(t1, cat.getTable("T", 1));
        assertSame(t2, cat.getTable("T", 5));

        assertSame(t1, cat.removeTable("T", 1));

        assertSame(t2, cat.getTable("T"));
        assertSame(t2, cat.getTable("T", 1));
        assertSame(t2, cat.getTable("T", 2));
        assertSame(t2, cat.getTable("T", 5));
    }


    @Test
    public void testBestMatch()
    {
        // Only available versions are less-than requested
        checkBestMatch(1, 5, 1);
        checkBestMatch(2, 5, 1, 2);
        checkBestMatch(3, 5, 2, 1, 3);
        checkBestMatch(3, 5, 3, 1, 2);
        checkBestMatch(3, 5, 3, 2, 1);
        checkBestMatch(3, 5, 2, 3, 1);

        // Only available versions are greater-than requested
        checkBestMatch(6, 5, 6);
        checkBestMatch(6, 5, 6, 9);
        checkBestMatch(6, 5, 9, 6);

        // Mix of less-than and greater-than
        checkBestMatch(6, 5, 9, 6, 4);
        checkBestMatch(6, 5, 3, 9, 6);
        checkBestMatch(6, 5, 3, 9, 6, 4);
        checkBestMatch(6, 5, 3, 9, 2, 6, 4);
    }

    private void checkBestMatch(int expected, int requested, Integer... available)
    {
        SimpleCatalog cat = new SimpleCatalog();
        List<Integer> asList = Arrays.asList(available);
        Integer best = SimpleCatalog.bestMatch(requested, asList);
        assertEquals("best match", expected, best.intValue());
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
