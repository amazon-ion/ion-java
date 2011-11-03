// Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.Symtabs.FRED_MAX_IDS;
import static com.amazon.ion.SystemSymbols.ION_1_0;
import static com.amazon.ion.SystemSymbols.ION_1_0_SID;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;
import static com.amazon.ion.SystemSymbols.SYMBOLS;

import com.amazon.ion.impl.IonSystemPrivate;
import com.amazon.ion.impl.IonValueImpl;
import com.amazon.ion.impl.IonValuePrivate;
import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.Iterator;
import org.junit.Before;
import org.junit.Test;


public class DatagramTest
    extends SequenceTestCase
{
    private IonLoader myLoader;


    @Override
    @Before
    public void setUp()
        throws Exception
    {
        super.setUp();
        myLoader = loader();
    }


    @Override
    protected IonSequence makeEmpty()
    {
        return system().newDatagram();
    }

    @Override
    protected IonSequence makeNull()
    {
        return null;
    }

    @Override
    protected IonSequence newSequence(Collection<? extends IonValue> children)
    {
        IonDatagram dg = system().newDatagram();
        dg.addAll(children);
        return dg;
    }

    @Override
    protected <T extends IonValue> IonSequence newSequence(T... elements)
    {
        IonDatagram dg = system().newDatagram();
        for (int i = 0; i < elements.length; i++)
        {
            dg.add(elements[i]);
        }
        return dg;
    }

    @Override
    protected String wrap(String... children)
    {
        StringBuilder buf = new StringBuilder();
        if (children != null)
        {
            for (String child : children)
            {
                buf.append(child);
                buf.append(' ');
            }
        }
        return buf.toString();
    }

    @Override
    protected IonDatagram wrapAndParse(String... children)
    {
        String text = wrap(children);
        return loader().load(text);
    }

    @Override
    protected IonContainer wrap(IonValue... children)
    {
        IonDatagram dg = system().newDatagram();
        for (int i = 0; i < children.length; i++)
        {
            dg.add(children[i]);
        }
        return dg;
    }


    public IonDatagram roundTrip(String text)
        throws Exception
    {
        IonDatagram datagram0 = myLoader.load(text);
        byte[] bytes = datagram0.getBytes();
        checkBinaryHeader(bytes);
        IonDatagram datagram1 = myLoader.load(bytes);
        return datagram1;
    }


    @Test
    public void testAutomaticSystemId()
        throws Exception
    {
        IonSystemPrivate system = system();
        SymbolTable      systemSymtab_1_0 = system.getSystemSymbolTable(ION_1_0);

        IonDatagram dg = system.newDatagram();

        IonNull v = system.newNull();
        assertTrue(v.getSymbolTable() == null || v.getSymbolTable().isSystemTable());

        dg.add(v);

        IonValue sysId = dg.systemGet(0);
        checkSymbol(ION_1_0, ION_1_0_SID, sysId);
        assertSame(systemSymtab_1_0, sysId.getSymbolTable());

        assertSame(systemSymtab_1_0, v.getSymbolTable());
    }

    @Test
    public void testManualSystemId()
        throws Exception
    {
        IonSystemPrivate system = system();
        SymbolTable      systemSymtab_1_0 = system.getSystemSymbolTable(ION_1_0);

        IonDatagram dg = system.newDatagram();

        IonSymbol sysId = system.newSymbol(ION_1_0);
        assertTrue(sysId.getSymbolTable() == null || sysId.getSymbolTable().isSystemTable());

        // $ion_1_0 at the front top-level is a systemId
        dg.add(sysId);

        assertSame(systemSymtab_1_0, sysId.getSymbolTable());

        // TODO adding $ion_1_1 should fail: unsupported version
    }



    //public void checkLeadingSymbolTable(IonDatagram dg)
    //{
    //    assertTrue("Datagram doesn't start with a symbol table",
    //               dg.systemGet(0).hasTypeAnnotation(ION_1_0));
    //}

    @Test
    public void testBinaryData()
        throws Exception
    {
        IonDatagram datagram0 = myLoader.load("swamp");
        assertEquals(1, datagram0.size());
        checkSymbol("swamp", datagram0.get(0));
        assertSame(datagram0, datagram0.get(0).getContainer());

        byte[] bytes = datagram0.getBytes();
        checkBinaryHeader(bytes);

        IonDatagram datagram1 = myLoader.load(bytes);
        assertEquals(1, datagram1.size());
        checkSymbol("swamp", datagram1.get(0));

        IonSymbol sym = (IonSymbol)datagram1.get(0);
        sym.getSymbolId();
        sym.stringValue();

        // System view should have IonVersionMarker(symbol), a symbol table then the symbol
        assertEquals(3, datagram1.systemSize()); // cas 22 apr 2008 was: 2

        Iterator<IonValue> sysIter = datagram1.systemIterator();
        IonSymbol versionMarker = (IonSymbol)sysIter.next();      // the IVM symbol is first
        assertSame(versionMarker, datagram1.systemGet(0));
        IonStruct localSymtabStruct = (IonStruct) sysIter.next(); // then the smbol table
        assertEquals(localSymtabStruct, datagram1.systemGet(1));

        IonSymbol symbol = (IonSymbol) sysIter.next();
        assertSame(symbol, datagram1.systemGet(2)); // cas 22 apr 2008: was 1
        assertSame(symbol, datagram1.get(0));

        SymbolTable symtab = symbol.getSymbolTable();
        assertEquals(symbol.getSymbolId(), symtab.getMaxId());

        // TODO if we keep max_id in the struct, should validate it here.
    }

    @Test
    public void testBinaryDataWithNegInt()
        throws Exception
    {
        IonDatagram datagram0 = myLoader.load("a::{}");
        assertEquals(1, datagram0.size());
        IonStruct struct = (IonStruct)(datagram0.get(0));
        IonInt i =
            (IonInt)system().clone(myLoader.load("-12345 a").get(0));
        struct.put("a", i);
        datagram0.getBytes();
        IonValue a = struct.get("a");
        struct.remove(a);
        int ival = -65;
        String sval = ""+ival;
        i = (IonInt)system().clone(myLoader.load(sval).get(0));
        struct.put("a", i);
        checkInt(ival, struct.get("a"));

        byte[] bytes = datagram0.getBytes();

        IonDatagram datagram1 = myLoader.load(bytes);
        assertEquals(1, datagram1.size());
        checkInt(ival, ((IonStruct)(datagram1.get(0))).get("a"));
        bytes = datagram1.getBytes();
        String s = datagram1.toString();
        s = ""+s;
    }

    @Test
    public void testSystemDatagram()
        throws Exception
    {
        IonSystem system = system();

        IonInt i = system.newNullInt();
        i.setValue(65);

        IonStruct struct = system.newNullStruct();
        SymbolTable sym = struct.getSymbolTable();
        if (sym == null) {
            sym = system.newLocalSymbolTable();
            ((IonValueImpl)struct).setSymbolTable(sym);
        }
        struct.put("ii", i);

        IonDatagram dg = system.newDatagram(struct);

        assertSame(struct, dg.get(0));
        IonStruct reloadedStruct = (IonStruct) dg.get(0);  // XXX
        assertEquals(struct, reloadedStruct);  // XXX
        assertSame(dg, reloadedStruct.getContainer());

        i = (IonInt) reloadedStruct.get("ii");

        for (int ival = -1000; ival <= 1000; ival++) {
            i.setValue(ival);
            byte[] bytes = dg.getBytes();
            checkBinaryHeader(bytes);
        }
    }


    @Test
    public void testGetBytes()
        throws Exception
    {
        IonDatagram dg = myLoader.load("hello '''hi''' 23 [a,b]");
        byte[] bytes1 = dg.getBytes();
        final int size = dg.byteSize();

        assertEquals(size, bytes1.length);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int outLen = dg.getBytes(out);
        assertEquals(size, outLen);

        byte[] bytes2 = out.toByteArray();
        assertEquals(size, bytes2.length);

        assertArrayEquals(bytes1, bytes2);

        // now check extraction into sub-array
        final int OFFSET = 5;
        assertTrue(bytes1.length > OFFSET);
        bytes2 = new byte[size + OFFSET];

        outLen = dg.getBytes(bytes2, OFFSET);
        assertEquals(size, outLen);

        for (int i = 0; i < bytes1.length; i++)
        {
            if (bytes1[i] != bytes2[i + OFFSET])
            {
                fail("Binary data differs at index " + i);
            }
        }

        try {
            dg.getBytes(new byte[3]);
            fail("Expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) { /* good */ }

        try {
            dg.getBytes(new byte[size + OFFSET - 1], OFFSET);
            fail("Expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) { /* good */ }
    }


    @Test
    public void testEncodingAnnotatedSymbol()
    {
        IonSystem system = system();
        IonList v = system.newNullList();

        IonDatagram dg = system.newDatagram(v);
        dg.getBytes();
    }

    @Test
    public void testEncodingSymbolInList()
    {
        IonSystem system = system();
        IonList v = system.newNullList();
        IonSymbol sym = system.newSymbol("sym");
        sym.addTypeAnnotation("ann");
        v.add(sym);

        IonDatagram dg = system.newDatagram(v);
        dg.getBytes();
    }

    @Test
    public void testEncodingSymbolInStruct()
    {
        IonSystem system = system();
        IonStruct struct1 = system.newNullStruct();
        struct1.addTypeAnnotation("ann1");

        IonSymbol sym = system.newSymbol("sym");
        sym.addTypeAnnotation("ann");
        struct1.add("g", sym);

        IonDatagram dg = system.newDatagram(struct1);
        dg.getBytes();
    }

    @Test
    public void testEncodingStructInStruct()
    {
        IonSystem system = system();
        IonStruct struct1 = system.newNullStruct();
        struct1.addTypeAnnotation("ann1");

        IonStruct struct2 = system.newNullStruct();
        struct1.addTypeAnnotation("ann2");

        IonSymbol sym = system.newSymbol("sym");
        sym.addTypeAnnotation("ann");

        struct1.add("f", struct2);
        struct2.add("g", sym);

        IonDatagram dg = system.newDatagram(struct1);
        dg.getBytes();
    }


    @Test
    public void testNewSingletonDatagramWithSymbolTable()
    {
        IonSystem system = system();
        IonNull aNull = system.newNull();
        aNull.addTypeAnnotation("ann");

        IonDatagram dg = system.newDatagram(aNull);


// FIXME: should this be here or not???? checkLeadingSymbolTable(dg);



        IonDatagram dg2 = reload(dg);
        IonNull v = (IonNull) dg2.get(0);
        assertTrue(v.hasTypeAnnotation("ann"));
    }

    @Test
    public void testNoSymbols()
        throws Exception
    {
        IonDatagram datagram = roundTrip("123");
        assertEquals(1, datagram.size());

        IonValue value = datagram.get(0);
        checkInt(123, value);
    }

    @Test
    public void testNullField()
        throws Exception
    {
        IonDatagram datagram = roundTrip("ann::{a:null.symbol}");
        assertEquals(1, datagram.size());
        IonStruct s = (IonStruct) datagram.get(0);
        checkSymbol(null, s.get("a"));

        datagram = roundTrip("ann::{a:null.struct}");
        assertEquals(1, datagram.size());
        s = (IonStruct) datagram.get(0);
        assertTrue(s.get("a").isNullValue());
    }

    @Test
    public void testAddingDatagramToDatagram()
    {
        IonDatagram dg1 = loader().load("one");
        IonDatagram dg2 = loader().load("two");

        // Cannot append a datagram
        try
        {
            dg1.add(dg2);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) { }

        // Cannot insert a datagram  // TODO JIRA ION-84
//        try
//        {
//            dg1.add(1, dg2);
//            fail("Expected IllegalArgumentException");
//        }
//        catch (IllegalArgumentException e) { }
    }


    @Test(expected = IllegalArgumentException.class)
    public void testNewDatagramFromDatagram()
    {
        IonDatagram dg1 = loader().load("one");
        system().newDatagram(dg1);
    }

    @Test
    public void testNewDatagramWithNoValue()
    {
        IonValue v = null;

        IonDatagram dg = system().newDatagram(v);
        assertEquals("datagram size", 0, dg.size());
    }

    @Test
    public void testNewDatagramWithContainedValue()
    {
        IonList list = system().newEmptyList();
        IonNull n = list.add().newNull();
        n.addTypeAnnotation("ann");

        IonDatagram dg = system().newDatagram(n);
        assertNotSame(n, dg.get(0));
        assertEquals(n, dg.get(0));
    }

    @Test
    public void testNewDatagramWithImports()
    {
        final int FRED_ID_OFFSET   = systemMaxId();
        final int LOCAL_ID_OFFSET  = FRED_ID_OFFSET + FRED_MAX_IDS[1];

        SymbolTable fred1   = Symtabs.register("fred",   1, catalog());
        IonSymbol   sym;

        IonDatagram db_raw = system().newDatagram(fred1);
        sym = system().newSymbol("fred_2");
        db_raw.add(sym);
        sym = system().newSymbol("localSym");
        db_raw.add(sym);

        byte[] bytes = db_raw.getBytes();

        IonDatagram dg_frombytes = loader().load(bytes);
        // TODO dg is dirty at this point... why? It's freshly loaded!

        assertEquals(4, dg_frombytes.systemSize());

        IonValue f2sym = dg_frombytes.systemGet(2);
        IonValue local = dg_frombytes.systemGet(3);

        if (f2sym == null
           || f2sym.getType().equals(IonType.SYMBOL) == false
           || ((IonSymbol)f2sym).stringValue().equals("fred_2") == false)
        {
            checkSymbol("fred_2",   FRED_ID_OFFSET + 2,   f2sym);
        }
        checkSymbol("fred_2",   FRED_ID_OFFSET + 2,   f2sym);
        checkSymbol("localSym", LOCAL_ID_OFFSET + 1,  local);

        SymbolTable symtab = f2sym.getSymbolTable();
        assertSame(symtab, local.getSymbolTable());
        SymbolTable[] importedTables = symtab.getImportedTables();
        assertEquals(1, importedTables.length);
        assertSame(fred1, importedTables[0]);
    }

    @Test
    public void testEmptyDatagram()
    {
        IonDatagram dg = loader().load("");
//        testEmptySequence(dg); // TODO JIRA ION-84 implement add(int,v)
        dg.add().newInt(1);
        testClearContainer(dg);
    }


    @Override
    @Test
    public void testRemoveViaIteratorThenDirect()
    {
        // TODO JIRA ION-91 implement remove on datagram iterator
    }


    @Test
    public void testCloningDatagram()
    {
        IonDatagram dg1 = loader().load("one 1 [1.0]");
        IonDatagram dg2 = system().clone(dg1);

        byte[] bytes1 = dg1.getBytes();
        byte[] bytes2 = dg2.getBytes();

        assertArrayEquals(bytes1, bytes2);
    }


    /**
     * Catches a simple case that failed for some time.
     */
    @Test
    public void testToString()
    {
        IonDatagram dg = loader().load("1");
        assertEquals("$ion_1_0 1", dg.toString());

        dg = loader().load("{a:b}");
        String text = dg.toString();
        assertTrue("missing version marker",
                   text.startsWith(ION_1_0 + ' '));
        assertTrue("missing data",
                   text.endsWith(" {a:b}"));

        // Just force symtab analysis and make sure output is still okay
        dg.getBytes(new byte[dg.byteSize()]);
        text = dg.toString();
        assertTrue("missing version marker",
                   text.startsWith(ION_1_0 + ' '));
        assertTrue("missing data",
                   text.endsWith(" {a:b}"));
    }

    @Test
    public void testToStringWithoutSymbols()
    {
        IonDatagram dg = system().newDatagram();
        dg.add().newInt(1);
        assertEquals(ION_1_0 + " 1", dg.toString());
    }

    @Test
    public void testToStringWithSymbols()
    {
        IonDatagram dg = system().newDatagram();
        dg.add().newSymbol("x");
        dg.getBytes();  // Force encoding and symtab construction

        String result = dg.toString();

        // Not all DOM impls will inject the symtab after getBytes()
        if (dg.systemGet(1) instanceof IonStruct)
        {
            assertEquals(ION_1_0 + ' '
                         + ION_SYMBOL_TABLE + "::{" + SYMBOLS + ":[\"x\"]}"
                         + " x",
                         result);
        }
        else
        {
            assertEquals(ION_1_0 + " x", result);

        }
    }


    @Test
    public void testReadOnlyDatagram()
    {
        IonInt one = system().newInt(1);
        IonInt two = system().newInt(2);
        IonDatagram dg = system().newDatagram(one);
        dg.makeReadOnly();
        assertSame(one, dg.get(0));

        try {
            dg.add(two);
            fail("Expected exception for modifying read-only value");
        }
        catch (ReadOnlyValueException e) { }
        assertEquals(1, dg.size());

        dg.byteSize();
    }

    /**
     * Verifies that detachment from binary buffer does deep materialization.
     */
    @Test
    public void testMaterializationOnRemove()
    {
        IonDatagram dg = loader().load("[[1]]");
        IonList l = (IonList) dg.remove(0);
        l = (IonList) l.get(0);
        assertEquals(1, ((IonInt)l.get(0)).intValue());
    }

    @Test
    public void testGetTopLevelValue()
    {
        IonDatagram dg = system().newDatagram();
        IonValue tlv = dg.add().newInt(12);
        assertSame(tlv, tlv.topLevelValue());

        IonList list = dg.add().newEmptyList();
        IonBool bool = list.add().newBool(true);

        assertSame(list, list.topLevelValue());
        assertSame(list, bool.topLevelValue());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetTopLevelValueOfDatagram()
    {
        IonDatagram dg = system().newDatagram();
        dg.topLevelValue();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetAssignedSymbolTable()
    {
        IonDatagram dg = system().newDatagram();
        ((IonValuePrivate)dg).getAssignedSymbolTable();
    }

    @Test
    public void testAddingIVM()
    {
        IonDatagram dg = system().newDatagram();
        IonSymbol ivm = system().newSymbol(SystemSymbols.ION_1_0);
        dg.add(ivm);
        // TODO ION-261
        // assertEquals(0, dg.size());
    }
}
