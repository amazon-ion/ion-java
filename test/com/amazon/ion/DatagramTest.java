/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import com.amazon.ion.impl.IonSequenceImpl;
import com.amazon.ion.impl.IonValueImpl;
import java.util.Iterator;


/**
 *
 */
public class DatagramTest
    extends IonTestCase
{
    private IonLoader myLoader;


    public void setUp()
        throws Exception
    {
        super.setUp();
        myLoader = system().newLoader();
    }



    public IonDatagram roundTrip(String text)
        throws Exception
    {
        IonDatagram datagram0 = myLoader.loadText(text);
        byte[] bytes = datagram0.toBytes();
        checkBinaryHeader(bytes);
        IonDatagram datagram1 = myLoader.load(bytes);
        return datagram1;
    }

    public void checkLeadingSymbolTable(IonDatagram dg)
    {
        assertTrue("Datagram doesn't start with a symbol table",
                   dg.systemGet(0).hasTypeAnnotation(SymbolTable.ION_1_0));
    }

    public void testBinaryData()
        throws Exception
    {
        IonDatagram datagram0 = myLoader.loadText("swamp");
        assertEquals(1, datagram0.size());
        checkSymbol("swamp", datagram0.get(0));
        assertSame(datagram0, datagram0.get(0).getContainer());

        byte[] bytes = datagram0.toBytes();
        checkBinaryHeader(bytes);

        IonDatagram datagram1 = myLoader.load(bytes);
        assertEquals(1, datagram1.size());
        checkSymbol("swamp", datagram1.get(0));

        // System view should have symbol table then symbol
        assertEquals(2, datagram1.systemSize());

        Iterator<IonValue> sysIter = datagram1.systemIterator();
        IonStruct localSymtabStruct = (IonStruct) sysIter.next();
        assertSame(localSymtabStruct, datagram1.systemGet(0));

        IonSymbol symbol = (IonSymbol) sysIter.next();
        assertSame(symbol, datagram1.systemGet(1));
        assertSame(symbol, datagram1.get(0));

        SymbolTable symtab = symbol.getSymbolTable();
        assertEquals(symbol.intValue(), symtab.getMaxId());

        // TODO if we keep max_id in the struct, should validate it here.
    }

    public void testBinaryDataWithNegInt()
        throws Exception
    {
        IonDatagram datagram0 = myLoader.loadText("a::{}");
        assertEquals(1, datagram0.size());
        IonStruct struct = (IonStruct)(datagram0.get(0));
        IonInt i =
            (IonInt)system().clone(myLoader.loadText("-12345 a").get(0));
        struct.put("a", i);
        datagram0.toBytes();
        IonValue a = struct.get("a");
        struct.remove(a);
        int ival = -65;
        String sval = ""+ival;
        i = (IonInt)system().clone(myLoader.loadText(sval).get(0));
        struct.put("a", i);
        checkInt(ival, struct.get("a"));

        byte[] bytes = datagram0.toBytes();

        IonDatagram datagram1 = myLoader.load(bytes);
        assertEquals(1, datagram1.size());
        checkInt(ival, ((IonStruct)(datagram1.get(0))).get("a"));
        bytes = datagram1.toBytes();
        String s = datagram1.toString();
        s = ""+s;
    }

    public void testSystemDatagram()
    throws Exception
    {
        IonSystem system = system();
        IonInt i = system.newInt();
        i.setValue(65);
        IonStruct struct = system.newStruct();
        LocalSymbolTable sym = struct.getSymbolTable();
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
            byte[] bytes = dg.toBytes();
            checkBinaryHeader(bytes);
        }
    }


    public void testNewSingletonDatagramWithSymbolTable()
    {
        IonSystem system = system();
        IonNull aNull = system.newNull();
        aNull.addTypeAnnotation("ann");

        IonDatagram dg = system.newDatagram(aNull);
        checkLeadingSymbolTable(dg);
        IonDatagram dg2 = reload(dg);
        IonNull v = (IonNull) dg2.get(0);
        assertTrue(v.hasTypeAnnotation("ann"));
    }

    public void testNoSymbols()
        throws Exception
    {
        IonDatagram datagram = roundTrip("123");
        assertEquals(1, datagram.size());

        IonValue value = datagram.get(0);
        checkInt(123, value);
    }

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


    // FIXME implement embedding
    public void XXXtestEmbedding()
    {
        IonDatagram sourceDatagram = myLoader.loadText("bean");
        IonDatagram destDatagram = myLoader.loadText("[java]");

        IonSymbol sourceBean = (IonSymbol) sourceDatagram.get(0);
        assertEquals("bean", sourceBean.stringValue());
        int beanSid = sourceBean.intValue();
        assertTrue(beanSid > 0);

        IonList destList = (IonList) destDatagram.get(0);
        IonSymbol javaSym = (IonSymbol) destList.get(0);
        assertEquals(beanSid, javaSym.intValue());

        // TODO remove cast
        ((IonSequenceImpl)destList).addEmbedded(sourceBean);
        assertIonEquals(sourceBean, destList.get(1));

        IonDatagram reloadedDatagram = myLoader.load(destDatagram.toBytes());
        IonList reloadedList = (IonList) reloadedDatagram.get(0);

        // Both symbols have the same sid!
        IonSymbol reloadedBean = (IonSymbol) reloadedList.get(1);
        checkSymbol("java", beanSid, reloadedList.get(0));
        checkSymbol("bean", beanSid, reloadedBean);

        assertSame(reloadedList, reloadedBean.getContainer());
        // TODO add this API to IonValues
//        assertNotSame(reloadedList, reloadedBean.getSystemContainer());
        // ...
    }
}
