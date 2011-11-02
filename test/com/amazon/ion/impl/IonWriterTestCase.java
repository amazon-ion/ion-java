// Copyright (c) 2009-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.Symtabs.FRED_MAX_IDS;
import static com.amazon.ion.Symtabs.GINGER_MAX_IDS;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;
import static com.amazon.ion.TestUtils.FERMATA;

import com.amazon.ion.EmptySymbolException;
import com.amazon.ion.IonBlob;
import com.amazon.ion.IonClob;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonList;
import com.amazon.ion.IonLob;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonText;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.Symtabs;
import com.amazon.ion.SystemSymbols;
import com.amazon.ion.TestUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Iterator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public abstract class IonWriterTestCase
    extends IonTestCase
{
    protected IonWriter iw;

    /**
     * Closes {@link #iw} if it's not null.
     */
    @After
    public void closeWriter()
    throws IOException
    {
        if (iw != null)
        {
            iw.close();
            iw = null;
        }
    }

    //=========================================================================

    protected IonWriter makeWriter()
        throws Exception
    {
        return makeWriter((SymbolTable[])null);
    }

    /**
     * Extracts bytes from the current writer and loads it into a datagram.
     */
    @SuppressWarnings("unused")
    protected IonDatagram reload()
        throws Exception
    {
        byte[] bytes = outputByteArray();
        IonDatagram dg;

        if (false) // Edit for debugging
        {
            try {
                // force all the classes to load to we can step
                // into the next time we try this and actually
                // see what's going on
                dg = loader().load(bytes);
            }
            catch (IonException e) {
                // do nothing
            }
        }

        dg = loader().load(bytes);
        return dg;
    }

    /**
     * Extracts bytes from the current writer and loads it into a datagram.
     */
    protected IonReader reread()
        throws Exception
    {
        byte[] bytes = outputByteArray();
        return system().newReader(bytes);
    }

    protected IonValue reloadSingleValue()
        throws Exception
    {
        byte[] bytes = outputByteArray();
        return system().singleValue(bytes);
    }


    protected abstract IonWriter makeWriter(SymbolTable... imports)
        throws Exception;

    protected abstract byte[] outputByteArray()
        throws Exception;

    /**
     * Validate that the output stream has been closed.
     */
    protected abstract void checkClosed();


    //=========================================================================

    @Test
    public void testWritingWithImports()
        throws Exception
    {
        final int FRED_ID_OFFSET   = systemMaxId();
        final int GINGER_ID_OFFSET = FRED_ID_OFFSET + FRED_MAX_IDS[1];
        final int LOCAL_ID_OFFSET  = GINGER_ID_OFFSET + GINGER_MAX_IDS[1];

        SymbolTable fred1   = Symtabs.register("fred",   1, catalog());
        SymbolTable ginger1 = Symtabs.register("ginger", 1, catalog());

        iw = makeWriter(fred1, ginger1);
        iw.writeSymbol("fred_2");
        iw.writeSymbol("g1");
        iw.writeSymbol("localSym");

        byte[] bytes = outputByteArray();
        IonDatagram dg = loader().load(bytes);

        assertEquals(5, dg.systemSize());

        IonValue f2sym = dg.systemGet(2);
        IonValue g1sym = dg.systemGet(3);
        IonValue local = dg.systemGet(4);

        checkSymbol("fred_2",   FRED_ID_OFFSET + 2,   f2sym);
        checkSymbol("g1",       GINGER_ID_OFFSET + 1, g1sym);
        checkSymbol("localSym", LOCAL_ID_OFFSET + 1,  local);

        SymbolTable symtab = f2sym.getSymbolTable();
        assertSame(symtab, g1sym.getSymbolTable());
        SymbolTable[] importedTables = symtab.getImportedTables();
        assertEquals(2, importedTables.length);
        assertSame(fred1, importedTables[0]);
        assertSame(ginger1, importedTables[1]);
    }

    @Test
    public void testWritingWithSystemImport()
        throws Exception
    {
        final int FRED_ID_OFFSET   = systemMaxId();
        final int LOCAL_ID_OFFSET  = FRED_ID_OFFSET + FRED_MAX_IDS[1];

        SymbolTable fred1   = Symtabs.register("fred",   1, catalog());

        iw = makeWriter(system().getSystemSymbolTable(), fred1);
        iw.writeSymbol("fred_2");
        iw.writeSymbol("localSym");

        byte[] bytes = outputByteArray();
        IonDatagram dg = loader().load(bytes);

        assertEquals(4, dg.systemSize());

        IonValue f2sym = dg.systemGet(2);
        IonValue local = dg.systemGet(3);

        checkSymbol("fred_2",   FRED_ID_OFFSET + 2,   f2sym);
        checkSymbol("localSym", LOCAL_ID_OFFSET + 1,  local);

        SymbolTable symtab = f2sym.getSymbolTable();
        assertSame(symtab, local.getSymbolTable());
        SymbolTable[] importedTables = symtab.getImportedTables();
        assertEquals(1, importedTables.length);
        assertSame(fred1, importedTables[0]);
    }

    // TODO test stepOut() when at top-level


    @Test
    public void testWriteInt()
    throws Exception
    {
        BigInteger bigPos = new BigInteger(Long.MAX_VALUE + "0");
        BigInteger bigNeg = new BigInteger(Long.MIN_VALUE + "0");

        iw = makeWriter();

        iw.writeNull(IonType.INT);
        iw.writeInt(Long.MAX_VALUE);
        iw.writeInt(Long.MIN_VALUE);
        iw.writeInt(null);
        iw.writeInt(bigPos);
        iw.writeInt(bigNeg);

        IonReader r = reread();
        assertEquals(IonType.INT, r.next());
        assertTrue(r.isNullValue());
        assertEquals(IonType.INT, r.next());
        assertEquals(Long.MAX_VALUE, r.longValue());
        assertEquals(IonType.INT, r.next());
        assertEquals(Long.MIN_VALUE, r.longValue());

        assertEquals(IonType.INT, r.next());
        assertTrue(r.isNullValue());
        assertEquals(IonType.INT, r.next());
        assertEquals(bigPos, r.bigIntegerValue());
        assertEquals(IonType.INT, r.next());
        assertEquals(bigNeg, r.bigIntegerValue());

        assertNull(r.next());
    }


    /**
     * Trap for JIRA ION-52
     */
    @Test
    public void testWritingNonAscii()
        throws Exception
    {
        String text = TestUtils.YEN_SIGN + FERMATA;

        iw = makeWriter();
        iw.writeString(text);
        iw.writeSymbol(text);

        IonDatagram dg = reload();
        IonText t = (IonString) dg.get(0);
        assertEquals(text, t.stringValue());

        t = (IonSymbol) dg.get(1);
        assertEquals(text, t.stringValue());
    }

    @Test
    public void testWritingBadSurrogates()
        throws Exception
    {
        String highFermata = FERMATA.substring(0, 1);
        String lowFermata  = FERMATA.substring(1, 2);

        testBadText(highFermata);
        testBadText(lowFermata);
        testBadText(highFermata + "x");
    }

    @Test
    public void testWritingEmptySymbol()
        throws Exception
    {
        iw = makeWriter();
        try
        {
            iw.writeSymbol("");
            fail("expected exception");
        }
        catch (EmptySymbolException e) { }
    }

    public void testBadText(String text)
        throws Exception
    {
        testBadString(text);
        testBadSymbol(text);
    }

    public void testBadString(String text)
        throws Exception
    {
        iw = makeWriter();
        try
        {
            iw.writeString(text);
            fail("expected exception");
        }
        catch (IllegalArgumentException e) { }
    }

    public void testBadSymbol(String text)
        throws Exception
    {
        iw = makeWriter();
        try
        {
            iw.writeSymbol(text);
            fail("expected exception");
        }
        catch (IllegalArgumentException e) { }
    }

    /**
     * Trap for JIRA ION-53
     * @throws Exception
     */
    @Test
    public void testWritingClob()
        throws Exception
    {
        byte[] data = new byte[256];
        for (int i = 0; i < data.length; i++)
        {
            data[i] = (byte) i;
        }

        iw = makeWriter();
        iw.writeBlob(data);
        iw.writeBlob(data, 10, 90);
        iw.writeClob(data);
        iw.writeClob(data, 20, 30);

        byte[] bytes = outputByteArray();
        IonDatagram dg = loader().load(bytes);
        assertEquals(4, dg.size());

        IonLob lob = (IonBlob) dg.get(0);
        assertTrue(Arrays.equals(data, lob.getBytes()));

        lob = (IonBlob) dg.get(1);
        assertEqualBytes(data, 10, 90, lob.getBytes());

        lob = (IonClob) dg.get(2);
        assertTrue(Arrays.equals(data, lob.getBytes()));

        lob = (IonClob) dg.get(3);
        assertEqualBytes(data, 20, 30, lob.getBytes());
    }


    @Test
    public void testWriteLobNull()
        throws Exception
    {
        iw = makeWriter();
        iw.writeBlob(null);
        iw.writeBlob(null, 10, 12);
        iw.writeClob(null);
        iw.writeClob(null, 23, 1);

        IonDatagram dg = reload();
        for (int i = 0; i < 4; i++)
        {
            IonLob lob = (IonLob) dg.get(i);
            assertTrue("dg[" + i +"] not null", lob.isNullValue());
        }
    }


    @Test @SuppressWarnings("deprecation")
    public void testWritingDeepNestedList() throws Exception {
        // JIRA ION-60
        IonDatagram dg = loader().load("[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]");
        iw = makeWriter();
        dg.writeTo(iw);
        iw.writeValue(dg);
    }

    // TODO test failure of getBytes before stepping all the way out

    @Test
    public void testBadSetFieldName()
        throws Exception
    {
        iw = makeWriter();
        iw.stepIn(IonType.STRUCT);

        try {
            iw.setFieldName(null);
            fail("expected exception");
        }
        catch (NullPointerException e) { }

        try {
            iw.setFieldName("");
            fail("expected exception");
        }
        catch (EmptySymbolException e) { }
    }

    @Test
    public void testWriteValueMissingFieldName()
        throws Exception
    {
        IonReader ir = system().newReader("{a:{b:10}}");
        iw = makeWriter();
        ir.next();
        iw.stepIn(IonType.STRUCT);

        // Missing call to setFieldName()
        try {
            iw.writeValue(ir);
            fail("expected exception");
        }
        catch (IllegalStateException e) { }
    }

    @Test
    public void testWriteValueCopiesFieldName()
        throws Exception
    {
        IonStruct data = struct("{a:{b:10}}");
        IonReader ir = system().newReader(data);
        ir.next();
        ir.stepIn();
        ir.next();
        assertEquals("a", ir.getFieldName());

        iw = makeWriter();
        iw.stepIn(IonType.STRUCT);
        iw.writeValue(ir);
        iw.stepOut();
        assertEquals(data, reloadSingleValue());

        IonValue a = data.get("a");
        ir = system().newReader(a);
        ir.next();

        iw = makeWriter();
        iw.stepIn(IonType.STRUCT);
        iw.writeValue(ir);
        iw.stepOut();
        assertEquals(data, reloadSingleValue());
    }

    @Test
    public void testWriteValueDifferentFieldName()
        throws Exception
    {
        String data = "{a:{b:10}}";
        IonReader ir = system().newReader(data);
        ir.next();
        ir.stepIn();
        ir.next();
        assertEquals("a", ir.getFieldName());

        iw = makeWriter();
        iw.stepIn(IonType.STRUCT);
        iw.setFieldName("c");
        iw.writeValue(ir);
        iw.stepOut();

        assertEquals("{c:{b:10}}", reloadSingleValue().toString());
    }

    @Test
    public void testWriteValuesCopiesCurrentValue()
        throws Exception
    {
        String data = "1 2 3";
        IonReader ir = system().newReader(data);
        ir.next();

        iw = makeWriter();
        iw.writeValues(ir);

        IonDatagram dg = reload();
        assertEquals(loader().load(data), dg);
    }

    @Test
    public void testWritingNulls()
        throws Exception
    {
        iw = makeWriter();
        IonDatagram expected = system().newDatagram();

        iw.writeBlob(null);
        expected.add().newNullBlob();

        iw.writeClob(null);
        expected.add().newNullClob();

        iw.writeDecimal((BigDecimal)null);
        expected.add().newNullDecimal();

        iw.writeString(null);
        expected.add().newNullString();

        iw.writeSymbol(null);
        expected.add().newNullSymbol();

        iw.writeTimestamp(null);
        expected.add().newNullTimestamp();

        assertEquals(expected, reload());
    }

    @Test
    public void testWritingAnnotations()
        throws Exception
    {
        iw = makeWriter();
        IonDatagram expected = system().newDatagram();

        iw.addTypeAnnotation("a");
        iw.writeNull();
        IonValue v = expected.add().newNull();
        v.addTypeAnnotation("a");

        iw.addTypeAnnotation("b");
        iw.addTypeAnnotation("c");
        iw.writeNull();
        v = expected.add().newNull();
        v.addTypeAnnotation("b");
        v.addTypeAnnotation("c");

        iw.addTypeAnnotation("b");
        iw.addTypeAnnotation("b");
        iw.writeNull();
        v = expected.add().newNull();
        v.setTypeAnnotations("b", "b");

        iw.setTypeAnnotations("b", "b");
        iw.writeNull();
        v = expected.add().newNull();
        v.setTypeAnnotations("b", "b");

        iw.addTypeAnnotation("b");
        iw.setTypeAnnotations("c", "d");
        iw.writeNull();
        v = expected.add().newNull();
        v.setTypeAnnotations("c", "d");

        iw.addTypeAnnotation("b");
        iw.setTypeAnnotations(new String[0]);
        iw.writeNull();
        v = expected.add().newNull();
        v.clearTypeAnnotations();

        iw.addTypeAnnotation("b");
        iw.setTypeAnnotations((String[])null);
        iw.writeNull();
        v = expected.add().newNull();
        v.clearTypeAnnotations();

        iw.addTypeAnnotation("b");
        iw.setTypeAnnotations();
        iw.writeNull();
        v = expected.add().newNull();
        v.clearTypeAnnotations();

        assertEquals(expected, reload());
    }

    @Test
    public void testWritingAnnotationIds()
        throws Exception
    {
        iw = makeWriter();
        IonDatagram expected = system().newDatagram();

        iw.addTypeAnnotation("b");
        iw.setTypeAnnotationIds(new int[0]);
        iw.writeNull();
        IonValue v = expected.add().newNull();
        v.clearTypeAnnotations();

        iw.addTypeAnnotation("b");
        iw.setTypeAnnotationIds(null);
        iw.writeNull();
        v = expected.add().newNull();
        v.clearTypeAnnotations();

        assertEquals(expected, reload());
    }

    @Test
    public void testFlushMidValue()
        throws Exception
    {
        iw = makeWriter();
        iw.addTypeAnnotation("a");
        iw.flush();
        iw.stepIn(IonType.STRUCT);
        iw.flush();
        iw.setFieldName("f");
        iw.flush();
        iw.addTypeAnnotation("a");
        iw.flush();
        iw.writeNull();
        iw.flush();
        iw.stepOut();
        iw.flush();

        IonStruct expected = struct("a::{f:a::null}");
        assertEquals(expected, reload().get(0));
    }

    @Test
    public void testFlushDoesNotReset()
    throws Exception
    {
        SymbolTable fred1 = Symtabs.register("fred",   1, catalog());
        iw = makeWriter(fred1);
        iw.writeSymbol("hey");
        iw.flush();
        iw.writeSymbol("now");
        iw.close();

        // Should have:  IVM SYMTAB hey now
        IonDatagram dg = reload();
        assertEquals(4, dg.systemSize());
    }

    @Test
    public void testFinishDoesReset()
    throws Exception
    {
        SymbolTable fred1 = Symtabs.register("fred",   1, catalog());
        iw = makeWriter(fred1);
        iw.writeSymbol("hey");
        iw.finish();
        iw.writeNull();
        iw.close();

        // Should have: IMV SYMTAB hey IMV null

        // TODO ION-165 Hack to work around the lazy DOM munging system values
        IonSystemPrivate lazySystem = new IonSystemImpl(catalog(), false);

        byte[] data = outputByteArray();
        Iterator<IonValue> it =
            lazySystem.systemIterate(new ByteArrayInputStream(data));
        IonSymbol ivm = (IonSymbol) it.next();
        checkSymbol(SystemSymbols.ION_1_0, ivm);

        IonStruct symtab = (IonStruct) it.next();
        checkAnnotation(SystemSymbols.ION_SYMBOL_TABLE, symtab);
        checkSymbol("hey", it.next());
        checkSymbol(SystemSymbols.ION_1_0, it.next());
        checkNullNull(it.next());
        assertFalse("expected EOF", it.hasNext());
    }

    @Test(expected = IllegalStateException.class)
    public void testFinishInContainer()
    throws Exception
    {
        iw = makeWriter();
        iw.stepIn(IonType.LIST);
        iw.finish();
    }

    @Test
    public void testCloseInContainer()
    throws Exception
    {
        iw = makeWriter();
        iw.stepIn(IonType.LIST);
        iw.close();
        checkClosed();
    }

    @Test
    public void testCloseAfterAnnotation()
    throws Exception
    {
        iw = makeWriter();
        iw.addTypeAnnotation("ann");
        iw.close();
        checkClosed();
    }

    @Test
    public void testCloseAfterFieldName()
    throws Exception
    {
        iw = makeWriter();
        iw.stepIn(IonType.STRUCT);
        iw.setFieldName("f");
        iw.close();
        checkClosed();
    }

    @Test
    public void testCloseAfterFieldAnnotation()
    throws Exception
    {
        iw = makeWriter();
        iw.stepIn(IonType.STRUCT);
        iw.setFieldName("f");
        iw.addTypeAnnotation("ann");
        iw.close();
        checkClosed();

        // Per ION-181, close() doesn't stepOut()
    }

    @Test @Ignore // TODO ION-236
    public void testWritingSymtabWithExtraAnnotations()
    throws Exception
    {
        String[] annotations =
            new String[]{ ION_SYMBOL_TABLE,  ION_SYMBOL_TABLE};
        iw = makeWriter();
        iw.setTypeAnnotations(annotations);
        iw.stepIn(IonType.STRUCT);
        iw.stepOut();
        iw.writeSymbol("foo");
        iw.close();

        IonDatagram dg = reload();
        IonStruct v = (IonStruct) dg.systemGet(1);
        Assert.assertArrayEquals(annotations, v.getTypeAnnotations());
    }

    /**
     * Discovered this old behavior during test builds, some user code relies
     * on it.
     */
    @Test @SuppressWarnings("deprecation")
    public void testWriteValueNull()
        throws Exception
    {
        iw = makeWriter();
        iw.writeValue((IonValue)null);

        IonDatagram dg = reload();
        assertEquals(0, dg.size());
    }

    @Test
    public void testWriteSymbolWithInt()
        throws Exception
    {
        iw = makeWriter();
        iw.writeSymbol(SystemSymbols.NAME_SID);

        IonDatagram dg = reload();
        IonSymbol s = (IonSymbol) dg.get(0);
        assertEquals(SystemSymbols.NAME, s.stringValue());
    }

    @Test
    public void testWriteValuesWithSymtab()
        throws Exception
    {
        SymbolTable fredSymtab =
            Symtabs.register(Symtabs.FRED_NAME, 1, catalog());
        SymbolTable gingerSymtab =
            Symtabs.register(Symtabs.GINGER_NAME, 1, catalog());
        String gingerSym = gingerSymtab.findSymbol(1);

        // First setup some data to be copied.
        IonDatagram dg = system().newDatagram(gingerSymtab);
        dg.add().newSymbol(gingerSym);
        IonReader r = system().newReader(dg.getBytes());

        // Now copy that data into a non-top-level context
        iw = makeWriter(fredSymtab);
        iw.stepIn(IonType.LIST);
        iw.writeValues(r);
        iw.stepOut();

        IonDatagram result = reload();
        IonList l = (IonList) result.get(0);
        assertEquals(1, l.size());
        IonSymbol s = (IonSymbol) l.get(0);
        assertEquals(gingerSym, s.stringValue());
        // Should've assigned a new SID
        assertEquals(systemMaxId() + Symtabs.FRED_MAX_IDS[1] + 1,
                     s.getSymbolId());
    }
}
