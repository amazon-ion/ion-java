// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.Symtabs.FRED_MAX_IDS;
import static com.amazon.ion.Symtabs.GINGER_MAX_IDS;
import static com.amazon.ion.SystemSymbolTable.ION_1_0_MAX_ID;
import static com.amazon.ion.TestUtils.FERMATA;

import com.amazon.ion.EmptySymbolException;
import com.amazon.ion.IonBlob;
import com.amazon.ion.IonClob;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonLob;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonString;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonText;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.Symtabs;
import com.amazon.ion.TestUtils;
import java.math.BigDecimal;
import java.util.Arrays;

/**
 *
 */
public abstract class IonWriterTestCase
    extends IonTestCase
{
    protected IonWriter makeWriter()
        throws Exception
    {
        return makeWriter((SymbolTable[])null);
    }

    protected IonDatagram reload()
        throws Exception
    {
        byte[] bytes = outputByteArray();
        IonDatagram dg;
        try {
            // force all the classes to load to we can step
            // into the next time we try this and actually
            // see what's going on
            dg = loader().load(bytes);
        }
        catch (IonException e) {
            // do nothing
        }
        dg = loader().load(bytes);
        return dg;
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


    public void testWritingWithImports()
        throws Exception
    {
        final int FRED_ID_OFFSET   = ION_1_0_MAX_ID;
        final int GINGER_ID_OFFSET = FRED_ID_OFFSET + FRED_MAX_IDS[1];
        final int LOCAL_ID_OFFSET  = GINGER_ID_OFFSET + GINGER_MAX_IDS[1];

        SymbolTable fred1   = Symtabs.register("fred",   1, catalog());
        SymbolTable ginger1 = Symtabs.register("ginger", 1, catalog());

        IonWriter writer = makeWriter(fred1, ginger1);
        writer.writeSymbol("fred_2");
        writer.writeSymbol("g1");
        writer.writeSymbol("localSym");

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

    public void testWritingWithSystemImport()
        throws Exception
    {
        final int FRED_ID_OFFSET   = ION_1_0_MAX_ID;
        final int LOCAL_ID_OFFSET  = FRED_ID_OFFSET + FRED_MAX_IDS[1];

        SymbolTable fred1   = Symtabs.register("fred",   1, catalog());

        IonWriter writer = makeWriter(system().getSystemSymbolTable(), fred1);
        writer.writeSymbol("fred_2");
        writer.writeSymbol("localSym");

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

    /**
     * Trap for JIRA ION-52
     */
    public void testWritingNonAscii()
        throws Exception
    {
        String text = TestUtils.YEN_SIGN + FERMATA;

        IonWriter writer = makeWriter();
        writer.writeString(text);
        writer.writeSymbol(text);

        IonDatagram dg = reload();
        IonText t = (IonString) dg.get(0);
        assertEquals(text, t.stringValue());

        t = (IonSymbol) dg.get(1);
        assertEquals(text, t.stringValue());
    }

    public void testWritingBadSurrogates()
        throws Exception
    {
        String highFermata = FERMATA.substring(0, 1);
        String lowFermata  = FERMATA.substring(1, 2);

        testBadText(highFermata);
        testBadText(lowFermata);
        testBadText(highFermata + "x");
    }

    public void testWritingEmptySymbol()
        throws Exception
    {
        IonWriter writer = makeWriter();
        try
        {
            writer.writeSymbol("");
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
        IonWriter writer = makeWriter();
        try
        {
            writer.writeString(text);
            fail("expected exception");
        }
        catch (IllegalArgumentException e) { }
    }

    public void testBadSymbol(String text)
        throws Exception
    {
        IonWriter writer = makeWriter();
        try
        {
            writer.writeSymbol(text);
            fail("expected exception");
        }
        catch (IllegalArgumentException e) { }
    }

    /**
     * Trap for JIRA ION-53
     * @throws Exception
     */
    public void testWritingClob()
        throws Exception
    {
        byte[] data = new byte[256];
        for (int i = 0; i < data.length; i++)
        {
            data[i] = (byte) i;
        }

        IonWriter writer = makeWriter();
        writer.writeBlob(data);
        writer.writeBlob(data, 10, 90);
        writer.writeClob(data);
        writer.writeClob(data, 20, 30);

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

    public void testWritingDeepNestedList() throws Exception {
        // JIRA ION-60
        IonDatagram dg = loader().load("[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]");
        IonWriter writer = makeWriter();
        writer.writeValue(dg);
    }

    // TODO test failure of getBytes before stepping all the way out

    public void testBadSetFieldName()
        throws Exception
    {
        IonWriter iw = makeWriter();
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

    public void testWriteValueMissingFieldName()
        throws Exception
    {
        IonReader ir = system().newReader("{a:{b:10}}");
        IonWriter iw = makeWriter();
        ir.next();
        iw.stepIn(IonType.STRUCT);

        // Missing call to setFieldName()
        try {
            iw.writeValue(ir);
            fail("expected exception");
        }
        catch (IllegalStateException e) { }
    }

    public void testWriteValueCopiesFieldName()
        throws Exception
    {
        String data = "{a:{b:10}}";
        IonReader ir = system().newReader(data);
        ir.next();
        ir.stepIn();
        ir.next();
        // Reader is now positioned at field a

        IonWriter iw = makeWriter();
        iw.stepIn(IonType.STRUCT);
        iw.writeValue(ir);
        iw.stepOut();

        assertEquals(data, reloadSingleValue().toString());
    }

    public void testWriteValueDifferentFieldName()
        throws Exception
    {
        String data = "{a:{b:10}}";
        IonReader ir = system().newReader(data);
        ir.next();
        ir.stepIn();
        ir.next();
        // Reader is now positioned at field a

        IonWriter iw = makeWriter();
        iw.stepIn(IonType.STRUCT);
        iw.setFieldName("c");
        iw.writeValue(ir);
        iw.stepOut();

        assertEquals("{c:{b:10}}", reloadSingleValue().toString());
    }

    public void testWritingNulls()
        throws Exception
    {
        IonWriter iw = makeWriter();
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

    public void testWritingAnnotations()
        throws Exception
    {
        IonWriter iw = makeWriter();
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

        // TODO ugh, writer and ionvalue behave differently
//        iw.addTypeAnnotation("b");
//        iw.addTypeAnnotation("b");
//        iw.writeNull();
//        v = expected.add().newNull();
//        v.addTypeAnnotation("b");
//        v.addTypeAnnotation("b");

        iw.addTypeAnnotation("b");
        iw.setTypeAnnotations(new String[]{"c", "d"});
        iw.writeNull();
        v = expected.add().newNull();
        v.addTypeAnnotation("c");
        v.addTypeAnnotation("d");

        iw.addTypeAnnotation("b");
        iw.setTypeAnnotations(new String[0]);
        iw.writeNull();
        v = expected.add().newNull();
        v.clearTypeAnnotations();

        iw.addTypeAnnotation("b");
        iw.setTypeAnnotations(null);
        iw.writeNull();
        v = expected.add().newNull();
        v.clearTypeAnnotations();

        assertEquals(expected, reload());
    }

    public void testWritingAnnotationIds()
        throws Exception
    {
        IonWriter iw = makeWriter();
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
}
