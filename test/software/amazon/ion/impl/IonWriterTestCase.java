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

package software.amazon.ion.impl;

import static software.amazon.ion.Symtabs.FRED_MAX_IDS;
import static software.amazon.ion.SystemSymbols.ION_SHARED_SYMBOL_TABLE;
import static software.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;
import static software.amazon.ion.SystemSymbols.NAME_SID;
import static software.amazon.ion.TestUtils.FERMATA;
import static software.amazon.ion.impl.PrivateIonWriterBase.ERROR_MISSING_FIELD_NAME;
import static software.amazon.ion.impl.PrivateUtils.newSymbolToken;
import static software.amazon.ion.junit.IonAssert.assertIonEquals;
import static software.amazon.ion.junit.IonAssert.expectNextField;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Iterator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import software.amazon.ion.EmptySymbolException;
import software.amazon.ion.FakeSymbolToken;
import software.amazon.ion.IonBlob;
import software.amazon.ion.IonClob;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonException;
import software.amazon.ion.IonList;
import software.amazon.ion.IonLob;
import software.amazon.ion.IonNull;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonString;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonSymbol;
import software.amazon.ion.IonTestCase;
import software.amazon.ion.IonText;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.IonWriter;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.Symtabs;
import software.amazon.ion.SystemSymbols;
import software.amazon.ion.TestUtils;
import software.amazon.ion.impl.PrivateIonWriter;
import software.amazon.ion.junit.IonAssert;

@SuppressWarnings("deprecation")
public abstract class IonWriterTestCase
    extends IonTestCase
{
    enum OutputForm { TEXT, BINARY, DOM }

    OutputForm myOutputForm;

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

    /** Validate that the output stream has been closed. */
    protected abstract void checkClosed();

    /** Validate whether the output stream has been flushed. */
    protected abstract void checkFlushed(boolean expectFlushed);

    //=========================================================================

    @Test
    public void testWritingWithImports()
        throws Exception
    {
        final int FRED_ID_OFFSET   = systemMaxId();
        final int GINGER_ID_OFFSET = FRED_ID_OFFSET + FRED_MAX_IDS[1];

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
        checkSymbol("localSym", local);

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
        checkSymbol("localSym", local);

        SymbolTable symtab = f2sym.getSymbolTable();
        assertSame(symtab, local.getSymbolTable());
        SymbolTable[] importedTables = symtab.getImportedTables();
        assertEquals(1, importedTables.length);
        assertSame(fred1, importedTables[0]);
    }

    // TODO test stepOut() when at top-level

    @Test
    public void testWritingFieldName()
        throws Exception
    {
        iw = makeWriter();
        iw.stepIn(IonType.STRUCT);
        iw.setFieldName("foo");
        iw.setFieldNameSymbol(newSymbolToken((String) null, 99)); // Replaces "foo"
        iw.writeNull();
        iw.stepOut();

        IonReader r = reread();
        r.next();
        r.stepIn();
        r.next();
        check(r).fieldName(null, 99);
    }

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
        checkSymbol(text, t);
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

    @Test
    public void testWritingUnknownSymbol()
        throws Exception
    {
        iw = makeWriter();
        iw.writeSymbolToken(newSymbolToken((String) null, 99));

        IonReader in = reread();
        in.next();
        IonAssert.checkSymbol(in, null, 99);
    }

    @Test
    public void testWritingSidlikeSymbols()
        throws Exception
    {
        iw = makeWriter();
        iw.writeSymbol("$99");

        IonReader in = reread();
        in.next();
        IonAssert.checkSymbol("$99", in);
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


    @Test
    public void testWritingDeepNestedList() throws Exception {
        IonDatagram dg = loader().load("[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]");
        iw = makeWriter();
        dg.writeTo(iw);
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
        expectNextField(ir, "a");

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
        expectNextField(ir, "a");

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

        iw.addTypeAnnotation("b");
        iw.setTypeAnnotationSymbols(new SymbolToken[0]);
        iw.writeNull();
        v = expected.add().newNull();
        v.clearTypeAnnotations();

        iw.addTypeAnnotation("b");
        iw.setTypeAnnotationSymbols((SymbolToken[])null);
        iw.writeNull();
        v = expected.add().newNull();
        v.clearTypeAnnotations();

        iw.addTypeAnnotation("b");
        iw.setTypeAnnotationSymbols();
        iw.writeNull();
        v = expected.add().newNull();
        v.clearTypeAnnotations();

        assertEquals(expected, reload());
    }

    /**
     * Test case to ensure that {@link IonWriter#setTypeAnnotations(String...)}
     * and {@link IonWriter#setTypeAnnotationSymbols(SymbolToken...)} clearing
     * of type annotations behavior is correct.
     */
    @Test
    public void testWritingClearedAnnotations()
        throws Exception
    {
        iw = makeWriter();
        IonDatagram expected = system().newDatagram();

        //===== Test on IonWriter.setTypeAnnotationSymbols(...) =====

        iw.setTypeAnnotationSymbols(new SymbolToken[0]); // empty SymbolToken[]
        iw.writeNull();
        IonValue v = expected.add().newNull();
        v.clearTypeAnnotations();

        iw.setTypeAnnotationSymbols((SymbolToken[]) null); // null SymbolToken[]
        iw.writeNull();
        v = expected.add().newNull();
        v.clearTypeAnnotations();

        iw.addTypeAnnotation("b");
        iw.setTypeAnnotationSymbols(new SymbolToken[0]); // empty SymbolToken[]
        iw.writeNull(); // expected: the pending "b" annotation is cleared
        v = expected.add().newNull();
        v.clearTypeAnnotations();

        iw.addTypeAnnotation("b");
        iw.setTypeAnnotationSymbols((SymbolToken[]) null); // null SymbolToken[]
        iw.writeNull(); // expected: the pending "b" annotation is cleared
        v = expected.add().newNull();
        v.clearTypeAnnotations();



        //===== Test on IonWriter.setTypeAnnotations(...) =====

        iw.setTypeAnnotations(new String[0]); // empty String[]
        iw.writeNull();
        v = expected.add().newNull();
        v.clearTypeAnnotations();

        iw.setTypeAnnotations((String[]) null); // null String[]
        iw.writeNull();
        v = expected.add().newNull();
        v.clearTypeAnnotations();

        iw.addTypeAnnotation("b");
        iw.setTypeAnnotations(new String[0]); // empty String[]
        iw.writeNull(); // expected: the pending "b" annotation is cleared
        v = expected.add().newNull();
        v.clearTypeAnnotations();

        iw.addTypeAnnotation("b");
        iw.setTypeAnnotations((String[]) null); // null String[]
        iw.writeNull(); // expected: the pending "b" annotation is cleared
        v = expected.add().newNull();
        v.clearTypeAnnotations();


        assertEquals(expected, reload());
    }

    @Test
    public void testWritingAnnotationWithUnknownText()
        throws Exception
    {
        iw = makeWriter();
        IonDatagram expected = system().newDatagram();

        iw.setTypeAnnotationSymbols(newSymbolToken((String) null, 99));
        iw.writeNull(); // expected: the type annotation is written
        IonValue v = expected.add().newNull();
        v.setTypeAnnotationSymbols(newSymbolToken(99));

        assertEquals(expected, reload());
    }

    @Test
    public void testWritingAnnotationWithBadSid()
        throws Exception
    {
        iw = makeWriter();
        IonDatagram expected = system().newDatagram();

        iw.setTypeAnnotationSymbols(newSymbolToken("a", 99));
        iw.writeNull(); // expected: the type annotation is written
        IonValue v = expected.add().newNull();
        v.setTypeAnnotations("a");

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

    Iterator<IonValue> systemIterateOutput()
        throws Exception
    {
        byte[] data = outputByteArray();
        Iterator<IonValue> it = system().systemIterate(data);
        return it;
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

        // Should have: IVM SYMTAB hey IVM null

        Iterator<IonValue> it = systemIterateOutput();

        if (myOutputForm != OutputForm.TEXT) { // TODO amznlabs/ion-java#8
            checkSymbol(SystemSymbols.ION_1_0, it.next());
        }
        checkAnnotation(SystemSymbols.ION_SYMBOL_TABLE, it.next());
        // TODO amznlabs/ion-java#63
        if (myOutputForm != OutputForm.TEXT)
        {
            checkSymbol(null, 12, it.next());
        }
        else
        {
            checkSymbol("hey", it.next());
        }
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
    }

    @Test
    public void testWritingEmptySymtab()
    throws Exception
    {
        iw = makeWriter();
        iw.addTypeAnnotation(ION_SYMBOL_TABLE);
        iw.stepIn(IonType.STRUCT);
        iw.stepOut();
        iw.writeSymbol("foo");
        iw.close();

        IonDatagram dg = reload();
        IonSymbol foo = (IonSymbol) dg.get(0);
        assertEquals(0, foo.getTypeAnnotations().length);
    }


    @Test @Ignore // TODO amznlabs/ion-java#15
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

    @Test
    public void testWriteSymbolTokenWithGoodSid()
        throws Exception
    {
        iw = makeWriter();

        iw.writeSymbolToken(newSymbolToken((String) null, NAME_SID));

        IonDatagram dg = reload();
        IonSymbol s = (IonSymbol) dg.get(0);
        checkSymbol(SystemSymbols.NAME, NAME_SID, s);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteSymbolTokenWithBadSid()
        throws Exception
    {
        iw = makeWriter();
        // Using FakeSymbolToken instead of new SymbolTokenImpl as
        // newSymbolToken(...) throws an AssertionError during construction
        // before it reaches the code-block that throws IllegalArgumentException
        // during writing.
        iw.writeSymbolToken(new FakeSymbolToken(null, -12));
    }


    @Test
    public void testWriteValuesWithSymtab()
        throws Exception
    {
        SymbolTable fredSymtab =
            Symtabs.register(Symtabs.FRED_NAME, 1, catalog());
        SymbolTable gingerSymtab =
            Symtabs.register(Symtabs.GINGER_NAME, 1, catalog());
        String gingerSym = gingerSymtab.findKnownSymbol(1);

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
        // Should've assigned a new SID
        checkSymbol(gingerSym, s);
    }

    /**
     * TODO amznlabs/ion-java#8 datagram is lazy creating local symtabs.
     * Should use a reader to check the results.
     */
    @Test
    public void testWriteIVMImplicitly()
        throws Exception
    {
        iw = makeWriter();
        iw.writeSymbol("foo");
        iw.writeSymbol(SystemSymbols.ION_1_0);
        iw.writeInt(1);
        iw.writeSymbolToken(newSymbolToken(SystemSymbols.ION_1_0_SID));
        iw.writeInt(2);

        Iterator<IonValue> it = systemIterateOutput();
        if (myOutputForm != OutputForm.TEXT) {
            checkSymbol(SystemSymbols.ION_1_0, it.next());
        }
        if (myOutputForm == OutputForm.BINARY) {
            checkAnnotation(ION_SYMBOL_TABLE, it.next());
        }
        // TODO amznlabs/ion-java#63
        if (myOutputForm == OutputForm.BINARY)
        {
            checkSymbol(null, 10, it.next());
        }
        else
        {
            checkSymbol("foo", it.next());
        }
        checkSymbol(SystemSymbols.ION_1_0, it.next());
        checkInt(1, it.next());
        checkSymbol(SystemSymbols.ION_1_0, it.next());
        checkInt(2, it.next());
    }

    @Test
    public void testWriteIVMExplicitly()
        throws Exception
    {
        iw = makeWriter();
        iw.writeSymbol("foo");
        ((PrivateIonWriter)iw).writeIonVersionMarker();
        iw.writeInt(1);

        IonDatagram dg = reload();
        assertEquals(2, dg.size());
    }

    @Test // TODO amznlabs/ion-java#8 Inconsistencies between writers
    public void testWritingDatagram()
        throws Exception
    {
        IonDatagram dg = loader().load("foo");
        iw = makeWriter();
        dg.writeTo(iw);

        Iterator<IonValue> it = systemIterateOutput();
        //if (myOutputIsBinary)
        {
            checkSymbol(SystemSymbols.ION_1_0, it.next());
        }
        if (myOutputForm != OutputForm.TEXT) {
            checkAnnotation(SystemSymbols.ION_SYMBOL_TABLE, it.next());
        }
        // TODO amznlabs/ion-java#63
        if (myOutputForm != OutputForm.TEXT)
        {
            checkSymbol(null, 10, it.next());
        }
        else
        {
            checkSymbol("foo", it.next());
        }
    }

    @Test
    public void testWritingFieldWithoutName()
        throws Exception
    {
        iw = makeWriter();
        iw.stepIn(IonType.STRUCT);
        try {
            iw.writeNull();
            fail("Expected exception");
        }
        catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains(ERROR_MISSING_FIELD_NAME));
        }
    }

    @Test
    public void testWritingNestedSymtab()
        throws Exception
    {
        iw = makeWriter();
        iw.addTypeAnnotation(SystemSymbols.ION_SYMBOL_TABLE);
        iw.stepIn(IonType.STRUCT);
        {
            assertEquals(1, ((PrivateIonWriter)iw).getDepth());

            iw.setFieldName("open");
            iw.addTypeAnnotation(SystemSymbols.ION_SYMBOL_TABLE);
            iw.stepIn(IonType.STRUCT);
            {
                assertEquals(2, ((PrivateIonWriter)iw).getDepth());
            }
            iw.stepOut();
        }
        iw.stepOut();
    }

    @Test
    public void testWritingLob()
        throws Exception
    {
        byte[] lobData = new byte[]{ 19, 0, Byte.MAX_VALUE, Byte.MIN_VALUE };
        iw = makeWriter();
        iw.writeClob(lobData);
        iw.writeBlob(lobData);

        IonReader r = reread();
        r.next();
        byte[] d = r.newBytes();
        assertArrayEquals(lobData, d);

        r.next();
        d = r.newBytes();
        assertArrayEquals(lobData, d);
    }

    @Test
    public void testAnnotationNotSetToIvmAfterFinish()
        throws Exception
    {
        iw = makeWriter();
        IonDatagram expected = system().newDatagram();

        IonValue value1 = oneValue("1");
        value1.writeTo(iw);
        // Resets the stream context, the next TLV that is written must behave
        // as if it is preceded by an IVM, depending on the writer's config.
        iw.finish();
        expected.add(value1);

        IonValue value2 = oneValue("2");
        value2.addTypeAnnotation("some_annot");
        value2.writeTo(iw);
        iw.close();
        // Expect: an annotation on value2
        expected.add(value2);

        IonAssert.assertIonEquals(expected, reload());
    }

    @Test
    public void testAnnotationNotSetToIvmOnStartOfStream()
        throws Exception
    {
        iw = makeWriter();
        IonDatagram expected = system().newDatagram();

        IonValue value1 = oneValue("1");
        value1.addTypeAnnotation("some_annot");
        value1.writeTo(iw);
        // Expect: an annotation on value1
        expected.add(value1);

        IonValue value2 = oneValue("2");
        value2.writeTo(iw);
        iw.close();
        expected.add(value2);

        IonAssert.assertIonEquals(expected, reload());
    }

    @Test
    public void testAnnotationNotSetToSymbolTable()
        throws Exception
    {
        iw = makeWriter();

        // ===== write =====
        IonValue value1 = oneValue("1");
        value1.writeTo(iw);
        iw.finish();

        // Shared symbol table injected
        SymbolTable sharedSymTab = loadSharedSymtab(Symtabs.FRED_SERIALIZED[1]);
        sharedSymTab.writeTo(iw);

        IonValue value2 = oneValue("2");
        value2.addTypeAnnotation("some_annot_2");
        value2.writeTo(iw);

        // Local symbol table injected
        SymbolTable fred1 = Symtabs.CATALOG.getTable("fred", 1);
        SymbolTable localSymTab = system().newLocalSymbolTable(fred1);
        checkLocalTable(localSymTab);
        localSymTab.writeTo(iw);

        IonValue value3 = oneValue("3");
        value3.addTypeAnnotation("some_annot_3");
        value3.writeTo(iw);
        iw.close();

        // ===== re-read and check =====
        IonReader reader = reread();

        assertEquals(IonType.INT, reader.next());
        assertEquals(1, reader.intValue());

        assertEquals(IonType.STRUCT, reader.next());
        assertArrayEquals(new String[] {ION_SHARED_SYMBOL_TABLE},
                          reader.getTypeAnnotations());

        assertEquals(IonType.INT, reader.next());
        assertEquals(2, reader.intValue());
        assertArrayEquals(new String[] {"some_annot_2"},
                          reader.getTypeAnnotations());

        assertEquals(IonType.INT, reader.next());
        assertEquals(3, reader.intValue());
        assertArrayEquals(new String[] {"some_annot_3"},
                          reader.getTypeAnnotations());

        SymbolTable readLocalSymTab = reader.getSymbolTable();
        checkLocalTable(readLocalSymTab);

        assertNull(reader.next());
    }


    @Test
    public void testWriteToWithFieldName()
        throws Exception
    {
        IonStruct s = system().newEmptyStruct();
        IonNull n = s.add("f").newNull();

        iw = makeWriter();

        n.writeTo(iw);
        iw.stepIn(IonType.STRUCT);
        {
            n.writeTo(iw);
            iw.setFieldName("g");
            n.writeTo(iw);
        }
        iw.stepOut();

        IonReader reader = reread();
        assertEquals(IonType.NULL, reader.next());

        assertEquals(IonType.STRUCT, reader.next());
        assertIonEquals(oneValue("{f:null, g:null}"),
                        system().newValue(reader));

        assertNull(reader.next());
    }
}
