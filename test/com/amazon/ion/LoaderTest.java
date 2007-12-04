/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import com.amazon.ion.system.StandardIonSystem;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;

/**
 *
 */
public class LoaderTest
    extends IonTestCase
{
    private IonLoader myLoader;


    @Override
    public void setUp()
        throws Exception
    {
        super.setUp();
        myLoader = system().newLoader();
    }


    /**
     * Parses text as a single Ion value.  If the text contains more than that,
     * a failure is thrown.
     *
     * @param text must not be <code>null</code>.
     * @return a single value, not <code>null</code>.
     */
    public IonValue loadOneValue(String text)
    {
        IonDatagram dg = myLoader.loadText(text);

        if (dg.size() == 0)
        {
            fail("No user values in text: " + text);
        }

        if (dg.size() > 1)
        {
            IonValue part = dg.get(1);
            fail("Found unexpected part <" + part + "> in text: " + text);
        }

        IonValue value = dg.get(0);
        dg.remove(value);
        return value;
    }


    //=========================================================================
    // Test cases

    public void testLoadingNonexistentFile()
        throws IOException
    {
        try
        {
            loadFile("good/no such file");
            fail("expected FileNotFoundException");
        }
        catch (FileNotFoundException e) { }
    }

    public void testLoadingBlankTextFiles()
        throws Exception
    {
        IonDatagram contents = loadFile("good/blank.ion");
        assertEquals(0, contents.size());

        contents = loadFile("good/empty.ion");
        assertEquals(0, contents.size());
    }

    public void testLoadingSimpleFile()
        throws Exception
    {
        IonDatagram contents = loadFile("good/one.ion");
        assertEquals(1, contents.size());
        IonInt one = (IonInt) contents.get(0);
        assertEquals(1, one.intValue());
        assertSame(contents, one.getContainer());

        contents = loadFile("good/allNulls.ion");
        assertEquals(1, contents.size());
        IonList nullsList = (IonList) contents.get(0);
        assertEquals(14, nullsList.size());
        assertSame(contents, nullsList.getContainer());

        // Load some binary.
        contents = loadFile("good/null.10n");
        assertEquals(1, contents.size());
        IonNull nullValue = (IonNull) contents.get(0);
        assertNotNull(nullValue);
    }

    public void testIteratingSimpleFile()
        throws Exception
    {
        File text = getTestdataFile("good/nonNulls.ion");
        FileInputStream fileStream = new FileInputStream(text);
        try
        {
            Reader reader = new InputStreamReader(fileStream, "UTF-8");
            IonReader i = system().newTextReader(reader);
            while (i.hasNext())
            {
                IonValue value = i.next();
                assertFalse(value.isNullValue());
                assertNull(value.getContainer());
            }
            i.close();
        }
        finally
        {
            fileStream.close();
        }
    }

    public void testLoadOneValue()
    {
        IonList value = (IonList) loadOneValue("[1]");
        assertNull(value.getContainer());
        IonInt elt = (IonInt) value.get(0);
        value.remove(elt);
        assertNull(elt.getContainer());
        checkInt(1, elt);
    }

    public void testIgnoreHeaderSymbol()
    {
        String text = SystemSymbolTable.ION_1_0 + " 123";

        IonInt value = (IonInt) loadOneValue(text);
        checkInt(123, value);
    }

    public void testClone()
        throws Exception
    {
        IonDatagram contents = loadFile("good/one.ion");
        IonValue one = contents.get(0);
        assertEquals(contents, one.getContainer());

        IonValue onePrime = system().clone(one);
        assertNotSame(one, onePrime);
        assertIonEquals(one, onePrime);
        assertNull("cloned value has container", onePrime.getContainer());

        IonList parent = system().newNullList();
        parent.add(onePrime);
        assertSame(parent, onePrime.getContainer());
        checkInt(1, onePrime);

        ((IonInt)one).setValue(2);
        checkInt(2, one);
        checkInt(1, onePrime);
    }

    // TODO move to ScannerTest
    public void testCloneWithAnnotation() {
        String s = "some_annotation::{foo:\"test\"}";
        IonStruct v = system().newNullStruct();
        v.put("bar", loadOneValue(s));
        IonStruct bar = (IonStruct) v.get("bar");
        bar.get("foo");
        IonStruct v2 = system().clone(v);
        bar = (IonStruct) v2.get("bar");
        bar.get("foo");
    }

    public void testCloneChildWithAnnotation()
    {
        String s = "some_annotation::{foo:\"test\"}";
        IonValue v = loadOneValue(s);
        IonStruct struct1 = system().newNullStruct();
        struct1.put("bar", v);
        IonValue bar = struct1.get("bar");
        assert bar instanceof IonStruct;
        ((IonStruct)bar).get("foo");

        v = loadOneValue("{a:\"aaaa\", b:\"bbbb\"}");
        assert v instanceof IonStruct;
        IonStruct struct2 = (IonStruct)v;

        IonValue child = struct1.get("bar");
        IonValue clone = system().clone(child);

        struct2.put("clone", clone);

        v = struct2.get("clone");
        assert v instanceof IonStruct;

        v = ((IonStruct)v).get("foo");
        assert v instanceof IonString;
        assert ((IonString)v).stringValue().equals("test");
    }


    public void testReloadingTextSeveralWays()
        throws IOException
    {
        String s = "ann::{field:value}";

        IonLoader loader = loader();

        IonDatagram dg = loader.loadText(s);
        checkReloading(dg);

        StringReader reader = new StringReader(s);
        dg = loader.loadText(reader);
        checkReloading(dg);

        // TODO Test passing (unnecessary) local symbol table.
//        reader = new StringReader(s);
//        dg = loader.loadText(reader, (LocalSymbolTable) null);
//        checkReloading(dg);

        byte[] bytes = s.getBytes("UTF-8");
        dg = loader.load(bytes);
//        checkReloading(dg);   FIXME this is a big bug!

        InputStream in = new ByteArrayInputStream(bytes);
        dg = loader.load(in);
        checkReloading(dg);

        in = new ByteArrayInputStream(bytes);
        dg = loader.loadText(in);
        checkReloading(dg);
    }

    public void checkReloading(IonDatagram dg)
    {
        byte[] binary = dg.toBytes();

        IonDatagram dg2 = loader().load(binary);
        assertEquals(1, dg2.size());
        IonStruct struct = (IonStruct) dg2.get(0);
        assertTrue(struct.hasTypeAnnotation("ann"));
        IonSymbol value = (IonSymbol) struct.get("field");
        assertEquals("value", value.stringValue());
    }

    public void testSingleValue()
    {
        StandardIonSystem sys = new StandardIonSystem();

        String image = "(this is a single sexp)";
        IonValue v1 =  sys.singleValue(image);

        IonDatagram dg = sys.newLoader().loadText(image);
        assert  v1.toString().equals( dg.get(0).toString() );

        byte[] bytes = dg.toBytes();

        IonValue v2 = sys.singleValue(bytes);
        assert v1.toString().equals( v2.toString() );

        try {
            v1 = sys.singleValue("(one) (two)");
            fail("Expected IonException");
        }
        catch (IonException ie) {
            // we hope for this case, when all is well
        }

        try {
            v1 = sys.singleValue("");
            fail("Expected IonException");
        }
        catch (IonException ie) { /* ok */ }
    }



    public static void main(String[] args)
        throws Exception
    {
        File f = getTestdataFile("good/decimalNegativeOneDotZero.10n");
        FileOutputStream out = new FileOutputStream(f);
        try
        {
            int[] data = { 0x10, 0x14, 0x01, 0x00,  // binary version marker
                           0x52, 0xC1, 0x8A
            };

            for (int i = 0; i < data.length; i++)
            {
                int b = data[i];
                out.write(b);
            }
        }
        finally
        {
            out.close();
        }
    }
}
