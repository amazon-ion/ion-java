/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.util;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonList;
import com.amazon.ion.IonLoader;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonValue;
import com.amazon.ion.SystemSymbolTable;
import com.amazon.ion.system.StandardIonSystem;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

/**
 *
 */
public class LoaderTest
    extends IonTestCase
{
    private IonLoader myLoader;


    public void setUp()
        throws Exception
    {
        super.setUp();
        myLoader = system().newLoader();
    }

    public IonDatagram loadFile(String filename)
        throws IOException
    {
        File text = getTestdataFile(filename);
        return myLoader.loadText(text);
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

        IonList parent = system().newList();
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
        IonStruct v = system().newStruct();
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
        IonStruct struct1 = system().newStruct();
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
}
