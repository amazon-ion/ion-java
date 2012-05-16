// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.impl._Private_Utils.EMPTY_BYTE_ARRAY;

import com.amazon.ion.impl._Private_Utils;
import com.amazon.ion.system.SimpleCatalog;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.zip.GZIPOutputStream;
import org.junit.Test;

/**
 *
 */
public class IonSystemTest
    extends IonTestCase
{
    //========================================================================
    // iterate(Reader)

    @Test(expected = NullPointerException.class)
    public void testIterateNullReader()
    {
        system().iterate((Reader) null);
    }

    @Test
    public void testIterateEmptyReader()
    {
        Reader in = new StringReader("");
        Iterator<IonValue> i = system().iterate(in);
        assertFalse(i.hasNext());
    }


    //========================================================================
    // iterate(InputStream)

    @Test(expected = NullPointerException.class)
    public void testIterateNullInputStream()
    {
        system().iterate((InputStream) null);
    }

    @Test
    public void testIterateEmptyInputStream()
    {
        InputStream in = new ByteArrayInputStream(EMPTY_BYTE_ARRAY);
        Iterator<IonValue> i = system().iterate(in);
        assertFalse(i.hasNext());
    }


    //========================================================================
    // iterate(String)

    @Test(expected = NullPointerException.class)
    public void testIterateNullString()
    {
        system().iterate((String) null);
    }

    @Test
    public void testIterateEmptyString()
    {
        Iterator<IonValue> i = system().iterate("");
        assertFalse(i.hasNext());
    }


    //========================================================================
    // iterate(byte[])

    @Test(expected = NullPointerException.class)
    public void testIterateNullBytes()
    {
        system().iterate((byte[]) null);
    }

    @Test
    public void testIterateEmptyBytes()
    {
        Iterator<IonValue> i = system().iterate(EMPTY_BYTE_ARRAY);
        assertFalse(i.hasNext());
    }


    //========================================================================
    // singleValue(String)

    @Test(expected = NullPointerException.class)
    public void testSingleValueNullString()
    {
        system().singleValue((String) null);
    }

    @Test(expected = UnexpectedEofException.class)
    public void testSingleValueEmptyString()
    {
        system().singleValue("");
    }


    /**
     * Ensure that singleValue() can handle Unicode.
     */
    @Test
    public void testSingleValueUtf8()
    {
        String utf8Text = "Tivoli Audio Model One weiß/silber";
        String data = '"' + utf8Text + '"';

        IonString v = (IonString) system().singleValue(data);
        assertEquals(utf8Text, v.stringValue());

        byte[] binary = encode(data);
        v = (IonString) system().singleValue(binary);
        assertEquals(utf8Text, v.stringValue());
    }


    //========================================================================
    // singleValue(byte[])

    @Test(expected = NullPointerException.class)
    public void testSingleValueNullBytes()
    {
        system().singleValue((byte[]) null);
    }

    @Test(expected = UnexpectedEofException.class)
    public void testSingleValueEmptyBytes()
    {
        system().singleValue(EMPTY_BYTE_ARRAY);
    }


    //========================================================================
    // clone(IonValue)

    /**
     * check for clone across two systems failing to
     * detach the child from the datagram constructing
     * the clone
     */
    @Test
    public void testTwoSystemsClone()
    {
        IonSystem system1 = system();
        IonSystem system2 = system(new SimpleCatalog());

        IonValue v1 = system1.singleValue("just_a_symbol");
        IonValue v2 = system2.clone(v1);

        IonStruct s = system2.newEmptyStruct();
        s.add("field1", v2);
    }


    //========================================================================
    // newLoader(...)

    @Test
    public void testNewLoaderDefaultCatalog()
    {
        IonLoader loader = system().newLoader();
        assertSame(catalog(), loader.getCatalog());
    }

    @Test
    public void testNewLoaderNullCatalog()
    {
        IonCatalog catalog = null;
        IonLoader loader = system().newLoader(catalog);
        assertSame(catalog(), loader.getCatalog());
    }


    //========================================================================
    // newDatagram(...)

    @Test
    public void testNewDatagramImporting()
    {
        // FIXME ION-75
        if (getDomType() == DomType.LITE)
        {
            logSkippedTest();
            return;
        }

        IonSystem ion = system();
        SymbolTable st =
            ion.newSharedSymbolTable("foobar", 1,
                                     Arrays.asList("s1").iterator());
        // st is not in the system catalog, but that shouldn't matter.

        IonDatagram dg = ion.newDatagram(st);
        dg.add().newSymbol("s1");
        dg.add().newSymbol("l1");
        byte[] bytes = dg.getBytes();


        IonDatagram dg2 = loader().load(bytes);
        IonSymbol s1 = (IonSymbol) dg2.get(0);
        checkUnknownSymbol(systemMaxId() + 1, s1);

        SymbolTable symbolTable = s1.getSymbolTable();
        assertEquals("foobar", symbolTable.getImportedTables()[0].getName());
        checkSymbol("l1", 11, symbolTable);
    }


    //========================================================================
    // newValue(IonReader)

    @Test(expected = IonException.class)
    public void testNewValueFailsWhenNoCurrentValue()
    {
        IonReader r = system().newReader("hi");
        // we don't call next()!
        system().newValue(r);
    }


    private byte[] gzip(byte[] input)
        throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream gzos = new GZIPOutputStream(baos);
        gzos.write(input);
        gzos.close();
        return baos.toByteArray();
    }

    private void checkGzipDetection(IonReader r)
    {
        assertEquals(IonType.INT, r.next());
        assertEquals(1234, r.intValue());
        assertEquals(null, r.next());
    }

    private void checkGzipDetection(byte[] bytes)
        throws Exception
    {
        IonSystem system = system();

        IonValue v = system.singleValue(bytes);
        checkInt(1234, v);

        IonReader r = system.newReader(bytes);
        checkGzipDetection(r);

        byte[] padded = new byte[bytes.length + 70];
        System.arraycopy(bytes, 0, padded, 37, bytes.length);
        r = system.newReader(padded, 37, bytes.length);
        checkGzipDetection(r);

        InputStream in = new ByteArrayInputStream(bytes);
        r = system.newReader(in);
        checkGzipDetection(r);

        Iterator<IonValue> i = system.iterate(bytes);
        checkInt(1234, i.next());

        in = new ByteArrayInputStream(bytes);
        i = system.iterate(in);
        checkInt(1234, i.next());

        IonLoader loader = loader();
        IonDatagram dg = loader.load(bytes);
        checkInt(1234, dg.get(0));

        in = new ByteArrayInputStream(bytes);
        dg = loader.load(in);
        checkInt(1234, dg.get(0));
    }

    @Test
    public void testGzipDetection()
        throws Exception
    {
        String ionText = "1234";
        byte[] textBytes = _Private_Utils.utf8(ionText);
        byte[] gzipTextBytes = gzip(textBytes);

        checkGzipDetection(textBytes);
        checkGzipDetection(gzipTextBytes);

        byte[] binaryBytes = loader().load(ionText).getBytes();
        byte[] gzipBinaryBytes = gzip(binaryBytes);

        checkGzipDetection(binaryBytes);
        checkGzipDetection(gzipBinaryBytes);
    }
}
