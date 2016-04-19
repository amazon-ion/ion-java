/*
 * Copyright 2010-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion;

import static software.amazon.ion.impl.PrivateUtils.EMPTY_BYTE_ARRAY;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Iterator;
import java.util.zip.GZIPOutputStream;
import org.junit.Test;
import software.amazon.ion.IonCatalog;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonException;
import software.amazon.ion.IonInt;
import software.amazon.ion.IonLoader;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonString;
import software.amazon.ion.IonSymbol;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.UnexpectedEofException;
import software.amazon.ion.impl.PrivateUtils;
import software.amazon.ion.system.SimpleCatalog;

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
     * Test that the cloned IonValue from IonSystem.clone() successfully
     * detaches from the internally materialized IonDatagram and the original
     * value's container (if it exists) when it is cloning across two
     * different IonSystems.
     */
    @Test
    public void testCloneDetachesFromDatagramOnDiffSystemClone()
    {
        IonValue original = system().singleValue("some_symbol");

        IonValue clone =
            newSystem(new SimpleCatalog()).clone(original);

        assertNull("Cloned value should not have a container (parent)",
                   clone.getContainer());
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

        IonSystem ion = system();
        SymbolTable st =
            ion.newSharedSymbolTable("foobar", 1,
                                     Arrays.asList("s1").iterator());
        // st is not in the system catalog, but that shouldn't matter.

        IonDatagram dg = ion.newDatagram(st);
        dg.add().newSymbol("s1");
        dg.add().newSymbol("l1");
        byte[] bytes = dg.getBytes();

        Iterator<IonValue> iterator = dg.iterator();

        IonSymbol symbol1 = (IonSymbol)iterator.next();
        SymbolToken token1 = symbol1.symbolValue();
        assertEquals("s1", token1.getText());
        assertEquals(10, token1.getSid());

        SymbolTable local = symbol1.getSymbolTable();
        Iterator<String> declaredSymbols = local.iterateDeclaredSymbolNames(); //doesn't include shared symbols
        assertEquals("l1", declaredSymbols.next());
        assertFalse(declaredSymbols.hasNext()); //s1 shouldn't be here because it's in a shared import

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

    @Test
    public void testNewValueWithHugeInt()
    {
        String huge = Long.MAX_VALUE + "0";
        IonInt i = system().newInt(new BigInteger(huge));
        IonReader r = system().newReader(i);
        r.next();
        IonInt i2 = (IonInt) system().newValue(r);
        assertEquals(i, i2);
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
        byte[] textBytes = PrivateUtils.utf8(ionText);
        byte[] gzipTextBytes = gzip(textBytes);

        checkGzipDetection(textBytes);
        checkGzipDetection(gzipTextBytes);

        byte[] binaryBytes = loader().load(ionText).getBytes();
        byte[] gzipBinaryBytes = gzip(binaryBytes);

        checkGzipDetection(binaryBytes);
        checkGzipDetection(gzipBinaryBytes);
    }
}
