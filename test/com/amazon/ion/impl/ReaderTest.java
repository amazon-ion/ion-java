// Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.
package com.amazon.ion.impl;

import static com.amazon.ion.impl.IonImplUtils.EMPTY_BYTE_ARRAY;
import static com.amazon.ion.impl.IonImplUtils.utf8;

import com.amazon.ion.BinaryTest;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonType;
import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.junit.Test;

/**
 *
 */
public class ReaderTest
    extends IonTestCase
{
    class InputStreamWrapper extends FilterInputStream
    {
        protected InputStreamWrapper(InputStream in)
        {
            super(in);
        }

        boolean closed = false;

        @Override
        public void close() throws IOException
        {
            assertFalse("stream already closed", closed);
            closed = true;
            super.close();
        }
    }


    /**
     * Fails if the iterator has a next element.
     */
    private void checkEmptyIterator(Iterator i)
    {
        try
        {
            i.next();
            fail("Expected NoSuchElementException");
        }
        catch (NoSuchElementException e) { /* good */ }
        assertFalse("iterator should be empty", i.hasNext());
    }


    //=========================================================================
    // Test cases

    @Test
    public void testClosingStream()
    throws Exception
    {
        byte[] data = utf8("test");
        InputStreamWrapper stream =
            new InputStreamWrapper(new ByteArrayInputStream(data));
        IonReader reader = system().newReader(stream);
        assertSame(IonType.SYMBOL, reader.next());
        reader.close();
        assertTrue("stream not closed", stream.closed);
    }

    @Test
    public void testClosingEmptyStream()
    throws Exception
    {
        InputStreamWrapper stream =
            new InputStreamWrapper(new ByteArrayInputStream(EMPTY_BYTE_ARRAY));
        IonReader reader = system().newReader(stream);
        assertSame(null, reader.next());
        reader.close();
        assertTrue("stream not closed", stream.closed);
    }

    @Test
    public void testNullInt()
    {
        {
            IonReader scanner = system().newReader("null.int");
            assertEquals(IonType.INT, scanner.next());
            assertTrue(scanner.isNullValue());
            assertEquals(null, scanner.bigIntegerValue());
        }
        {
            for (final String hex : Arrays.asList("E0 01 00 EA 2F", "E0 01 00 EA 3F")) {
                IonReader scanner = system().newReader(BinaryTest.hexToBytes(hex));
                assertEquals(IonType.INT, scanner.next());
                assertTrue(scanner.isNullValue());
                assertEquals(null, scanner.bigIntegerValue());
            }
        }
    }

/* jonker 2008-05-29 Disabled everything while we (temporarily) remove IonReader

    public void testSimpleScan()
    {
        IonReader scanner = system().newReader("abc");
        SymbolTable symtab = scanner.getLocalSymbolTable();

        IonSymbol value = (IonSymbol) scanner.next();

        assertSame(symtab, scanner.getLocalSymbolTable());
        assertEquals(value.intValue(), symtab.findSymbol("abc"));
    }


    public void testCustomSid()
    {
        IonReader scanner = system().newReader("abc");
        LocalSymbolTable symtab = scanner.getLocalSymbolTable();
        assertEquals(-1, symtab.findSymbol("abc"));

        symtab.addSymbol("blah");
        symtab.addSymbol("boo");
        int sid = symtab.addSymbol("abc");

        IonSymbol value = (IonSymbol) scanner.next();

        assertSame(symtab, scanner.getLocalSymbolTable());
        assertEquals(sid, value.intValue());
    }


    public void testIncrementalParsing()
    {
        IonReader scanner = system().newReader("abc def ghi");
        LocalSymbolTable symtab = scanner.getLocalSymbolTable();


        IonSymbol value = (IonSymbol) scanner.next();
        checkSymbol("abc", value);
        assertEquals(-1, symtab.findSymbol("def"));
        assertEquals(-1, symtab.findSymbol("ghi"));

        value = (IonSymbol) scanner.next();
        checkSymbol("def", value);
        assertTrue(-1 != symtab.findSymbol("def"));
        assertEquals(-1, symtab.findSymbol("ghi"));

        value = (IonSymbol) scanner.next();
        checkSymbol("ghi", value);
        assertTrue(-1 != symtab.findSymbol("def"));
        assertTrue(-1 != symtab.findSymbol("ghi"));
    }


    public void testSettingSymbolTable()
    {
        IonReader scanner = system().newReader("abc def ghi");
        LocalSymbolTable symtab0 = scanner.getLocalSymbolTable();


        IonSymbol value = (IonSymbol) scanner.next();  // abc

        assertEquals(-1, symtab0.findSymbol("def"));
        assertEquals(-1, symtab0.findSymbol("ghi"));


        LocalSymbolTable symtab1 = system().newLocalSymbolTable();
        int defId = symtab1.addSymbol("def");
        scanner.setLocalSymbolTable(symtab1);
        assertSame(symtab1, scanner.getLocalSymbolTable());

        value = (IonSymbol) scanner.next();            // def
        assertEquals(defId, value.intValue());
        assertEquals(-1, symtab0.findSymbol("def"));
        assertEquals(-1, symtab0.findSymbol("ghi"));
        assertEquals(-1, symtab1.findSymbol("ghi"));


        LocalSymbolTable symtab2 = system().newLocalSymbolTable();
        int ghiId = symtab2.addSymbol("ghi");
        scanner.setLocalSymbolTable(symtab2);
        assertSame(symtab2, scanner.getLocalSymbolTable());

        value = (IonSymbol) scanner.next();            // ghi
        assertEquals(-1,    symtab0.findSymbol("ghi"));
        assertEquals(-1,    symtab1.findSymbol("ghi"));
        assertEquals(-1,    symtab2.findSymbol("def"));
        assertEquals(ghiId, symtab2.findSymbol("ghi"));
        assertEquals(ghiId, value.intValue());
    }


    public void testScannerTermination()
    {
        IonReader scanner = system().newReader("");
        checkEmptyIterator(scanner);
        scanner.close();

        // Try calling next before hasNext
        scanner= system().newReader("1");
        try {
            scanner.next();
            checkEmptyIterator(scanner);
        }
        finally {
            scanner.close();
        }


        // TODO imlement raw Scanning via public API
        Iterator<IonValue> iterator = new SystemReader(system(), "");
        checkEmptyIterator(iterator);

        // Try calling next before hasNext
        iterator = new SystemReader(system(), "1");
        iterator.next();
        checkEmptyIterator(iterator);
    }
    */
}
