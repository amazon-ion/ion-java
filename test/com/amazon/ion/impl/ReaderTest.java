/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonValue;
import com.amazon.ion.LocalSymbolTable;
import com.amazon.ion.SymbolTable;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 *
 */
public class ReaderTest
    extends IonTestCase
{
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
}
