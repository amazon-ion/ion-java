// Copyright (c) 2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.Symtabs.FRED_MAX_IDS;
import static com.amazon.ion.Symtabs.LOCAL_SYMBOLS_ABC;
import static com.amazon.ion.Symtabs.makeLocalSymtab;
import static com.amazon.ion.impl._Private_DmsdkUtils.newBinaryWriterWithLocalSymbolTable;
import static com.amazon.ion.impl._Private_Utils.EMPTY_STRING_ARRAY;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.Symtabs;
import java.io.ByteArrayOutputStream;
import org.junit.After;
import org.junit.Test;

/**
 *
 */
public class BinaryWriterWithLocalSymtabsTest
    extends IonTestCase
{
    protected ByteArrayOutputStream myOutputStream;
    protected IonWriter myWriter;

    @Override
    @After
    public void tearDown()
        throws Exception
    {
        myOutputStream = null;
        myWriter = null;
        super.tearDown();
    }

    /**
     * Initializes the {@link #myOutputStream} and assigns a new binary
     * writer bootstrapped with the local symbols defined in
     * {@code localSymtab} to {@link #myWriter}.
     */
    protected void
    makeBinaryWriterWithLocalSymbols(SymbolTable localSymtab)
    {
        myOutputStream = new ByteArrayOutputStream();
        myWriter = newBinaryWriterWithLocalSymbolTable(system(),
                                                       myOutputStream,
                                                       localSymtab);
    }

    protected byte[] outputByteArray() throws Exception
    {
        myWriter.close();
        return myOutputStream.toByteArray();
    }

    private void checkInternedSymbols(SymbolTable localSymtab,
                                      String[] localSymbols)
    {
        for (String localSymbol : localSymbols)
        {
            assertNotNull(localSymtab.find(localSymbol));
        }
    }

    private void
    checkConstructionWithLocalSymbolsAndImports(String[] localSymbols,
                                                SymbolTable... imports)
    {
        SymbolTable localSymtab = makeLocalSymtab(system(), localSymbols,
                                                  imports);
        makeBinaryWriterWithLocalSymbols(localSymtab);
        SymbolTable writerSymtab = myWriter.getSymbolTable();

        assertNotSame(localSymtab, writerSymtab);

        checkInternedSymbols(writerSymtab, localSymbols);

        assertArrayContentsSame(writerSymtab.getImportedTables(), imports);
    }

    //==========================================================================

    @Test(expected=IllegalArgumentException.class)
    public void testConstructionWithSystemSymtab()
        throws Exception
    {
        SymbolTable systemSymtab = system().getSystemSymbolTable();

        makeBinaryWriterWithLocalSymbols(systemSymtab);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructionWithSharedSymtab()
        throws Exception
    {
        SymbolTable fred1 = Symtabs.register("fred", 1, catalog());

        makeBinaryWriterWithLocalSymbols(fred1);
    }

    @Test
    public void testConstructionWithLocalSymbolsAndNoImports()
        throws Exception
    {
        checkConstructionWithLocalSymbolsAndImports(LOCAL_SYMBOLS_ABC);
    }

    @Test
    public void testConstructionWithLocalSymbolsAndOneImports()
        throws Exception
    {
        SymbolTable fred1     = Symtabs.register("fred", 1, catalog());

        checkConstructionWithLocalSymbolsAndImports(LOCAL_SYMBOLS_ABC,
                                                    fred1);
    }

    @Test
    public void testConstructionWithLocalSymbolsAndTwoImports()
        throws Exception
    {
        SymbolTable fred1     = Symtabs.register("fred", 1, catalog());
        SymbolTable ginger1   = Symtabs.register("ginger", 1, catalog());

        checkConstructionWithLocalSymbolsAndImports(LOCAL_SYMBOLS_ABC,
                                                    fred1, ginger1);
    }

    @Test
    public void testConstructionWithNoLocalSymbolsAndNoImports()
        throws Exception
    {
        String[] localSymbols = EMPTY_STRING_ARRAY;

        checkConstructionWithLocalSymbolsAndImports(localSymbols);
    }

    @Test
    public void testConstructionWithNoLocalSymbolsAndOneImports()
        throws Exception
    {
        String[] localSymbols = EMPTY_STRING_ARRAY;
        SymbolTable fred1     = Symtabs.register("fred", 1, catalog());

        checkConstructionWithLocalSymbolsAndImports(localSymbols,
                                                    fred1);
    }

    @Test
    public void testConstructionWithNoLocalSymbolsAndTwoImports()
        throws Exception
    {
        String[] localSymbols = EMPTY_STRING_ARRAY;
        SymbolTable fred1     = Symtabs.register("fred", 1, catalog());
        SymbolTable ginger1   = Symtabs.register("ginger", 1, catalog());

        checkConstructionWithLocalSymbolsAndImports(localSymbols,
                                                    fred1, ginger1);
    }

    @Test
    public void testWritingWithNoImports()
        throws Exception
    {
        final int localSidOffset = systemMaxId();

        SymbolTable localSymtab = makeLocalSymtab(system(), LOCAL_SYMBOLS_ABC);
        makeBinaryWriterWithLocalSymbols(localSymtab);

        // write test data

        myWriter.writeSymbol("a");
        // Builder makes a copy of the symtab
        assertNotSame(localSymtab, myWriter.getSymbolTable());
        localSymtab = myWriter.getSymbolTable();

        myWriter.writeSymbol("not_interned");
        assertSame(localSymtab, myWriter.getSymbolTable());

        myWriter.writeInt(1234);
        assertSame(localSymtab, myWriter.getSymbolTable());

        byte[] bytes = outputByteArray();
        IonDatagram dg = loader().load(bytes);

        // check written bytes

        assertEquals(5, dg.systemSize());

        IonValue val = dg.systemGet(2);
        checkSymbol("a", localSidOffset+1, val);

        val = dg.systemGet(3);
        checkSymbol("not_interned", localSidOffset+4, val);

        val = dg.systemGet(4);
        checkInt(1234, val);
    }

    @Test
    public void testWritingWithImports()
        throws Exception
    {
        final int fredSidOffset  = systemMaxId();
        final int localSidOffset = fredSidOffset + FRED_MAX_IDS[1];

        SymbolTable fred1 = Symtabs.register("fred", 1, catalog());

        SymbolTable localSymtab = makeLocalSymtab(system(),
                                                  LOCAL_SYMBOLS_ABC, fred1);
        makeBinaryWriterWithLocalSymbols(localSymtab);

        // write test data

        myWriter.writeSymbol("a");
        // Builder makes a copy of the symtab
        assertNotSame(localSymtab, myWriter.getSymbolTable());
        localSymtab = myWriter.getSymbolTable();

        myWriter.writeSymbol("not_interned");
        assertSame(localSymtab, myWriter.getSymbolTable());

        myWriter.writeSymbol("fred_1");
        assertSame(localSymtab, myWriter.getSymbolTable());

        myWriter.writeInt(1234);
        assertSame(localSymtab, myWriter.getSymbolTable());

        // check written bytes

        byte[] bytes = outputByteArray();
        IonDatagram dg = loader().load(bytes);

        assertEquals(6, dg.systemSize());

        IonValue val = dg.systemGet(2);
        checkSymbol("a", localSidOffset+1, val);

        val = dg.systemGet(3);
        checkSymbol("not_interned", localSidOffset+4, val);

        val = dg.systemGet(4);
        checkSymbol("fred_1", fredSidOffset+1, val);

        val = dg.systemGet(5);
        checkInt(1234, val);
    }
}
