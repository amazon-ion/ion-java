// Copyright (c) 2009-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonContainer;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonList;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.Symtabs;
import org.junit.Test;

/**
 *
 */
public class ValueWriterTest
    extends IonWriterTestCase
{
    private IonWriter myWriter;
    private IonDatagram myDatagram;
    private boolean myOutputInList;


    @Override
    protected IonWriter makeWriter(SymbolTable... imports)
        throws Exception
    {
        myDatagram = system().newDatagram(imports);

        IonContainer c =
            (myOutputInList ? myDatagram.add().newEmptyList() : myDatagram);

        myWriter = system().newWriter(c);
        return myWriter;
    }

    @Override
    protected byte[] outputByteArray()
        throws Exception
    {
        return myDatagram.getBytes();
    }

    @Override
    protected void checkClosed()
    {
        // Nothing to do.
    }

    //=========================================================================

    @Override
    public void testBadString(String text)
        throws Exception
    {
        // Tree writer accepts bad text, but it's caught at output time.
        iw = makeWriter();
        iw.writeString(text);
        iw.finish();

        try
        {
            myDatagram.toString();
            fail("expected exception");
        }
        catch (IllegalArgumentException e) { }
    }

    @Override
    public void testBadSymbol(String text)
        throws Exception
    {
        // Tree writer accepts bad text, but it's caught at output time.
        iw = makeWriter();
        iw.writeSymbol(text);
        iw.finish();

        try
        {
            myDatagram.toString();
            fail("expected exception");
        }
        catch (IllegalArgumentException e) { }
    }

    // TODO more thorough testing that you can't output from DOM w/ bad text


    @Test
    public void testWriteValuesWithSymtabIntoContainer()
        throws Exception
    {
        myOutputInList = true;

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
        iw.writeValues(r);

        IonDatagram result = reload();
        IonList l = (IonList) result.get(0);
        assertEquals(1, l.size());

        IonSymbol s = (IonSymbol) l.get(0);
        assertEquals(gingerSym, s.stringValue());
        // Should've assigned a new SID
        assertEquals(systemMaxId() + Symtabs.FRED_MAX_IDS[1] + 1,
                     s.getSymbolId());
    }



    @Override @Test
    public void testWriteIVMImplicitly()
        throws Exception
    {
        super.testWriteIVMImplicitly();

        // TODO ION-261
        // assertEquals(2, myDatagram.size());
    }


    @Override @Test
    public void testWriteIVMExplicitly()
        throws Exception
    {
        super.testWriteIVMExplicitly();
        // TODO ION-261
        //assertEquals(2, myDatagram.size());
    }
}
