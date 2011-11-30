/*
 * Copyright (c) 2008-2009 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;


/**
 *
 */
public class BinaryReaderSystemProcessingTest
    extends ReaderSystemProcessingTestCase
{
    protected byte[] myBytes;

    @Override
    protected void prepare(String text)
        throws Exception
    {
        IonLoader loader = loader();
        IonDatagram datagram = loader.load(text);
        myBytes = datagram.getBytes();
    }

    @Override
    public IonReader read() throws Exception
    {
        return system().newReader(myBytes);
    }

    @Override
    public IonReader systemRead() throws Exception
    {
        return system().newSystemReader(myBytes);
    }

    @Override
    protected boolean checkMissingSymbol(String expected,
                                         int expectedSymbolTableSid,
                                         int expectedLocalSid)
        throws Exception
    {
        assertSame(IonType.SYMBOL, myReader.getType());

        InternedSymbol sym = myReader.symbolValue();
        assertEquals(null, sym.getText());
        assertEquals(expectedSymbolTableSid, sym.getId());

        assertEquals("$" + expectedSymbolTableSid, myReader.stringValue());
        assertEquals(expectedSymbolTableSid, myReader.getSymbolId());

        // when missing from a shared table the symbol
        // will not have been added to the local symbols
        return false;
    }
}
