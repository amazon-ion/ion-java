// Copyright (c) 2008-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;



/**
 *
 */
public class TextReaderSystemProcessingTest
    extends ReaderSystemProcessingTestCase
{
    private String myText;

    @Override
    protected void prepare(String text)
    {
        myText = text;
    }

    @Override
    protected IonReader read() throws Exception
    {
        return system().newReader(myText);
    }

    @Override
    protected IonReader systemRead() throws Exception
    {
        return system().newSystemReader(myText);
    }

    @Override
    protected boolean checkMissingSymbol(String expected, int expectedSymbolTableSid, int expectedLocalSid)
        throws Exception
    {
        // When reading text and symtab is missing, we'll get the name right
        // but we won't know the right sid.
        // note that this form of checkSymbol does force a sid
        // to be assigned to this symbol will have an id
        checkSymbol(expected);

        // when missing from a shared table the symbol
        // will have been added to the local symbols
        return true;
    }
}
