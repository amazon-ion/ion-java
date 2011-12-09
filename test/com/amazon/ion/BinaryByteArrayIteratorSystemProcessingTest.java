// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import java.util.Iterator;


public class BinaryByteArrayIteratorSystemProcessingTest
    extends IteratorSystemProcessingTest
{
    private byte[] myBytes;

    @Override
    protected void prepare(String text)
        throws Exception
    {
        myBytes = encode(text);
    }

    @Override
    protected Iterator<IonValue> iterate()
    {
        return system().iterate(myBytes);
    }

    @Override
    protected Iterator<IonValue> systemIterate()
    {
        return system().systemIterate(myBytes);
    }

    @Override
    boolean checkMissingFieldName(String expectedText,
                                  int expectedEncodedSid,
                                  int expectedLocalSid)
        throws Exception
    {
        checkFieldName(null, expectedEncodedSid);

        // when missing from a shared table the symbol
        // will not have been added to the local symbols
        return false;
    }

    @Override
    protected boolean checkMissingSymbol(String expectedText,
                                         int expectedEncodedSid,
                                         int expectedLocalSid)
        throws Exception
    {
        checkMissingSymbol(expectedEncodedSid);

        // when missing from a shared table the symbol
        // will not have been added to the local symbols
        return false;
    }
}
