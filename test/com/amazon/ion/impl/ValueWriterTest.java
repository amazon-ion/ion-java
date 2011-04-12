// Copyright (c) 2009-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import org.junit.Test;

/**
 *
 */
public class ValueWriterTest
    extends IonWriterTestCase
{
    private IonWriter myWriter;
    private IonDatagram myDatagram;


    @Override
    protected IonWriter makeWriter(SymbolTable... imports)
        throws Exception
    {
        myDatagram = system().newDatagram(imports);
        myWriter = system().newWriter(myDatagram);
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



    @Override @Test
    public void testWritingBadSurrogates()
    {
        logSkippedTest();
    }
}
