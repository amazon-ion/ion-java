// Copyright (c) 2009-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonBinaryWriter;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;

/**
 *
 */
@SuppressWarnings("deprecation")
public class OldBinaryWriterTest
    extends IonWriterTestCase
{
    private IonBinaryWriter myWriter;


    @Override
    protected IonWriter makeWriter(SymbolTable... imports)
        throws Exception
    {
        myWriter = system().newBinaryWriter(imports);
        return myWriter;
    }

    @Override
    protected byte[] outputByteArray()
        throws Exception
    {
        return myWriter.getBytes();
    }

    @Override
    protected void checkClosed()
    {
        // Nothing to do.
    }
}
