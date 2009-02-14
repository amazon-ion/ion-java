// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import java.io.ByteArrayOutputStream;

/**
 *
 */
public class TextWriterTest
    extends WriterTestCase
{
    private ByteArrayOutputStream myOutputStream;


    @Override
    protected IonWriter makeWriter(SymbolTable... imports)
        throws Exception
    {
        myOutputStream = new ByteArrayOutputStream();
        return system().newTextWriter(myOutputStream, imports);
    }

    @Override
    protected byte[] outputByteArray()
        throws Exception
    {
        myOutputStream.close();
        byte[] utf8Bytes = myOutputStream.toByteArray();

//        String ionText = new String(utf8Bytes, "UTF-8"); // for debugging

        return utf8Bytes;
    }
}
