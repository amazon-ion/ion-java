// Copyright (c) 2009-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;


/**
 * Tests TextReader - BinaryWriter - BinaryReader
 */
public class TrBwBrProcessingTest
    extends BinaryReaderSystemProcessingTest
{
    @Override
    protected void prepare(String text)
        throws Exception
    {
        IonReader textReader = system().newSystemReader(text);
        IonBinaryWriter binaryWriter = system().newBinaryWriter();
        binaryWriter.writeValues(textReader);
        myBytes = binaryWriter.getBytes();
    }
}
