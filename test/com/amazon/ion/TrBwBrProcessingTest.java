// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

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
        IonReader textReader = system().newReader(text);
        IonBinaryWriter binaryWriter = system().newBinaryWriter();
        binaryWriter.writeValues(textReader);
        myBytes = binaryWriter.getBytes();
    }

    @Override
    public void testLocalTableReplacement() throws Exception
    {
        // TODO fix disabled test case
        System.err.println("WARNING: skipped "
                           + com.amazon.ion.TrBwBrProcessingTest.class
                           + ".testLocalTableReplacement()");
    }

    @Override
    public void testLocalTableResetting() throws Exception
    {
        // TODO fix disabled test case
        System.err.println("WARNING: skipped "
                           + com.amazon.ion.TrBwBrProcessingTest.class
                           + ".testLocalTableResetting()");
    }

    @Override
    public void testLocalTableWithGreaterImport() throws Exception
    {
        // TODO fix disabled test case
        System.err.println("WARNING: skipped "
                           + com.amazon.ion.TrBwBrProcessingTest.class
                           + ".testLocalTableWithGreaterImport()");
    }

    @Override
    public void testLocalTableWithLesserImport() throws Exception
    {
        // TODO fix disabled test case
        System.err.println("WARNING: skipped "
                           + com.amazon.ion.TrBwBrProcessingTest.class
                           + ".testLocalTableWithLesserImport()");
    }

    @Override
    public void testTrivialLocalTableReplacement() throws Exception
    {
        // TODO fix disabled test case
        System.err.println("WARNING: skipped "
                           + com.amazon.ion.TrBwBrProcessingTest.class
                           + ".testTrivialLocalTableReplacement()");
    }
}
