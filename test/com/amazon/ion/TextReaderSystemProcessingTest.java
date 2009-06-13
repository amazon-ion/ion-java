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
    protected boolean processingBinary()
    {
        return false;
    }

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
    protected void checkMissingSymbol(String expected, int expectedSid)
        throws Exception
    {
        // When reading text and symtab is missing, we'll get the name right
        // but we won't know the right sid.
        checkSymbol(expected);
    }

    /**
     * FIXME JIRA ION-8  remove override to enable test case
     * This is only here to disable this test case
     */
    @Override
    public void testSurrogateGluing()
        throws Exception
    {
        System.err.println("Disabled test case TextReaderSystemProcessingTest.testSurrogateGluing; jira:ION-8");
    }
}
