// Copyright (c) 2008-2011 Amazon.com, Inc.  All rights reserved.

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
}
