/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;


/**
 *
 */
public class TextReaderSystemProcessingTest
    extends ReaderSystemProcessingTestCase
{
    @Override
    protected IonReader read(String text) throws Exception
    {
        return system().newReader(text);
    }

    @Override
    protected IonReader systemRead(String text) throws Exception
    {
        return system().newSystemReader(text);
    }
}
