// Copyright (c) 2008-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import org.junit.Assert;



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
    protected void checkAnnotations(String[] expecteds, int[] expectedSids)
    {
        String[] typeAnnotations = myReader.getTypeAnnotations();
        Assert.assertArrayEquals(expecteds, typeAnnotations);

        // TODO ION-172
//        int[] sids = myReader.getTypeAnnotationIds();
//        Assert.assertArrayEquals(expectedSids, sids);
    }
}
