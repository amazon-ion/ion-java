// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import static com.amazon.ion.impl.IonImplUtils.intIterator;

import com.amazon.ion.ReaderMaker;
import com.amazon.ion.junit.Injected.Inject;
import com.amazon.ion.junit.IonAssert;
import java.io.IOException;
import java.util.Iterator;
import org.junit.Test;

/**
 *
 */
public class ReaderTest
    extends ReaderTestCase
{
    @Inject("readerMaker")
    public static final ReaderMaker[] READER_MAKERS = ReaderMaker.values();



    @Test // Traps ION-133
    public void testStepOut() throws IOException
    {
        read("{a:{b:1,c:2},d:false}");

        in.next();
        in.stepIn();
        in.next();
        assertEquals("a", in.getFieldName());
        in.stepIn();
        in.next();
        assertEquals("b", in.getFieldName());
        in.stepOut(); // skip c
        expectNoCurrentValue();
        in.next();
        assertEquals("d", in.getFieldName());
        expectEof();
    }


    @Test
    public void testIterateTypeAnnotationIds()
    throws Exception
    {
        if (myReaderMaker.sourceIsText()) return;

        read("ann::ben::null");

        in.next();
        Iterator<Integer> typeIds = in.iterateTypeAnnotationIds();
        IonAssert.assertIteratorEquals(intIterator(10, 11), typeIds);
        expectEof();
    }
}
