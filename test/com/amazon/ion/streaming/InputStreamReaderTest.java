// Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.
package com.amazon.ion.streaming;

import static com.amazon.ion.impl.IonImplUtils.EMPTY_BYTE_ARRAY;
import static com.amazon.ion.impl.IonImplUtils.utf8;

import com.amazon.ion.InputStreamWrapper;
import com.amazon.ion.IonType;
import com.amazon.ion.ReaderMaker;
import com.amazon.ion.junit.Injected.Inject;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Test;

/**
 *
 */
public class InputStreamReaderTest
    extends ReaderTestCase
    implements InputStreamWrapper
{
    class CloseWatchingInputStream extends FilterInputStream
    {
        protected CloseWatchingInputStream(InputStream in)
        {
            super(in);
        }

        boolean closed = false;

        @Override
        public void close() throws IOException
        {
            assertFalse("stream already closed", closed);
            closed = true;
            super.close();
        }
    }


    private CloseWatchingInputStream myWatcher;

    public InputStream wrap(InputStream in)
    {
        myWatcher = new CloseWatchingInputStream(in);
        return myWatcher;
    }

    @Inject("readerMaker")
    public static final ReaderMaker[] READER_MAKERS =
        ReaderMaker.valuesWith(ReaderMaker.Feature.STREAM);


    //=========================================================================
    // Test cases

    @Test
    public void testClosingStream()
    throws Exception
    {
        byte[] utf8 = utf8("test");
        read(utf8, this);
        assertSame(IonType.SYMBOL, in.next());
        in.close();
        assertTrue("stream not closed", myWatcher.closed);
    }

    @Test
    public void testClosingEmptyStream()
    throws Exception
    {
        read(EMPTY_BYTE_ARRAY, this);
        assertSame(null, in.next());
        in.close();
        assertTrue("stream not closed", myWatcher.closed);
    }
}
