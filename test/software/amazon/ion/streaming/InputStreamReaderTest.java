/*
 * Copyright 2007-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion.streaming;

import static software.amazon.ion.impl.PrivateUtils.EMPTY_BYTE_ARRAY;
import static software.amazon.ion.impl.PrivateUtils.utf8;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Test;
import software.amazon.ion.InputStreamWrapper;
import software.amazon.ion.IonType;
import software.amazon.ion.ReaderMaker;
import software.amazon.ion.junit.Injected.Inject;

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
