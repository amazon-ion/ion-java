/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion.streaming;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonType;
import com.amazon.ion.ReaderMaker;
import com.amazon.ion.Span;
import com.amazon.ion.TestUtils;
import com.amazon.ion.junit.Injected.Inject;
import org.junit.Test;

/**
 * @see NonSpanReaderTest
 */
public class SpanReaderTest
    extends ReaderFacetTestCase
{
    @Inject("readerMaker")
    public static final ReaderMaker[] READER_MAKERS =
        ReaderMaker.valuesExcluding(NON_SPAN_READERS);


    public SpanReaderTest()
    {
        mySeekableReaderRequired = false;
    }


    @Test
    public void testCallingCurrentSpan()
    {
        if (getStreamingMode() == StreamingMode.NEW_STREAMING_INCREMENTAL) {
            // TODO the incremental reader does not currently support the SpanProvider or SeekableReader facets.
            //      See ion-java/issues/382 and ion-java/issues/383.
            return;
        }
        String text =
            "null true 3 4e0 5.0 6666-06-06T '7' \"8\" {{\"\"}} {{}} [] () {}";

        IonDatagram dg = loader().load(text);

        Span[] positions = new Span[dg.size()];

        read(text);
        for (int i = 0; i < dg.size(); i++)
        {
            assertEquals(dg.get(i).getType(), in.next());
            positions[i] = sp.currentSpan();
        }
        expectEof();


        // Collect spans *after* extracting scalar body.

        read(text);
        for (int i = 0; i < dg.size(); i++)
        {
            IonType t =  in.next();
            assertEquals(dg.get(i).getType(),t);
            if (! IonType.isContainer(t))
            {
                TestUtils.consumeCurrentValue(in);
            }

//            assertEquals(positions[i], sp.currentSpan());  //FIXME
        }
        expectTopEof();
    }


    @Test
    public void testCurrentSpanWithinContainers()
    {
        if (getStreamingMode() == StreamingMode.NEW_STREAMING_INCREMENTAL) {
            // TODO the incremental reader does not currently support the SpanProvider or SeekableReader facets.
            //      See ion-java/issues/382 and ion-java/issues/383.
            return;
        }
        read("{f:v,g:[c]} s");

        in.next();
        in.stepIn();
            in.next();
            Span fPos = sp.currentSpan();
            assertEquals("v", in.stringValue());
            in.next();
            Span gPos = sp.currentSpan();
            in.stepIn();
                in.next();
                assertEquals("c", in.stringValue());
                Span cPos = sp.currentSpan();
                expectEof();
            in.stepOut();
            expectEof();
        in.stepOut();
        in.next();
        Span sPos = sp.currentSpan();
        expectTopEof();
    }



    //========================================================================
    // Failure cases

    @Test(expected=IllegalStateException.class)
    public void testCurrentSpanBeforeFirstTopLevel()
    {
        if (getStreamingMode() == StreamingMode.NEW_STREAMING_INCREMENTAL) {
            // TODO the incremental reader does not currently support the SpanProvider or SeekableReader facets.
            //      See ion-java/issues/382 and ion-java/issues/383.
            throw new IllegalStateException();
        }
        read("foo");
        sp.currentSpan();
    }


    private void callCurrentSpanBeforeFirstChild(String ionText)
    {
        if (getStreamingMode() == StreamingMode.NEW_STREAMING_INCREMENTAL) {
            // TODO the incremental reader does not currently support the SpanProvider or SeekableReader facets.
            //      See ion-java/issues/382 and ion-java/issues/383.
            return;
        }
        read(ionText);
        in.next();
        in.stepIn();
        sp.currentSpan();
    }

    @Test(expected=IllegalStateException.class)
    public void testCurrentSpanBeforeFirstListChild()
    {
        if (getStreamingMode() == StreamingMode.NEW_STREAMING_INCREMENTAL) {
            // TODO the incremental reader does not currently support the SpanProvider or SeekableReader facets.
            //      See ion-java/issues/382 and ion-java/issues/383.
            throw new IllegalStateException();
        }
        callCurrentSpanBeforeFirstChild("[v]");
    }

    @Test(expected=IllegalStateException.class)
    public void testCurrentSpanBeforeFirstSexpChild()
    {
        if (getStreamingMode() == StreamingMode.NEW_STREAMING_INCREMENTAL) {
            // TODO the incremental reader does not currently support the SpanProvider or SeekableReader facets.
            //      See ion-java/issues/382 and ion-java/issues/383.
            throw new IllegalStateException();
        }
        callCurrentSpanBeforeFirstChild("(v)");
    }

    @Test(expected=IllegalStateException.class)
    public void testCurrentSpanBeforeFirstStructChild()
    {
        if (getStreamingMode() == StreamingMode.NEW_STREAMING_INCREMENTAL) {
            // TODO the incremental reader does not currently support the SpanProvider or SeekableReader facets.
            //      See ion-java/issues/382 and ion-java/issues/383.
            throw new IllegalStateException();
        }
        callCurrentSpanBeforeFirstChild("{f:v}");
    }


    private void callCurrentSpanAfterLastChild(String ionText)
    {
        if (getStreamingMode() == StreamingMode.NEW_STREAMING_INCREMENTAL) {
            // TODO the incremental reader does not currently support the SpanProvider or SeekableReader facets.
            //      See ion-java/issues/382 and ion-java/issues/383.
            return;
        }
        read(ionText);
        in.next();
        in.stepIn();
        in.next();
        in.next();
        sp.currentSpan();
    }

    @Test(expected=IllegalStateException.class)
    public void testCurrentSpanAfterLastListChild()
    {
        if (getStreamingMode() == StreamingMode.NEW_STREAMING_INCREMENTAL) {
            // TODO the incremental reader does not currently support the SpanProvider or SeekableReader facets.
            //      See ion-java/issues/382 and ion-java/issues/383.
            throw new IllegalStateException();
        }
        callCurrentSpanAfterLastChild("[v]");
    }

    @Test(expected=IllegalStateException.class)
    public void testCurrentSpanAfterLastSexpChild()
    {
        if (getStreamingMode() == StreamingMode.NEW_STREAMING_INCREMENTAL) {
            // TODO the incremental reader does not currently support the SpanProvider or SeekableReader facets.
            //      See ion-java/issues/382 and ion-java/issues/383.
            throw new IllegalStateException();
        }
        callCurrentSpanAfterLastChild("(v)");
    }

    @Test(expected=IllegalStateException.class)
    public void testCurrentSpanAfterLastStructChild()
    {
        if (getStreamingMode() == StreamingMode.NEW_STREAMING_INCREMENTAL) {
            // TODO the incremental reader does not currently support the SpanProvider or SeekableReader facets.
            //      See ion-java/issues/382 and ion-java/issues/383.
            throw new IllegalStateException();
        }
        callCurrentSpanAfterLastChild("{f:v}");
    }


    @Test(expected=IllegalStateException.class)
    public void testCurrentSpanAtEndOfStream()
    {
        if (getStreamingMode() == StreamingMode.NEW_STREAMING_INCREMENTAL) {
            // TODO the incremental reader does not currently support the SpanProvider or SeekableReader facets.
            //      See ion-java/issues/382 and ion-java/issues/383.
            throw new IllegalStateException();
        }
        read("foo");
        in.next();
        assertEquals(null, in.next());
        sp.currentSpan();
    }
}
