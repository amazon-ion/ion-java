/*
 * Copyright 2011-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import org.junit.Test;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonType;
import software.amazon.ion.ReaderMaker;
import software.amazon.ion.Span;
import software.amazon.ion.TestUtils;
import software.amazon.ion.junit.Injected.Inject;

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
        read("foo");
        sp.currentSpan();
    }


    private void callCurrentSpanBeforeFirstChild(String ionText)
    {
        read(ionText);
        in.next();
        in.stepIn();
        sp.currentSpan();
    }

    @Test(expected=IllegalStateException.class)
    public void testCurrentSpanBeforeFirstListChild()
    {
        callCurrentSpanBeforeFirstChild("[v]");
    }

    @Test(expected=IllegalStateException.class)
    public void testCurrentSpanBeforeFirstSexpChild()
    {
        callCurrentSpanBeforeFirstChild("(v)");
    }

    @Test(expected=IllegalStateException.class)
    public void testCurrentSpanBeforeFirstStructChild()
    {
        callCurrentSpanBeforeFirstChild("{f:v}");
    }


    private void callCurrentSpanAfterLastChild(String ionText)
    {
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
        callCurrentSpanAfterLastChild("[v]");
    }

    @Test(expected=IllegalStateException.class)
    public void testCurrentSpanAfterLastSexpChild()
    {
        callCurrentSpanAfterLastChild("(v)");
    }

    @Test(expected=IllegalStateException.class)
    public void testCurrentSpanAfterLastStructChild()
    {
        callCurrentSpanAfterLastChild("{f:v}");
    }


    @Test(expected=IllegalStateException.class)
    public void testCurrentSpanAtEndOfStream()
    {
        read("foo");
        in.next();
        assertEquals(null, in.next());
        sp.currentSpan();
    }
}
