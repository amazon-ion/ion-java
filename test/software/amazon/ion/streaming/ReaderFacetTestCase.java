/*
 * Copyright 2013-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static software.amazon.ion.facet.Facets.assumeFacet;
import static software.amazon.ion.util.Spans.currentSpan;

import org.junit.After;
import software.amazon.ion.OffsetSpan;
import software.amazon.ion.ReaderMaker;
import software.amazon.ion.SeekableReader;
import software.amazon.ion.Span;
import software.amazon.ion.SpanProvider;
import software.amazon.ion.TextSpan;
import software.amazon.ion.facet.Facets;

public abstract class ReaderFacetTestCase
    extends ReaderTestCase
{
    /**
     * These readers don't support the {@link SpanProvider} facet.
     *
     * @see NonSpanReaderTest
     */
    public static final ReaderMaker[] NON_SPAN_READERS =
    {
    };

    /**
     * These are the readers that don't support {@link OffsetSpan}s.
     */
    public static final ReaderMaker[] NON_OFFSET_SPAN_READERS =
        ReaderMaker.valuesWith(ReaderMaker.Feature.DOM);

    /**
     * These are the readers that don't support {@link TextSpan}s.
     */
    public static final ReaderMaker[] NON_TEXT_SPAN_READERS =
        ReaderMaker.valuesWithout(ReaderMaker.Feature.TEXT);

    /**
     * These readers don't support the {@link SeekableReader} facet.
     *
     * @see NonSeekableReaderTest
     */
    public static final ReaderMaker[] NON_SEEKABLE_READERS =  // TODO amznlabs/ion-java#17
    {
        ReaderMaker.FROM_INPUT_STREAM_BINARY,
        ReaderMaker.FROM_INPUT_STREAM_TEXT,
        ReaderMaker.FROM_READER
    };



    protected SpanProvider sp;
    protected boolean mySpanProviderRequired = true;
    protected SeekableReader sr;
    protected boolean mySeekableReaderRequired = true;


    @After @Override
    public void tearDown() throws Exception
    {
        sp = null;
        sr = null;
        super.tearDown();
    }

    protected void initFacets()
    {
        sp = in.asFacet(SpanProvider.class);
        if (mySpanProviderRequired)
        {
            assertNotNull("SpanProvider not available", sp);
        }

        sr = in.asFacet(SeekableReader.class);
        if (mySeekableReaderRequired)
        {
            assertNotNull("SeekableReader not available", sr);
        }
    }

    @Override
    protected final void read(byte[] ionData)
    {
        super.read(ionData);
        initFacets();
    }

    @Override
    protected final void read(String ionText)
    {
        super.read(ionText);
        initFacets();
    }


    protected void expectNoFacet(Class<?> facetType, Object subject)
    {
        Object facet = Facets.asFacet(facetType, subject);
        if (facet != null)
        {
            fail("Didn't expect facet " + facetType.getSimpleName()
                 + " on " + facet);
        }

        if (facetType.isInstance(subject))
        {
            fail(subject.getClass().getSimpleName()
                 + " shouldn't be instanceof unexpected facet "
                 + facetType.getSimpleName());
        }
    }

    protected void hoist(Span s)
    {
        sr.hoist(s);
        expectTopLevel();
        expectNoCurrentValue();
    }


    protected void checkSpan(int startLine, int startColumn, TextSpan ts)
    {
        assertEquals("startLine",   startLine,   ts.getStartLine());
        assertEquals("startColumn", startColumn, ts.getStartColumn());
        assertEquals("finishLine",   -1, ts.getFinishLine());
        assertEquals("finishColumn", -1, ts.getFinishColumn());
    }


    private void checkSpan(long start, long finish, OffsetSpan offsets)
    {
        assertEquals("startOffset",  start,  offsets.getStartOffset());
        assertEquals("finishOffset", finish, offsets.getFinishOffset());
    }


    protected void checkCurrentSpan(long start, long finish)
    {
        Span span = sp.currentSpan();

        OffsetSpan offsets = assumeFacet(OffsetSpan.class, span);
        checkSpan(start, finish, offsets);

        offsets = currentSpan(OffsetSpan.class, in);
        checkSpan(start, finish, offsets);
    }
}
