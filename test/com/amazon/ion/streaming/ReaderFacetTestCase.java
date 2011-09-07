// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import com.amazon.ion.Facets;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.OffsetSpan;
import com.amazon.ion.ReaderMaker;
import com.amazon.ion.SeekableReader;
import com.amazon.ion.Span;
import com.amazon.ion.SpanProvider;
import com.amazon.ion.TextSpan;
import com.amazon.ion.impl.IonReaderOctetPosition;
import com.amazon.ion.junit.IonAssert;
import org.junit.After;

/**
 *
 */
public abstract class ReaderFacetTestCase
    extends IonTestCase
{
    /**
     * These readers don't support the {@link SpanProvider} facet.
     */
    public static final ReaderMaker[] NON_SPAN_READERS =
    {
        ReaderMaker.FROM_INPUT_STREAM_TEXT    // TODO ION-231
    };

    /**
     * These are the readers that don't support {@link TextSpan}s.
     */
    public static final ReaderMaker[] NON_TEXT_SPAN_READERS =
    {
        ReaderMaker.FROM_STRING,               // TODO ION-240
        ReaderMaker.FROM_BYTES_BINARY,
        ReaderMaker.FROM_BYTES_TEXT,           // TODO ION-240
        ReaderMaker.FROM_BYTES_OFFSET_TEXT,    // TODO how does offset affect position?
        ReaderMaker.FROM_BYTES_OFFSET_BINARY,
        ReaderMaker.FROM_INPUT_STREAM_TEXT,    // TODO ION-231
        ReaderMaker.FROM_INPUT_STREAM_BINARY,
        ReaderMaker.FROM_DOM
    };

    /**
     * These readers don't support the {@link SeekableReader} facet.
     */
    public static final ReaderMaker[] NON_SEEKABLE_READERS =
    {
        ReaderMaker.FROM_INPUT_STREAM_BINARY,    // TODO ION-231
        ReaderMaker.FROM_INPUT_STREAM_TEXT
    };


    protected ReaderMaker myReaderMaker;

    public void setReaderMaker(ReaderMaker maker)
    {
        myReaderMaker = maker;
    }


    protected IonReader in;
    protected SpanProvider sp;
    protected boolean mySpanProviderRequired = true;
    protected SeekableReader sr;
    protected boolean mySeekableReaderRequired = true;


    @After @Override
    public void tearDown() throws Exception
    {
        in = null;
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

    protected final void read(byte[] ionData)
    {
        in = myReaderMaker.newReader(system(), ionData);
        initFacets();
    }

    protected final void read(String ionText)
    {
        in = myReaderMaker.newReader(system(), ionText);
        initFacets();
    }


    protected void expectNoFacet(Class<?> facetType, Object subject)
    {
        Object facet = Facets.asFacet(facetType, subject);
        if (facet != null)
        {
            // This idiom gives a helpful message.
            assertEquals("Didn't expect facet " + facetType, null, facet);
        }
    }

    protected void expectNoCurrentValue()
    {
        IonAssert.assertNoCurrentValue(in);
    }

    protected void expectTopLevel()
    {
        IonAssert.assertTopLevel(in);
    }

    protected void expectEof()
    {
        IonAssert.assertEof(in);
    }

    protected void expectTopEof()
    {
        IonAssert.assertTopEof(in);
    }


    protected void hoist(Span s)
    {
        sr.hoist(s);
        expectTopLevel();
        expectNoCurrentValue();
    }

    protected void checkCurrentSpan(long start, long finish)
    {
        OffsetSpan span = sp.currentSpan().asFacet(OffsetSpan.class);
        assertNotNull(span);
        assertEquals("startOffset",  start,  span.getStartOffset());
        assertEquals("finishOffset", finish, span.getFinishOffset());

        // Transitional APIs
        long len = finish - start;

        IonReaderOctetPosition pos = sp.currentSpan().asFacet(IonReaderOctetPosition.class);
        assertNotNull(pos);
        assertEquals(start,  pos.getOffset());
        assertEquals(start,  pos.getStartOffset());
        assertEquals(len,    pos.getLength());
        assertEquals(finish, pos.getFinishOffset());
    }
}
