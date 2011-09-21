// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.util;

import static com.amazon.ion.util.Spans.currentSpan;
import static org.junit.Assert.assertNull;

import com.amazon.ion.Span;
import com.amazon.ion.TextSpan;
import org.junit.Test;

public class SpansTest
{
    @Test
    public void testCurrentSpan()
    {
        Span s = currentSpan(null);
        assertNull("span should be null", s);

        s = currentSpan(new Object());
        assertNull("span should be null", s);
    }


    @Test
    public void testCurrentSpanFacet()
    {
        TextSpan s = currentSpan(TextSpan.class, null);
        assertNull("span should be null", s);

        s = currentSpan(TextSpan.class, new Object());
        assertNull("span should be null", s);
    }
}
