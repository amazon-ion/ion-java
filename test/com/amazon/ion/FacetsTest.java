// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.Facets.assumeFacet;
import static org.junit.Assert.assertSame;

import org.junit.Test;

/**
 *
 */
public class FacetsTest
{
    final private Integer i = 13;

    final private Faceted o = new Faceted()
    {
        public <T> T asFacet(Class<T> facetType)
        {
            if (facetType == Number.class) return facetType.cast(i);
            return null;
        }
    };


    @Test(expected = IonException.class)
    public void assumeFacetThrowsOnNullObject()
    {
        @SuppressWarnings("unused") // We want this idiom to work
        Span n = assumeFacet(Span.class, null);
    }

    @Test(expected = IonException.class)
    public void assumeFacetThrowsOnNoFacet()
    {
        Span s = o.asFacet(Span.class);
        assertSame(null, s);
        s = assumeFacet(Span.class, o);
    }

    @Test
    public void assumeFacetSuccess()
    {
        Number n = o.asFacet(Number.class);
        assertSame(i, n);
        n = assumeFacet(Number.class, o);
        assertSame(i, n);
    }
}
