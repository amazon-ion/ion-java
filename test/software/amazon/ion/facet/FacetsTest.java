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

package software.amazon.ion.facet;

import static org.junit.Assert.assertSame;
import static software.amazon.ion.facet.Facets.asFacet;
import static software.amazon.ion.facet.Facets.assumeFacet;

import org.junit.Test;
import software.amazon.ion.Span;
import software.amazon.ion.facet.Faceted;
import software.amazon.ion.facet.UnsupportedFacetException;

public class FacetsTest
{
    private static final Integer i = 13;

    private static class Unrelated { }

    private static class Base { }

    private static class Mid extends Base implements Span
    {
        public <T> T asFacet(Class<T> facetType)
        {
            if (facetType == Number.class) return facetType.cast(i);
            return null;
        }
    }

    private static class Bottom extends Mid { }

    private final Faceted o = new Mid();


    @Test
    public void testAsFacetOfFaceted()
    {
        Faceted subject = null;
        assertSame(null, asFacet(Span.class,   subject));

        subject = new Mid();
        assertSame(null, asFacet(Object.class,    subject));
        assertSame(null, asFacet(Unrelated.class, subject));
        assertSame(null, asFacet(Faceted.class,   subject));
        assertSame(null, asFacet(Base.class,      subject));
        assertSame(null, asFacet(Span.class,      subject));
        assertSame(i,    asFacet(Number.class,    subject));

        subject = new Bottom();
        assertSame(null, asFacet(Object.class,    subject));
        assertSame(null, asFacet(Unrelated.class, subject));
        assertSame(null, asFacet(Faceted.class,   subject));
        assertSame(null, asFacet(Base.class,      subject));
        assertSame(null, asFacet(Span.class,      subject));
        assertSame(i,    asFacet(Number.class,    subject));
    }


    @Test
    public void testAsFacetOfObject()
    {
        Object subject = null;
        assertSame(null, asFacet(Span.class,   subject));

        subject = new Base();
        assertSame(null,    asFacet(Unrelated.class, subject));
        assertSame(subject, asFacet(Object.class,    subject)); // Cast
        assertSame(subject, asFacet(Base.class,      subject)); // Cast

        subject = new Mid();
        assertSame(null, asFacet(Object.class,    subject));
        assertSame(null, asFacet(Unrelated.class, subject));
        assertSame(null, asFacet(Faceted.class,   subject));
        assertSame(null, asFacet(Base.class,      subject));
        assertSame(null, asFacet(Span.class,      subject));
        assertSame(i,    asFacet(Number.class,    subject));

        subject = new Bottom();
        assertSame(null, asFacet(Object.class,    subject));
        assertSame(null, asFacet(Unrelated.class, subject));
        assertSame(null, asFacet(Faceted.class,   subject));
        assertSame(null, asFacet(Base.class,      subject));
        assertSame(null, asFacet(Span.class,      subject));
        assertSame(i,    asFacet(Number.class,    subject));
    }


    @Test(expected = UnsupportedFacetException.class)
    public void assumeFacetThrowsOnNullFacted()
    {
        Faceted subject = null;
        @SuppressWarnings("unused") // We want this assignment idiom to work
        Span n = assumeFacet(Span.class, subject);
    }

    @Test(expected = UnsupportedFacetException.class)
    public void assumeFacetThrowsOnNullObject()
    {
        Object subject = null;
        @SuppressWarnings("unused") // We want this assignment idiom to work
        Span n = assumeFacet(Span.class, subject);
    }


    @Test(expected = UnsupportedFacetException.class)
    public void assumeFacetOfFacetedThrowsOnNoFacet()
    {
        Faceted subject = new Mid();
        assumeFacet(Span.class, subject);
    }

    @Test(expected = UnsupportedFacetException.class)
    public void assumeFacetOfFacetedObjectThrowsOnNoFacet()
    {
        Object subject = new Mid();
        assumeFacet(Span.class, subject);
    }

    @Test(expected = UnsupportedFacetException.class)
    public void assumeFacetOfUnfacetedObjectThrowsOnNoFacet()
    {
        Object subject = new Base();
        assumeFacet(Unrelated.class, subject);
    }

    @Test
    public void assumeFacetOfFacetedSuccess()
    {
        Number n = o.asFacet(Number.class);
        assertSame(i, n);
        n = assumeFacet(Number.class, o);
        assertSame(i, n);
    }

    @Test
    public void assumeFacetOfFacetedObjectSuccess()
    {
        Object subject = new Mid();
        assertSame(i, assumeFacet(Number.class, subject));

        subject = new Base();
        assertSame(subject, assumeFacet(Base.class, subject));
    }
}
