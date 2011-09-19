// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.util;

import static com.amazon.ion.facet.Facets.asFacet;

import com.amazon.ion.Span;
import com.amazon.ion.SpanProvider;

/**
 * Utility methods for working with {@link Span}s.
 *
 * @since R13
 */
public final class Spans
{
    /**
     * Attempts to get a {@link Span} from the given object, if it
     * supports {@link SpanProvider#currentSpan()}.
     *
     * @param spanProvider may be null.
     * @return null if there's not a current span.
     */
    public static Span currentSpan(Object spanProvider)
    {
        SpanProvider sp = asFacet(SpanProvider.class, spanProvider);
        Span span = (sp == null ? null : sp.currentSpan());
        return span;
    }


    /**
     * Attempts to get a {@link Span} facet from the given object, if it
     * supports {@link SpanProvider#currentSpan()}.
     *
     * @param spanProvider may be null.
     * @return null if there's not a current span supporting
     *  {@code spanFacetType}.
     */
    public static <T> T currentSpan(Class<T> spanFacetType,
                                    Object spanProvider)
    {
        Span span = currentSpan(spanProvider);
        T spanFacet = asFacet(spanFacetType, span);
        return spanFacet;
    }
}
