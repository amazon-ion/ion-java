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

package software.amazon.ion.util;

import static software.amazon.ion.facet.Facets.asFacet;

import software.amazon.ion.Span;
import software.amazon.ion.SpanProvider;

/**
 * Utility methods for working with {@link Span}s.
 *
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
