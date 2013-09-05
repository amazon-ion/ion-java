// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 * Provide the ability to retrieve {@link Span}s (abstract value positions)
 * of Ion data.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 * <p>
 * This functionality may be accessed as a facet of most {@link IonReader}s.
 *
 * @since IonJava R13
 */
public interface SpanProvider
{
    /**
     * Gets the current span of this object, generally covering a single value
     * on the source.
     */
    public Span currentSpan();


    /**
     * Gets a span covering all the children of the current span, which must
     * be a container.
     *
     * @throws IonException if the current span covers anything other than a
     * single container.
     */
//    public Span contentSpan();  // TODO later
    // TODO move to Span interface?


    /**
     * Gets a span covering the container of the current span.
     *
     * @throws IonException if the current span is at top-level.
     */
//    public Span containerSpan();  // TODO later
    // TODO move to Span interface?
}
