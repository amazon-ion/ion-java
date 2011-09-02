// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import java.io.InputStream;

/**
 * Exposes the positions of a {@link Span} in the form of zero-based offsets
 * within the source.  The "unit of measure" the offsets count depends on the
 * source type: for byte arrays or {@link InputStream}s, the offsets count
 * octets, but for {@link String}s or {@link java.io.Reader}s the offsets count
 * UTF-16 code units.
 * <p>
 * As with all spans, positions lie <em>between</em> values, and when the start
 * and finish positions are equal, the span is said to be <em>empty</em>.
 * <p>
 * To get one of these from a {@link Span}, use
 * {@link Faceted#asFacet(Class) asFacet}{@code (OffsetSpan.class)} or one of
 * the helpers from {@link Facets}.
 */
public interface OffsetSpan
{
    /**
     * Returns this span's start position as a zero-based offset within the
     * source.
     */
    public long getStartOffset();

    /**
     * Returns this span's finish position as a zero-based offset within the
     * source.
     */
    public long getFinishOffset();
}
