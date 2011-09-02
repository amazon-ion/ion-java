// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 * Exposes the edges of a {@link Span} in the form of zero-based octet offsets
 * within the source stream.
 * <p>
 * As with all spans, positions lie <em>between</em> values, and when the start
 * and finish positions are equal, the span is said to be <em>empty</em>.
 */
public interface OffsetSpan
{
    /**
     * Returns this span's start position as an octet offset within the source
     * byte stream.
     */
    public long getStartOffset();

    /**
     * Returns this span's finish position as an octet offset within the source
     * byte stream.
     */
    public long getFinishOffset();
}
