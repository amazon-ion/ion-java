// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 * Exposes the positions of a {@link Span} in the form of <em>one-based</em>
 * line and column numbers within the source text stream.
 * <p>
 * As with all spans, positions lie <em>between</em> values, and when the start
 * and finish positions are equal, the span is said to be <em>empty</em>.
 * <p>
 * To get one of these from a {@link Span}, use
 * {@link Faceted#asFacet(Class) asFacet}{@code (TextSpan.class)} or one of
 * the helpers from {@link Facets}.
 */
public interface TextSpan
{
    /**
     * Returns the line number of this span's start position, counting from
     * one.
     */
    public long getStartLine();

    /**
     * Returns the column number of this span's start position, counting from
     * one.
     */
    public long getStartColumn();


    /**
     * Returns the line number of this span's finish position, counting from
     * one.
     */
    public long getFinishLine();

    /**
     * Returns the column number of this span's finish position, counting from
     * one.
     */
    public long getFinishColumn();
}
