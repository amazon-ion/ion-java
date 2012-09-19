// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import com.amazon.ion.facet.Faceted;
import com.amazon.ion.util.Spans;

/**
 * Exposes the positions of a {@link Span} in the form of <em>one-based</em>
 * line and column numbers within the source text stream.
 * <p>
 * As with all spans, positions lie <em>between</em> values, and when the start
 * and finish positions are equal, the span is said to be <em>empty</em>.
 * <p>
 * To get one of these from a {@link Span}, use
 * {@link Faceted#asFacet(Class) asFacet}{@code (TextSpan.class)} or one of
 * the helpers from {@link Spans}.
 *
 * @since IonJava R13
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
     * In most cases, the finish position is implicit and this method returns
     * {@code -1}.  That's since in general (notably for containers) the
     * finish offset can't be determined without significant effort to parse
     * to the end of the value.
     */
    public long getFinishLine();

    /**
     * Returns the column number of this span's finish position, counting from
     * one.
     * In most cases, the finish position is implicit and this method returns
     * {@code -1}.  That's since in general (notably for containers) the
     * finish offset can't be determined without significant effort to parse
     * to the end of the value.
     */
    public long getFinishColumn();
}
