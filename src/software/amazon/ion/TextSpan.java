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

package software.amazon.ion;

import software.amazon.ion.facet.Faceted;
import software.amazon.ion.util.Spans;

/**
 * Exposes the positions of a {@link Span} in the form of <em>one-based</em>
 * line and column numbers within the source text stream.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 * <p>
 * As with all spans, positions lie <em>between</em> values, and when the start
 * and finish positions are equal, the span is said to be <em>empty</em>.
 * <p>
 * To get one of these from a {@link Span}, use
 * {@link Faceted#asFacet(Class) asFacet}{@code (TextSpan.class)} or one of
 * the helpers from {@link Spans}.
 *
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
