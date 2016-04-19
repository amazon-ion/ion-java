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
 * An immutable reference to a consecutive sequence of values (perhaps
 * including large hierarchies) within some base source of Ion data.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 * <p>
 * A span is conceptually comprised of two abstract <em>positions</em> or
 * offsets within the base source.  The <em>start</em> position denotes the
 * leftmost edge of the span, and the <em>finish</em> position denotes the
 * rightmost edge.
 * Positions lie <em>between</em> values, and when the start and finish
 * positions are equal, the span is said to be <em>empty</em>.
 * <p>
 * A span is said to <em>cover</em> the values that lie within its edges.
 * <p>
 * A span is <em>balanced</em> if it starts and finishes within the same
 * container, otherwise it is <em>unbalanced</em>.  This library currently
 * does not support unbalanced spans.
 * <p>
 * Since different source types require different positioning techniques,
 * spans is {@link Faceted} to expose the position implementation.
 *
 * <h2>Acknowledgements</h2>
 * This design and terminology is heavily based on Wilfred J. Hansen's work on
 * subsequence references.
 *
 * @see SpanProvider
 * @see Spans
 * @see TextSpan
 * @see OffsetSpan
 *
 * @see <a href="http://portal.acm.org/citation.cfm?id=133234">Subsequence
 * References: First-Class Values for Substrings</a>
 *
 */
public interface Span
    extends Faceted
{
    /**
     * Gets a span covering the container of this span.
     *
     * @throws IonException if the current span is at top-level.
     */
//    public Span containerSpan();  // TODO later.  Move to a facet?


    /**
     * Gets a span covering all the children of this span, which must cover a
     * single container.
     *
     * @throws IonException if this span covers anything other than a
     * single container.
     */
//    public Span contentSpan();  // TODO later.  Move to a facet?
}
