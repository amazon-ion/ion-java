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

import java.io.InputStream;
import software.amazon.ion.facet.Faceted;
import software.amazon.ion.facet.Facets;

/**
 * Exposes the positions of a {@link Span} in the form of zero-based offsets
 * within the source.  The "unit of measure" the offsets count depends on the
 * source type: for byte arrays or {@link InputStream}s, the offsets count
 * octets, but for {@link String}s or {@link java.io.Reader}s the offsets count
 * UTF-16 code units.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 * <p>
 * As with all spans, positions lie <em>between</em> values, and when the start
 * and finish positions are equal, the span is said to be <em>empty</em>.
 * <p>
 * To get one of these from a {@link Span}, use
 * {@link Faceted#asFacet(Class) asFacet}{@code (OffsetSpan.class)} or one of
 * the helpers from {@link Facets}.
 *
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
     * source.  In some cases, the finish position is implicit and this method
     * returns {@code -1}.  This includes most text sources, since in
     * general (notably for containers) the finish offset can't be determined
     * without significant effort to parse to the end of the value.
     */
    public long getFinishOffset();
}
