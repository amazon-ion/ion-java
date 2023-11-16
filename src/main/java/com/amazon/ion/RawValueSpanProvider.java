/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion;

/**
 * Provide the ability to retrieve {@link Span}s (abstract value positions)
 * of raw Ion values, excluding type and length octets.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 * <p>
 * This functionality may be accessed as a facet of binary {@link IonReader}s.
 *
 * @deprecated This is a private API subject to change without notice.
 */
@Deprecated
public interface RawValueSpanProvider
{

    /**
     * Constructs a Span, which may be faceted as an {@link OffsetSpan}, that
     * provides the start and end byte positions of the current value.
     * <p>
     * NOTE: for Ion {@code int} values, users should not use these byte positions to
     * determine which primitive (if any) the value can fit into, because the
     * sign of Ion {@code int} values is encoded into the type ID byte, which is
     * not included in this span.
     * <p>
     * <b>WARNING:</b> Spans provided by this method are not compatible with the
     * {@link SeekableReader} facet, because they lack the type ID and length
     * bytes that are important when reconstructing the context on seek. For
     * {@link SpanProvider#currentSpan()} should be used to retrieve seekable
     * Spans.
     * @return the constructed Span
     */
    public Span valueSpan();

    /**
     * @return the byte[] that backs this span. This span's start and end positions
     * may be used as indices into the returned buffer. NOTE: does NOT perform
     * a copy of the buffer; care must be taken not to mutate and corrupt the
     * data.
     */
    public byte[] buffer();

}
