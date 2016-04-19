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

/**
 * Provide the ability to retrieve {@link Span}s (abstract value positions)
 * of Ion data.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 * <p>
 * This functionality may be accessed as a facet of most {@link IonReader}s.
 *
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
