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
 * An {@link IonReader} facet providing the ability to retrieve
 * {@link Span}s (abstract value positions) and seek to positions
 * within the source.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 * <p>
 * A span may be used to seek a different reader instance than the one that
 * generated it, provided that the two readers have the same source.
 * Violations of this constraint may not be detected reliably, so be careful
 * or you'll get unsatisfying results.
 *
 */
public interface SeekableReader
    extends SpanProvider
{
    /**
     * Seeks this reader to produce the given span as if its values were at
     * top-level.
     * The caller cannot {@link IonReader#stepOut stepOut} from the span nor
     * continue reading beyond it.
     * <p>
     * After calling this method, this reader's current span will be empty and
     * positioned just before the first value of the given span; the caller
     * must call {@link IonReader#next() next()} to begin reading values.
     * At the end of the span, the reader will behave as if it's at EOF
     * regardless whether the source has more data beyond the span.
     * <p>
     * Hoisting makes the span's values appear to be at top-level even if they
     * have containers in the source.
     * The {@linkplain IonReader#getDepth() depth} will be zero, and
     * calls to {@link IonReader#getFieldName() getFieldName()} will return
     * null even if the span's original parent was a struct.
     *
     * @throws IonException if the given span is unbalanced.
     */
    public void hoist(Span span);


    /**
     * Seeks this reader to produce the given subsequence as if the values
     * were the body of a container.
     * The caller can {@link IonReader#stepOut stepOut} from the subsequence
     * but cannot continue reading beyond it.
     * <p>
     * After calling this method, this reader's current span will be empty and
     * positioned just before the first value of the given span; the caller
     * must call {@link #next()} to begin reading values.
     * The {@linkplain #getDepth() depth} will be one.
     * <p>
     * If the original parent was a struct, then this reader will implement
     * {@link #isInStruct()} and {@link #getFieldName()}.
     *
     * @throws IonException if the given span is unbalanced or at top-level.
     */
//  public void stepIn(Span span);


    /**
     * Seeks this reader to produce data from the left of the given span,
     * continuing "up and out" from the current container to the end of the
     * source.
     * <p>
     * After calling this method, the reader's current span will be empty and
     * positioned just before the first value of the given span; the caller
     * must call {@link #next()} to begin reading values.
     * The {@linkplain #getDepth() depth} will be the same as in the source.
     * <p>
     */
//    public void start(Span span);  // TODO later
}
