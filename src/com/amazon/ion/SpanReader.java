// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 * An {@link IonReader} facet providing the ability to retrieve
 * {@link Span}s (abstract value positions) and seek to positions
 * within the source.
 * <p>
 * A span may be used to seek a different reader instance than the one that
 * generated it, provided that the two readers have the same source.
 * Violations of this constraint may not be detected reliably, so be careful
 * or you'll get unsatisfying results.
 */
public interface SpanReader
{
    /**
     * Gets the current span of this reader, generally covering a single value
     * on the source.
     *
     * @throws IonException if there is no current value. This occurs at the
     * start of the source, immediately after a call to {@link #stepIn()} or
     * {@link #stepOut()}, or when the prior call to {@link #next()} returned
     * null (meaning: end of container or end of stream).
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


    /**
     * Resets this reader to produce the given span as if the values were at
     * top-level.
     * The caller cannot {@link #stepOut} from the span nor continue
     * reading beyond it.
     * <p>
     * After calling this method, this reader's current span will be empty and
     * positioned just before the first value of the given span; the caller
     * must call {@link #next()} to begin reading values.
     * The {@linkplain #getDepth() depth} will be zero.
     * <p>
     * Calls to {@link #getFieldName()} will return null even if the span's
     * original parent was a struct.
     *
     * @throws IonException if the given span is unbalanced.
     */
    public void hoist(Span span);


    /**
     * Resets this reader to produce the given subsequence as if the values
     * were the body of a container.
     * The caller can {@link #stepOut} from the subsequence but cannot
     * continue reading beyond it.
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
    public void stepIn(Span span);


    /**
     * Resets this reader to produce data from the left of the given span,
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
