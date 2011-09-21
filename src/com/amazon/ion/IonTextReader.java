// Copyright (c) 2009-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import com.amazon.ion.util.Spans;

/**
 * Extends {@link IonReader} with capabilites specialized to Ion text data
 * streams.
 *
 * @deprecated Since IonJava R13.  Use {@link TextSpan} instead.
 *
 * @see SpanProvider
 * @see TextSpan
 * @see Spans#currentSpan(Class, Object)
 */
@Deprecated
public interface IonTextReader
    extends IonReader
{
    /**
     * Gets the line number where the parser is currently located.
     *
     * @deprecated Since IonJava R13.
     *  Use {@link TextSpan#getStartLine()} instead.
     */
    @Deprecated
    public long getLineNumber();

    /**
     * Gets the offset within the current line the parser is currently
     * located.  This may be at the beginning, the
     * end, or in the middle of an item.  The exact location is especially
     * vague if a parsing error has occurred (the location will be at or very
     * near the error but that could be anywhere near the value itself).
     *
     * @deprecated Since IonJava R13.
     *  Use {@link TextSpan#getStartColumn()} instead.
     */
    @Deprecated
    public long getLineOffset();
}
