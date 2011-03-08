// Copyright (c) 2009-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 * Extends {@link IonReader} with capabilites specialized to Ion text data
 * streams.
 */
public interface IonTextReader
    extends IonReader
{
    /**
     * Gets the line number where the parser is currently located.
     */
    public long getLineNumber();

    /**
     * Gets the offset within the current line the parser is currently
     * located.  This may be at the beginning, the
     * end, or in the middle of an item.  The exact location is especially
     * vague if a parsing error has occurred (the location will be at or very
     * near the error but that could be anywhere near the value itself).
     */
    public long getLineOffset();
}
