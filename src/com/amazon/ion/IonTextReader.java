// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 *   IonTextReader extends IonReader to allow access to the
 *   text sources line number and offset.  This is especially
 *   valuable for reporting parsing errors.
 */
public interface IonTextReader
    extends IonReader
{
    /**
     * returns the line number where the parser is currently located for text
     * input sources or the top level value index (1 based) for binary sources.
     */
    public long getLineNumber();

    /**
     * returns the offset with in the current line the parser is currently
     * located on for text input sources or the offset within the current
     * top level value for binary sources.  This may be at the beginning, the
     * end, or in the middle of an item.  The exact location is especially
     * vague if a parsing error has occurred (the location will be at or very
     * near the error but that could be anywhere near the value itself).
     */
    public long getLineOffset();
}
