// Copyright (c) 2009-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import com.amazon.ion.util.Spans;

/**
 * Extends {@link IonReader} with capabilites specialized to Ion text data
 * streams.
 *
 * @deprecated Since IonJava R13. Use {@link TextSpan} instead.
 *
 * @see SpanProvider
 * @see TextSpan
 * @see Spans#currentSpan(Class, Object)
 */
@Deprecated
public interface IonTextReader
    extends IonReader
{

}
