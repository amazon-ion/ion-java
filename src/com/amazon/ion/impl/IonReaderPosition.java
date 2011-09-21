// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.Span;


/**
 * This interface defines the objects which can hold a readers current value position.
 * <p>
 * Note well that instances of {@link IonReaderPosition} are necessarily opaque and are only
 * guaranteed to be valid with the instance of {@link IonReaderWithPosition} that vended the
 * position.
 *
 * @deprecated Use {@link Span}.
 */
@Deprecated
public interface IonReaderPosition
    extends Span
{
}
