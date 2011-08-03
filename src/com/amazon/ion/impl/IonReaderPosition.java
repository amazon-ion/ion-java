// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;


/**
 * This interface defines the objects which can hold a readers current value position.
 * <p>
 * Note well that instances of {@link IonReaderPosition} are necessarily opaque and are only
 * guaranteed to be valid with the instance of {@link IonReaderWithPosition} that vended the
 * position.
 */
public interface IonReaderPosition
{
    /**
     * Returns a facet of this position if available.
     *
     * @param <T>       The facet type to request.
     * @param type      The type token of the facet type to request.
     *
     * @return  An instance of T representing the facet of this position or <code>null</code>
     *          if the facet is unsupported.
     */
    public <T> T asFacet(Class<T> type);
}
