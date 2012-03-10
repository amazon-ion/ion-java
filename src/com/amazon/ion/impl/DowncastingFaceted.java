// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.
package com.amazon.ion.impl;

import com.amazon.ion.facet.Faceted;

/**
 * Provides a simple implementation of {@link Faceted}
 * that delegates facet interpolation as a cast.
 */
abstract class DowncastingFaceted
    implements Faceted
{
    public final <T> T asFacet(final Class<T> type)
    {
        if (!type.isInstance(this))
        {
            return null;
        }
        return type.cast(this);
    }
}
