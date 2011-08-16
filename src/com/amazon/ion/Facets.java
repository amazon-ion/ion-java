// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 * Utility methods for working with facets.
 *
 * @see Faceted
 */
public class Facets
{
    /**
     * Returns a facet of the given object if available, throwing an
     * exception otherwise.
     *
     * @return not null.
     *
     * @throws IonException if {@code o} is null or if the requested facet
     * isn't available.
     */
    public static <T> T assumeFacet(Class<T> facetType, Faceted o)
    {
        if (o != null)
        {
            T facet = o.asFacet(facetType);
            if (facet != null)
            {
                return facet;
            }
        }
        throw new IonException("Facet not available: " + facetType);
    }
}
