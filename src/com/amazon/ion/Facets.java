// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 * Utility methods for working with {@link Faceted facets}.
 *
 * @see Faceted
 */
public class Facets
{
    //   *** IMPLEMENTATION NOTE ***
    //
    // In order to ensure fast operation, the code here is denormalized.
    // It's very simple so I find that preferable to additional null-checks
    // that would be required by following the "Once And Only Once" rule.


    /**
     * Returns a facet of the given subject if available, returning null
     * otherwise.
     * <p>
     * This does not attempt to cast the subject to the requested type, since
     * the {@link Faceted} interface declares the intent to control the
     * conversion.
     *
     * @return the requested facet, or null if {@code subject} is null or if
     *  the requested facet isn't available.
     */
    public static <T> T asFacet(Class<T> facetType, Faceted subject)
    {
        return subject == null ? null : subject.asFacet(facetType);
    }


    /**
     * Returns a facet of the given subject if available, returning null
     * otherwise.
     * <p>
     * If the subject implements {@link Faceted}, then this conversion is
     * delegated to {@link Faceted#asFacet(Class)}. Otherwise, a simple
     * cast of the subject is attempted.
     *
     * @return the requested facet, or null if {@code subject} is null or if
     *  the requested facet isn't available.
     */
    public static <T> T asFacet(Class<T> facetType, Object subject)
    {
        T facet = null;
        if (subject instanceof Faceted)
        {
            facet = ((Faceted)subject).asFacet(facetType);
        }
        else if (facetType.isInstance(subject))
        {
            facet = facetType.cast(subject);
        }

        return facet;
    }


    /**
     * Returns a facet of the given subject if available, throwing an
     * exception otherwise.
     * <p>
     * This does not attempt to cast the subject to the requested type, since
     * the {@link Faceted} interface declares the intent to control the
     * conversion.
     *
     * @return not null.
     *
     * @throws FacetNotAvailable if {@code subject} is null or if the
     *  requested facet isn't available.
     */
    public static <T> T assumeFacet(Class<T> facetType, Faceted subject)
    {
        if (subject != null)
        {
            T facet = subject.asFacet(facetType);
            if (facet != null) return facet;
        }

        throw new FacetNotAvailable(facetType, subject);
    }


    /**
     * Returns a facet of the given subject if available, throwing an
     * exception otherwise.
     * <p>
     * <p>
     * If the subject implements {@link Faceted}, then this conversion is
     * delegated to {@link Faceted#asFacet(Class)}. Otherwise, a simple
     * cast of the subject is attempted.
     *
     * @return not null.
     *
     * @throws FacetNotAvailable if {@code subject} is null or if the
     *  requested facet isn't available.
     */
    public static <T> T assumeFacet(Class<T> facetType, Object subject)
    {
        if (subject instanceof Faceted)
        {
            T facet = ((Faceted)subject).asFacet(facetType);
            if (facet != null) return facet;
        }
        else if (facetType.isInstance(subject))
        {
            return facetType.cast(subject);
        }

        throw new FacetNotAvailable(facetType, subject);
    }
}
