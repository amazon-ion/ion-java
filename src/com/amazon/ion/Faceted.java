// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 * Provides access to optional extension interfaces of a subject instance.
 * <p>
 * In general, facets should not extend the subject type.
 * This makes use of the facet a bit less convenient, since the user must
 * retain references to both the facet and its subject.
 * However, such extension leads to very challenging implementation problems,
 * especially when the subject is a decorator, adaptor, or similar wrapper
 * around the actual provider of the facet.
 *
 * <h2>Acknowledgements</h2>
 * This is an adaptation of the Extension Objects pattern as written by
 * Erich Gamma.
 *
 * @see Facets
 */
public interface Faceted
{
    /**
     * Returns a facet of this object if available.
     *
     * @param <T>       The requested facet type.
     * @param facetType The type token of the requested facet type.
     *
     * @return  An instance of T representing the facet of this position or
     *          null if the facet is unsupported by this object.
     *
     * @see Facets#assumeFacet(Class, Faceted) for an alternative that throws
     * an exception if the facet isn't available.
     */
    public <T> T asFacet(Class<T> facetType);
}
