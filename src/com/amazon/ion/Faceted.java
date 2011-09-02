// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 * Provides access to optional extension interfaces of a subject instance.
 * Users can request a facet of the subject by passing the desired type token
 * to {@link #asFacet(Class)}. Different implementations, or even different
 * instances, of a subject may support different facets.
 * Consult the subject's documentation to determine which facets are available
 * in each circumstance.
 *
 * <h2>Design Notes</h2>
 * <p>
 * In general, facet interfaces should not extend the subject type.
 * This makes use of the facet a bit less convenient, since the user must
 * retain references to both the facet and its subject.
 * However, such extension can lead to challenging implementation problems,
 * especially when the subject is a decorator, adaptor, or similar wrapper
 * around the actual provider of the facet.
 * <p>
 * Given a concrete {@link Faceted} class, it may be that some instances
 * support a particular facet while others do not, depending on the state of
 * the subject or the way it was constructed. In such cases
 * {@link #asFacet asFacet} should choose whether to return the facet based on
 * the subject's state.  Such classes should <em>not</em> extend the facet
 * interface (directly or indirectly), since that allows clients to bypass
 * {@link #asFacet(Class)} and simply downcast the subject to the facet,
 * causing problems for instances that can't support the facet.
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
     * Returns a facet of this subject if available.
     *
     * @param <T>       The requested facet type.
     * @param facetType The type token of the requested facet type.
     *
     * @return  An instance of T representing the facet of this subject, or
     *          null if the facet is not available.
     *
     * @see Facets#assumeFacet(Class, Faceted)
     */
    public <T> T asFacet(Class<T> facetType);
}
