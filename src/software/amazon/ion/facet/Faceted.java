/*
 * Copyright 2011-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion.facet;

/**
 * Provides access to optional extension interfaces of a subject instance.
 * Users can request a facet of the subject by passing the desired type token
 * to {@link #asFacet(Class)}. Different implementations, or even different
 * instances, of a subject may support different facets.
 * Consult the subject's documentation to determine which facets are available
 * in each circumstance.
 *
 * <h2>Design Notes</h2>
 *
 * Given a concrete {@link Faceted} class, it may be that some instances
 * support a particular facet while others do not, depending on the state of
 * the subject or the way it was constructed. In such cases
 * {@link #asFacet(Class) asFacet} should choose whether to return the facet
 * based on the subject's state.
 * Such classes should <em>not</em> extend the facet
 * interface (directly or indirectly), since that allows clients to bypass
 * {@link #asFacet(Class) asFacet} and simply downcast the subject to the facet,
 * causing problems for instances that can't support the facet.
 *
 * @see Facets
 *
 */
public interface Faceted
{
    /**
     * Returns a facet of this subject if supported.
     *
     * @param <T>       The requested facet type.
     * @param facetType The type token of the requested facet type.
     *
     * @return  An instance of T representing the facet of the subject, or
     *          null if the facet is not supported by the subject.
     *
     * @see Facets#asFacet(Class, Faceted)
     * @see Facets#assumeFacet(Class, Faceted)
     */
    public <T> T asFacet(Class<T> facetType);
}
