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
 * Utility methods for working with facets.
 *
 * @see Faceted
 *
 */
public class Facets
{
    //   *** IMPLEMENTATION NOTE ***
    //
    // In order to ensure fast operation, the code here is denormalized.
    // It's very simple so I find that preferable to additional null-checks
    // that would be required by following the "Once And Only Once" rule.


    /**
     * Returns a facet of the given subject if supported, returning null
     * otherwise.
     * <p>
     * This does not attempt to cast the subject to the requested type, since
     * the {@link Faceted} interface declares the intent to control the
     * conversion.
     *
     * @return the requested facet, or null if {@code subject} is null or if
     *  subject doesn't support the requested facet type.
     */
    public static <T> T asFacet(Class<T> facetType, Faceted subject)
    {
        return subject == null ? null : subject.asFacet(facetType);
    }


    /**
     * Returns a facet of the given subject if supported, returning null
     * otherwise.
     * <p>
     * If the subject implements {@link Faceted}, then this conversion is
     * delegated to {@link Faceted#asFacet(Class)}. Otherwise, a simple
     * cast of the subject is attempted.
     *
     * @return the requested facet, or null if {@code subject} is null or if
     *  subject doesn't support the requested facet type.
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
     * Returns a facet of the given subject if supported, throwing an
     * exception otherwise.
     * <p>
     * This does not attempt to cast the subject to the requested type, since
     * the {@link Faceted} interface declares the intent to control the
     * conversion.
     *
     * @return not null.
     *
     * @throws UnsupportedFacetException if {@code subject} is null or if the
     *  subject doesn't support the requested facet type.
     */
    public static <T> T assumeFacet(Class<T> facetType, Faceted subject)
    {
        if (subject != null)
        {
            T facet = subject.asFacet(facetType);
            if (facet != null) return facet;
        }

        throw new UnsupportedFacetException(facetType, subject);
    }


    /**
     * Returns a facet of the given subject if supported, throwing an
     * exception otherwise.
     * <p>
     * If the subject implements {@link Faceted}, then this conversion is
     * delegated to {@link Faceted#asFacet(Class)}. Otherwise, a simple
     * cast of the subject is attempted.
     *
     * @return not null.
     *
     * @throws UnsupportedFacetException if {@code subject} is null or if the
     *  subject doesn't support the requested facet type.
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

        throw new UnsupportedFacetException(facetType, subject);
    }
}
