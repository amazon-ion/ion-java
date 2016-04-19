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
 * Indicates a failed request to find a required facet of
 * some subject.
 *
 * @see Facets#assumeFacet(Class, Faceted)
 *
 */
public class UnsupportedFacetException
    extends UnsupportedOperationException
{
    private static final long serialVersionUID = 1L;

    private Class<?> myFacetType;
    private Object   mySubject;

    public UnsupportedFacetException(Class<?> facetType, Object subject)
    {
        myFacetType = facetType;
        mySubject = subject;
    }

    @Override
    public String getMessage()
    {
        return "Facet " + myFacetType.getName()
                + " is not supported by " + mySubject;
    }

    /**
     * Gets the facet type that's not supported by the subject instance.
     *
     * @return may be null.
     */
    public Class<?> getFacetType()
    {
        return myFacetType;
    }

    /**
     * Gets the subject instance that didn't support the requested facet.
     *
     * @return may be null.
     */
    public Object getSubject()
    {
        return mySubject;
    }
}
