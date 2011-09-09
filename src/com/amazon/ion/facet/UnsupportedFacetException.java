// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.facet;


/**
 * Indicates a failed request to find a required facet of
 * some subject.
 *
 * @see Facets#assumeFacet(Class, Faceted)
 *
 * @since R13
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
