// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.facet;


/**
 * Indicates a failed request to find a required {@linkplain Facets facet} of
 * some subject.
 *
 * @see Facets#assumeFacet(Class, Faceted)
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

    public Class<?> getFacetType()
    {
        return myFacetType;
    }

    public Object getSubject()
    {
        return mySubject;
    }
}
