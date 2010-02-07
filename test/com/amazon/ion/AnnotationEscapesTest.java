// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 *
 */
public class AnnotationEscapesTest
    extends TextTestCase
{

    @Override
    protected String wrap(String ionText)
    {
        return "'" + ionText + "'::null";
    }

    @Override
    protected String unwrap(IonValue v)
    {
        return v.getTypeAnnotations()[0];
    }
}
