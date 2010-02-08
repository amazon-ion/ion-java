// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 *
 */
public class StringFieldNameEscapesTest
    extends TextTestCase
{

    @Override
    protected String wrap(String ionText)
    {
        return "{\"" + ionText + "\":null}";
    }

    @Override
    protected String printed(String ionText)
    {
        return "{\'" + ionText + "\':null}";
    }

    @Override
    protected String unwrap(IonValue v)
    {
        return ((IonStruct)v).iterator().next().getFieldName();
    }
}
