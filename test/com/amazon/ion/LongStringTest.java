// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 *
 */
public class LongStringTest
    extends TextTestCase
{

    @Override
    protected String wrap(String ionText)
    {
        return "'''" + ionText + "'''";
    }

    @Override
    protected String printed(String ionText)
    {
        return "\"" + ionText + "\"";
    }
}
