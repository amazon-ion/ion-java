// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

/**
 *
 */
public class AssertionsEnabledTest
{
    /**
     * Make sure our unit test framework has assertions enabled.
     */
    @Test
    public void testAssertionsEnabled()
    {
        String message = "Java assertion failure";
        try
        {
            assert false : message;
            fail("Assertions not enabled");
        }
        catch (AssertionError e)
        {
            assertEquals(message, e.getMessage());
        }
    }
}
