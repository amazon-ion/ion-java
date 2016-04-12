// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

public interface Checker
{
    /**
     * @param expectedText null means text isn't known.
     */
    Checker fieldName(String expectedText, int expectedSid);

    /** Check the first annotation's text */
    Checker annotation(String expectedText);

    /** Check the first annotation's text and sid */
    Checker annotation(String expectedText, int expectedSid);

    /** Check that all the annotations exist in the given order. */
    Checker annotations(String[] expectedTexts, int[] expectedSids);
}
