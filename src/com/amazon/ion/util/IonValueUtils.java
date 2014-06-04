// Copyright (c) 2014 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.util;

import com.amazon.ion.IonValue;

/**
 * Utility methods for working with {@link IonValue}s.
 */
public class IonValueUtils
{
    /**
     * Determines whether a value is Java null, or any Ion null.
     *
     * @param value may be null.
     *
     * @return {@code (value == null || value.isNullValue())}
     */
    public static final boolean anyNull(IonValue value)
    {
        return (value == null || value.isNullValue());
    }
}
