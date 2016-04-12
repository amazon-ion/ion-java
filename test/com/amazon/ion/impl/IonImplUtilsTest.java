// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import org.junit.Assert;
import org.junit.Test;

public class IonImplUtilsTest
{
    @Test
    public void testEmptyUtf8()
    {
        byte[] bytes = _Private_Utils.utf8("");
        Assert.assertArrayEquals(_Private_Utils.EMPTY_BYTE_ARRAY, bytes);
    }

    @Test
    public void testEasyUtf8()
    throws Exception
    {
        String input = "abcdefghijklm";
        byte[] bytes = _Private_Utils.utf8(input);
        byte[] direct = input.getBytes("UTF-8");
        Assert.assertArrayEquals(direct, bytes);
    }
}
