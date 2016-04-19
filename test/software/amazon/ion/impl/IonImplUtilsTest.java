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

package software.amazon.ion.impl;

import org.junit.Assert;
import org.junit.Test;
import software.amazon.ion.impl.PrivateUtils;

public class IonImplUtilsTest
{
    @Test
    public void testEmptyUtf8()
    {
        byte[] bytes = PrivateUtils.utf8("");
        Assert.assertArrayEquals(PrivateUtils.EMPTY_BYTE_ARRAY, bytes);
    }

    @Test
    public void testEasyUtf8()
    throws Exception
    {
        String input = "abcdefghijklm";
        byte[] bytes = PrivateUtils.utf8(input);
        byte[] direct = input.getBytes("UTF-8");
        Assert.assertArrayEquals(direct, bytes);
    }
}
