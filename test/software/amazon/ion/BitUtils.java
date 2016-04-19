/*
 * Copyright 2016-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion;

public class BitUtils
{
    public static byte[] bytes(int... ints)
    {
        byte[] b = new byte[ints.length];
        for (int i = 0; i < ints.length; i++)
        {
            b[i] = (byte) (checkBounds(ints[i], 0, 256) & 0xFF);
        }
        return b;
    }

    private static int checkBounds(int value, int lowerInclusive,
                                   int upperExclusive)
    {
        if (value < lowerInclusive)
        {
            throw new IllegalArgumentException(String
                .format("Value %s is less than lower bound %s", value,
                        lowerInclusive));
        }
        else if (value >= upperExclusive)
        {
            throw new IllegalArgumentException(String
                .format("Value %s is greater than or equal to upper bound %s",
                        value, upperExclusive));
        }
        else
        {
            return value;
        }
    }
}
