package com.amazon.ion;

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
