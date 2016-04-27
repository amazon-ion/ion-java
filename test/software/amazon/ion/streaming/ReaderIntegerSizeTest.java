// Copyright (c) 2016 Amazon.com, Inc.  All rights reserved.

package software.amazon.ion.streaming;

import java.math.BigInteger;
import org.junit.Test;
import software.amazon.ion.IntegerSize;
import software.amazon.ion.ReaderMaker;
import software.amazon.ion.junit.Injected.Inject;

public class ReaderIntegerSizeTest
    extends ReaderTestCase
{

    private enum IntRadix {
        DECIMAL
        {
            @Override
            String getString(BigInteger integer)
            {
                return integer.toString();
            }
        },
        HEX
        {
            @Override
            String getString(BigInteger integer)
            {
                return injectRadixPrefix("0x", integer.toString(16));
            }
        },
        HEX_UPPER
        {
            @Override
            String getString(BigInteger integer)
            {
                return injectRadixPrefix("0x", integer.toString(16).toUpperCase());
            }
        },
        BINARY
        {
            @Override
            String getString(BigInteger integer)
            {
                return injectRadixPrefix("0b", integer.toString(2));
            }
        };

        abstract String getString(BigInteger integer);

        private static String injectRadixPrefix(String radixPrefix, String value)
        {
            if(value.charAt(0) == '-')
            {
                value = "-" + radixPrefix + value.replaceFirst("-", "");
            }
            else
            {
                value = radixPrefix + value;
            }
            return value;
        }
    }

    @Inject("readerMaker")
    public static final ReaderMaker[] READER_MAKERS = ReaderMaker.values();

    @Inject("radix")
    public static final IntRadix[] RADIXES = IntRadix.values();

    public IntRadix radix;

    public void setRadix(IntRadix value)
    {
        radix = value;
    }

    @Test
    public void testGetIntegerSizeNull()
    {
        read(  "null "
             + "true false "
             + "null.int "
             + "42.1 -421e-1 "
             + "123d4 "
             + "2016-08T "
             + "\"bar\" "
             + "foo "
             + "{{ abcd }} "
             + "{{ \"abcd\" }} "
             + "{} "
             + "[] "
             + "() "
            );
        while (in.next() != null)
        {
            assertNull(in.getIntegerSize());
        }
    }

    @Test
    public void testGetIntegerSizePositiveLongBoundary()
    {
        long boundary = Long.MAX_VALUE;
        testGetIntegerSizeLongBoundary(boundary, loadBoundaries(boundary, radix));
    }

    @Test
    public void testGetIntegerSizeNegativeLongBoundary()
    {
        long boundary = Long.MIN_VALUE;
        testGetIntegerSizeLongBoundary(boundary, loadBoundaries(boundary, radix));
    }

    @Test
    public void testGetIntegerSizePositiveIntBoundary()
    {
        int boundary = Integer.MAX_VALUE;
        testGetIntegerSizeIntBoundary(boundary, loadBoundaries(boundary, radix).longValue());
    }

    @Test
    public void testGetIntegerSizeNegativeIntBoundary()
    {
        int boundary = Integer.MIN_VALUE;
        testGetIntegerSizeIntBoundary(boundary, loadBoundaries(boundary, radix).longValue());
    }

    private void testGetIntegerSizeIntBoundary(int boundaryValue, long pastBoundary)
    {
        in.next();
        assertEquals(IntegerSize.INT, in.getIntegerSize());
        assertEquals(boundaryValue, in.intValue());
        assertEquals(IntegerSize.INT, in.getIntegerSize()); // assert nothing changes until next()
        in.next();
        assertEquals(IntegerSize.LONG, in.getIntegerSize());
        assertEquals(pastBoundary, in.longValue());
        assertEquals(IntegerSize.LONG, in.getIntegerSize());
    }

    private void testGetIntegerSizeLongBoundary(long boundaryValue, BigInteger pastBoundary)
    {
        in.next();
        assertEquals(IntegerSize.LONG, in.getIntegerSize());
        assertEquals(boundaryValue, in.longValue());
        assertEquals(IntegerSize.LONG, in.getIntegerSize());
        in.next();
        assertEquals(IntegerSize.BIG_INTEGER, in.getIntegerSize());
        assertEquals(pastBoundary, in.bigIntegerValue());
        assertEquals(IntegerSize.BIG_INTEGER, in.getIntegerSize());
    }

    private BigInteger loadBoundaries(long boundaryValue, IntRadix intRadix)
    {
        BigInteger boundary = BigInteger.valueOf(boundaryValue);
        BigInteger pastBoundary = (boundaryValue < 0) ? boundary.subtract(BigInteger.ONE) : boundary.add(BigInteger.ONE);
        String ionText = intRadix.getString(boundary) + " " + intRadix.getString(pastBoundary);
        read(ionText);
        return pastBoundary;
    }
}
