// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.Decimal.negativeZero;
import static java.math.MathContext.DECIMAL64;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import junit.framework.TestCase;


/**
 * @see JavaNumericsTest
 */
public class ExtendedDecimalTest
    extends TestCase
{
    private static final Decimal ZERO_0 = Decimal.valueOf("0.");
    private static final Decimal ZERO_1 = Decimal.valueOf("0.0");
    private static final Decimal ZERO_3 = Decimal.valueOf("0.000");

    private static final Decimal NEG_ZERO_0 = negativeZero(0);
    private static final Decimal NEG_ZERO_1 = negativeZero(1);
    private static final Decimal NEG_ZERO_3 = negativeZero(3);

    public void testCommonEquals()
    {
        Decimal ibd = Decimal.valueOf(3);
        BigDecimal bd = new BigDecimal(3);

        assertEquals(ibd, bd);
        assertEquals(bd, ibd);
    }

    public void testStaticEquals()
    {
        assertTrue(Decimal.equals(BigDecimal.ZERO, Decimal.ZERO));
        assertTrue(Decimal.equals(NEG_ZERO_0, NEG_ZERO_0));
        assertTrue(Decimal.equals(NEG_ZERO_3, NEG_ZERO_3));

        assertFalse(Decimal.equals(ZERO_0, ZERO_3));
        assertFalse(Decimal.equals(ZERO_3, ZERO_0));

        assertFalse(Decimal.equals(NEG_ZERO_0, NEG_ZERO_3));
        assertFalse(Decimal.equals(NEG_ZERO_3, NEG_ZERO_0));

        assertFalse(Decimal.equals(BigDecimal.ZERO, Decimal.NEGATIVE_ZERO));
        assertFalse(Decimal.equals(Decimal.NEGATIVE_ZERO, BigDecimal.ZERO));
    }


    public void testCreationFromDouble()
    {
        testPositiveZero(0, new BigDecimal(0.0d));
        testPositiveZero(0, new BigDecimal(0.0d, DECIMAL64));

        testPositiveZero(1, BigDecimal.valueOf(0.0d));
        testPositiveZero(1, Decimal.valueOf(0.0d));
        testNegativeZero(1, Decimal.valueOf(-0.0d));

        testPositiveZero(1, Decimal.valueOf(0.0d, DECIMAL64));
        testNegativeZero(1, Decimal.valueOf(-0.0d, DECIMAL64));
    }

    public void testCreationFromBigDecimal()
    {
        assertSame(Decimal.ZERO, Decimal.valueOf(Decimal.ZERO));
        assertSame(Decimal.NEGATIVE_ZERO, Decimal.valueOf(Decimal.NEGATIVE_ZERO));

        BigDecimal val = new BigDecimal("1.23");
        Decimal converted = Decimal.valueOf(val);
        assertEquals(val, converted);
        assertTrue(Decimal.equals(val, converted));
    }

    public void testToString()
    {
        assertEquals("-0", NEG_ZERO_0.toString());
        assertEquals("-0", NEG_ZERO_0.toEngineeringString());
        assertEquals("-0", NEG_ZERO_0.toPlainString());

        assertEquals("-0.000", NEG_ZERO_3.toString());
        assertEquals("-0.000", NEG_ZERO_3.toEngineeringString());
        assertEquals("-0.000", NEG_ZERO_3.toPlainString());

        assertEquals("-0", negativeZero(0, MathContext.DECIMAL128).toString());
    }

    public void testFloatAndDoubleValue()
    {
        testNegativeZero(0, NEG_ZERO_0);
        testNegativeZero(1, NEG_ZERO_1);
        testNegativeZero(3, NEG_ZERO_3);

        // There's strange interaction between the context and doubleValue()
        // and toString().  This tickled some bugs.
        testNegativeZero(0, negativeZero(0));
        testNegativeZero(0, negativeZero(0, MathContext.DECIMAL128));

        testNegativeZero(-1, negativeZero(-1));
    }

    /**
     * @see JavaNumericsTest#testBigDecimalParsing()
     */
    public void testParsing()
    {
        // The BigDecimal parser doesn't allow whitespace, which makes our
        // life easier.
        badFormat(" 0");
        badFormat(" -0");
        badFormat("0 ");

        assertEquals(BigDecimal.ZERO, Decimal.valueOf("0"));
        assertEquals(BigDecimal.ZERO, Decimal.valueOf("+0"));
        assertEquals(Decimal.NEGATIVE_ZERO, Decimal.valueOf("-0"));
        testNegativeZero(3,  Decimal.valueOf("-0.000"));
        testNegativeZero(7,  Decimal.valueOf("-0.000e-4"));
        testNegativeZero(-1, Decimal.valueOf("-0.000e4"));

        // BigDecimal.valueOf(double) is defined in terms of print-and-parse
        assertEquals("0.1", Decimal.valueOf(0.1).toString());
        assertEquals("0.1", Decimal.valueOf("0.1").toString());
    }

    public void badFormat(String val)
    {
        try {
            Decimal.valueOf(val);
            fail("Expected exception");
        }
        catch (NumberFormatException e) { /* expected */ }
    }


    private void testNegativeZero(int expectedScale, Decimal val)
    {
        checkNegativeZero(expectedScale, val);
        checkPositiveZero(expectedScale, val.abs());
        checkPositiveZero(expectedScale, val.abs(MathContext.DECIMAL32));
        checkPositiveZero(expectedScale, Decimal.plainBigDecimal(val));
    }

    private void testPositiveZero(int expectedScale, BigDecimal val)
    {
        checkPositiveZero(expectedScale, val);
        checkPositiveZero(expectedScale, val.abs());
        checkPositiveZero(expectedScale, val.abs(MathContext.DECIMAL32));
        checkPositiveZero(expectedScale, Decimal.plainBigDecimal(val));
    }

    private void checkNegativeZero(int expectedScale, Decimal val)
    {
        checkZero(expectedScale, val);
        assertEquals(0, Float.compare(-0.0f, val.floatValue()));
        assertEquals(0, Double.compare(-0.0d, val.doubleValue()));
        assertTrue(Decimal.isNegativeZero(val));
        assertTrue(val.isNegativeZero());
    }

    private void checkPositiveZero(int expectedScale, BigDecimal val)
    {
        checkZero(expectedScale, val);
        assertEquals(0, Float.compare(0.0f, val.floatValue()));
        assertEquals(0, Double.compare(0.0d, val.doubleValue()));
        assertFalse(Decimal.isNegativeZero(val));
        if (val instanceof Decimal)
        {
            Decimal ibd = (Decimal) val;
            assertFalse(ibd.isNegativeZero());
        }
    }

    private void checkZero(int expectedScale, BigDecimal val)
    {
        assertEquals("unscaled", BigInteger.ZERO, val.unscaledValue());
        assertEquals("scale", expectedScale, val.scale());
        assertTrue(0.0f == val.floatValue());
        assertTrue(0.0d == val.doubleValue());
    }

    private void checkEquals(boolean expected, BigDecimal v1, BigDecimal v2)
    {
        assertEquals(expected, Decimal.equals(v1, v2));
        assertEquals(expected, Decimal.equals(v2, v1));
    }
}
