/*
 * Copyright 2009-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.math.BigDecimal;
import java.math.BigInteger;
import org.junit.Test;

/**
 * Tests illustrating semantics of built-in Java numeric types.
 */
public class JavaNumericsTest
    extends IonTestCase
{
    @Test
    public void testDoubleEquality()
    {
        Double posZeroDotZero = new Double( 0.0d);
        Double negZeroDotZero = new Double(-0.0d);

        // Object Double can distinguish +/- zero
        assertFalse(negZeroDotZero.equals(posZeroDotZero));
        assertTrue(negZeroDotZero.compareTo(posZeroDotZero) < 0);

        // Primitive double cannot
        assertTrue(-0.0d == 0.0d);

        // But:
        assertTrue(Double.compare(-0.0d, 0.0d) < 0);

        // Neither type can distinguish by precision
        assertTrue(new Double(0.0d).equals(new Double(0.00000d)));
        assertTrue(0.0d == 0.00000d);
    }

    @Test
    public void testDoubleNegativeZeroResult()
    {
        assertTrue(Double.compare(-0.0d, 0.0d) < 0);

        assertTrue(Double.compare(-1 * 0.0d, 0.0d) < 0);
        assertTrue(Double.compare(0.0d * -1, 0.0d) < 0);

        assertEquals(0, Double.compare(-1 * 0.0d, -0.0d));
    }

    @Test
    public void testDoublePrinting()
    {
        assertEquals("-0.0", "" + -0.d);
        assertEquals("-0.0", "" + -0.0d);
        assertEquals("-0.0", "" + -0.00d);

        assertEquals("-0.0", "" + new Double(-0.d));
        assertEquals("-0.0", "" + new Double(-0.0d));
        assertEquals("-0.0", "" + new Double(-0.00d));
    }

    /**
     * BigInteger doesn't distinguish negative zero.
     */
    @Test
    public void testBigIntegerSignum()
    {
        BigInteger bi = new BigInteger(-1, new byte[]{ 0 });
        assertEquals(0, bi.signum());
    }

    @Test
    public void testBigDecimalScale()
    {
        BigDecimal val = new BigDecimal("1.00");
        assertEquals(1,   val.intValue());
        assertEquals(100, val.unscaledValue().intValue());
        assertEquals(2,   val.scale());

        // Scale can be controlled precisely by constructing from String
        val = new BigDecimal("0.");
        assertEquals(BigInteger.ZERO, val.unscaledValue());
        assertEquals(0, val.scale());

        val = new BigDecimal("0.00");
        assertEquals(BigInteger.ZERO, val.unscaledValue());
        assertEquals(2, val.scale());

        // No decimal places when constructing directly from double zero
        val = new BigDecimal(0d);
        assertEquals(BigInteger.ZERO, val.unscaledValue());
        assertEquals(0, val.scale());

        val = new BigDecimal(0.00d);
        assertEquals(BigInteger.ZERO, val.unscaledValue());
        assertEquals(0, val.scale());

        // But valueOf goes through Double.toString which prints 0d as 0.0
        val = BigDecimal.valueOf(0d);
        assertEquals(BigInteger.ZERO, val.unscaledValue());
        assertEquals(1, val.scale());

        val = BigDecimal.valueOf(0.00d);
        assertEquals(BigInteger.ZERO, val.unscaledValue());
        assertEquals(1, val.scale());
    }

    /**
     * Creating a BigDecimal from negative-zero double loses the negative.
     */
    @Test
    public void testBigDecimalNegativeZero()
    {
        BigDecimal bd = new BigDecimal(-0.0d);
        assertEquals(0, bd.signum());
        assertEquals(0, Double.compare(0.0d, bd.doubleValue()));
        assertEquals(bd, bd.negate());
        assertEquals(0, bd.compareTo(new BigDecimal(0.0d)));
    }

    @Test
    public void testBigDecimalParsing()
    {
        badBigDecimalFormat(" 0");
        badBigDecimalFormat("0 ");

        assertEquals(BigDecimal.ZERO, new BigDecimal("0"));
        assertEquals(BigDecimal.ZERO, new BigDecimal("+0"));
        assertEquals(BigDecimal.ZERO, new BigDecimal("-0"));

        assertEquals("0.1000000000000000055511151231257827021181583404541015625",
                     new BigDecimal(0.1).toString());
        assertEquals("0.1",
                     new BigDecimal("0.1").toString());
    }

    public void badBigDecimalFormat(String val)
    {
        try {
            new BigDecimal(val);
            fail("Expected exception");
        }
        catch (NumberFormatException e) { /* expected */ }
    }
}
