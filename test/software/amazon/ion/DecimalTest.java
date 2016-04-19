/*
 * Copyright 2007-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import org.junit.Test;
import software.amazon.ion.Decimal;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonDecimal;
import software.amazon.ion.IonType;
import software.amazon.ion.NullValueException;



public class DecimalTest
    extends IonTestCase
{
    /** A double that's too big for a float */
    public static final double A_DOUBLE = 1D + Float.MAX_VALUE;


    public static void checkNullDecimal(IonDecimal value)
    {
        assertSame(IonType.DECIMAL, value.getType());
        assertTrue("isNullValue is false", value.isNullValue());

        try
        {
            value.floatValue();
            fail("Expected NullValueException");
        }
        catch (NullValueException e) { }

        try
        {
            value.doubleValue();
            fail("Expected NullValueException");
        }
        catch (NullValueException e) { }

        assertNull("bigDecimalValue() isn't null", value.bigDecimalValue());
    }


    public void modifyDecimal(IonDecimal value)
    {
        float fVal = 123.45F;

        value.setValue(fVal);
        assertEquals(fVal, value.floatValue());
        assertEquals((double) fVal, value.doubleValue());
        assertEquals(fVal, value.bigDecimalValue().floatValue());

        value.setValue(A_DOUBLE);
        assertEquals(A_DOUBLE, value.doubleValue());
        assertEquals(A_DOUBLE, value.bigDecimalValue().doubleValue());

        value.setValue(null);
        checkNullDecimal(value);
    }


    //=========================================================================
    // Test cases

    /**
     * Ensure that {@link Decimal#equals} and {@link Decimal#hashCode}
     * are in sync.
     */
    @Test
    public void testDecimalHash()
    {
        BigDecimal d1 = new BigDecimal ("0.");
        BigDecimal d2 = Decimal.valueOf("0.");
        assertEquals(d1, d2);
        assertEquals("hash code", d1.hashCode(), d2.hashCode());

        d1 = new BigDecimal ("0.00");
        d2 = Decimal.valueOf("0.00");
        assertEquals(d1, d2);
        assertEquals("hash code", d1.hashCode(), d2.hashCode());

        // Decimal doesn't override equals() so it negative and positive zeros
        // are equal.  We can't change that since we can't make it symmetric.
        // We must also keep the hash codes the same.
        d1 = new BigDecimal ( "0.00");
        d2 = Decimal.valueOf("-0.00");
        assertEquals(d1, d2);
        assertEquals("hash code", d1.hashCode(), d2.hashCode());
    }


    @Test
    public void testFactoryDecimal()
    {
        IonDecimal value = system().newNullDecimal();
        checkNullDecimal(value);
        modifyDecimal(value);
    }

    @Test
    public void testTextNullDecimal()
    {
        IonDecimal value = (IonDecimal) oneValue("null.decimal");
        checkNullDecimal(value);
        modifyDecimal(value);
    }

    @Test
    public void testDecimals()
    {
        IonDecimal value = (IonDecimal) oneValue("1.0");
        assertSame(IonType.DECIMAL, value.getType());
        assertFalse(value.isNullValue());
        assertArrayEquals(new String[0], value.getTypeAnnotations());
        assertEquals(1.0F, value.floatValue());
        assertEquals(1.0D, value.doubleValue());

        assertEquals(new BigDecimal(1).setScale(1), value.bigDecimalValue());
        // TODO more...

        value = (IonDecimal) oneValue("a::1.0");
        assertFalse(value.isNullValue());
        checkAnnotation("a", value);

        // Ensure that annotation makes it through value mods
        modifyDecimal(value);
        checkAnnotation("a", value);
    }

    @Test
    public void testDFormat()
    {
        IonDecimal value = (IonDecimal) oneValue("0d0");
        assertEquals(0D, value.doubleValue());

        value = (IonDecimal) oneValue("0D0");
        assertEquals(0D, value.doubleValue());

        value = (IonDecimal) oneValue("123d0");
        assertEquals(123D, value.doubleValue());

        value = (IonDecimal) oneValue("123D0");
        assertEquals(123D, value.doubleValue());

        value = (IonDecimal) oneValue("123.45d0");
        assertEquals(123.45D, value.doubleValue());

        value = (IonDecimal) oneValue("123.45D0");
        assertEquals(123.45D, value.doubleValue());

        value = (IonDecimal) oneValue("123d1");
        assertEquals(1230D, value.doubleValue());

        value = (IonDecimal) oneValue("-123d1");
        assertEquals(-1230D, value.doubleValue());

        value = (IonDecimal) oneValue("123d+1");
        assertEquals(1230D, value.doubleValue());

        value = (IonDecimal) oneValue("-123d+1");
        assertEquals(-1230D, value.doubleValue());

        value = (IonDecimal) oneValue("123d-1");
        assertEquals(12.3D, value.doubleValue());

        value = (IonDecimal) oneValue("-123d-1");
        assertEquals(-12.3D, value.doubleValue());
    }

    @Test
    public void testPrinting()
    {
        testPrinting("0d0", "0.");
        testPrinting("0D0", "0.");
        testPrinting("0.",  "0.");

        testPrinting("1.0",  "1.0");
        testPrinting("1.00", "1.00");
        testPrinting("10.0", "10.0");
        testPrinting("10.0d1",  "100.");
        testPrinting("10.0d-1", "1.00");
        testPrinting("100d-2",  "1.00");
        testPrinting("100d2",   "100d2");
        testPrinting("100d0",   "100.");

        testPrinting("123d3",  "123d3");
        testPrinting("123d1",  "123d1");
        testPrinting("123d0",  "123.");
        testPrinting("123d-1", "12.3");
        testPrinting("123d-2", "1.23");
        testPrinting("123d-3", "0.123");
        testPrinting("123d-4", "0.0123");

        testPrinting("-123d3",  "-123d3");
        testPrinting("-123d1",  "-123d1");
        testPrinting("-123d0",  "-123.");
        testPrinting("-123d-1", "-12.3");
        testPrinting("-123d-2", "-1.23");
        testPrinting("-123d-3", "-0.123");
        testPrinting("-123d-4", "-0.0123");

        // Zeros are a bit trickier
//        testPrinting("0.0", "0.0");
//        testPrinting("0.00", "0.00");
        testPrinting("0d1",   "0d1");
        testPrinting("0d-1",  "0.0");
        testPrinting("0d2",   "0d2");
        testPrinting("0d-2",  "0.00"); // should be 0.00 or 0.0d-1 ?
    }

    public void testPrinting(String input, String output)
    {
        IonDecimal value = (IonDecimal) oneValue(input);
        assertEquals(output, value.toString());
    }


    @Test
    public void testNegativeZero()
    {
        IonDecimal value = decimal("-0.");
        testNegativeZero(0, value);

        IonDecimal value2 = decimal("-0d2");
        assertFalse(value2.equals(value));
        testNegativeZero(-2, value2);

        value2 = decimal("-0d1");
        assertFalse(value2.equals(value));
        testNegativeZero(-1, value2);

        value2 = decimal("1.");
        value2.setValue(-0f);
        testNegativeZero(1, value2);

        value2 = decimal("1.");
        value2.setValue(-0d);
        testNegativeZero(1, value2);
    }

    public void testNegativeZero(int scale, IonDecimal actual)
    {
        assertEquals(-0f, actual.floatValue());
        assertEquals(-0d, actual.doubleValue());

        BigDecimal bd = actual.bigDecimalValue();
        Decimal dec = actual.decimalValue();

        assertEquals(0, BigDecimal.ZERO.compareTo(bd));

        checkDecimal(0, scale, bd);
        checkDecimal(0, scale, dec);
        assertEquals(0, Decimal.NEGATIVE_ZERO.compareTo(dec));
        assertTrue(dec.isNegativeZero());
    }

    public void checkDecimal(int unscaled, int scale, BigDecimal actual)
    {
        assertEquals("decimal unscaled value",
                     unscaled, actual.unscaledValue().intValue());
        assertEquals("decimal scale",
                     scale, actual.scale());
    }

    @Test
    public void testNegativeZeroEquality()
    {
        checkEquality(false, "0.",  "-0.");
        checkEquality(false, "0.000",  "-0.000");
    }

    @Test
    public void testPrecisionEquality()
    {
        checkEquality(true, "1.",  "1.");
        checkEquality(true, "0.0", "0.0");

        checkEquality(false, "1.",  "1.0");
        checkEquality(false, "1.0", "1.00");

        checkEquality(false, "0.",  "0.0");
        checkEquality(false, "0.0", "0.00");
    }


    public void checkEquality(boolean expected, String v1, String v2)
    {
        IonDecimal d1 = decimal(v1);
        IonDecimal d2 = decimal(v2);

        assertEquals(expected, d1.equals(d2));
        assertEquals(expected, d2.equals(d1));
    }


    @Test
    public void testBinaryDecimals()
        throws Exception
    {
        IonDatagram dg = loadTestFile("good/decimalOneDotZero.10n");
        assertEquals(1, dg.size());

        IonDecimal value = (IonDecimal) dg.get(0);
        BigDecimal dec = value.bigDecimalValue();
        checkDecimal(10, 1, dec);
        assertEquals(1,  dec.intValue());

        dg = loadTestFile("good/decimalNegativeOneDotZero.10n");
        assertEquals(1, dg.size());

        value = (IonDecimal) dg.get(0);
        dec = value.bigDecimalValue();
        checkDecimal(-10, 1, dec);
        assertEquals(-1, dec.intValue());
    }


    @Test
    public void testScale()
    {
        final BigDecimal one_00 = new BigDecimal("1.00");

        assertEquals(1,   one_00.intValue());
        assertEquals(100, one_00.unscaledValue().intValue());
        assertEquals(2,   one_00.scale());

        IonDecimal value = (IonDecimal) oneValue("1.00");
        assertEquals(one_00, value.bigDecimalValue());
    }


    @Test
    public void testSetValue()
    {
        IonDecimal value = decimal("1.23");
        value.setValue(123);
        checkDecimal(123, 0, value.bigDecimalValue());
    }

}
