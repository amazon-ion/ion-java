/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import com.amazon.ion.IonNumber.Classification;
import java.math.BigDecimal;



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

        assertNull("toBigDecimal() isn't null", value.bigDecimalValue());
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

    public void testFactoryDecimal()
    {
        IonDecimal value = system().newNullDecimal();
        checkNullDecimal(value);
        modifyDecimal(value);
    }

    public void testTextNullDecimal()
    {
        IonDecimal value = (IonDecimal) oneValue("null.decimal");
        checkNullDecimal(value);
        modifyDecimal(value);
    }

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
        testPrinting("123d-3", "1.23d-1");
        testPrinting("123d-4", "1.23d-2");

        testPrinting("-123d3",  "-123d3");
        testPrinting("-123d1",  "-123d1");
        testPrinting("-123d0",  "-123.");
        testPrinting("-123d-1", "-12.3");
        testPrinting("-123d-2", "-1.23");
        testPrinting("-123d-3", "-1.23d-1");
        testPrinting("-123d-4", "-1.23d-2");

        // Zeros are a bit trickier
//        testPrinting("0.0", "0.0");
//        testPrinting("0.00", "0.00");
        testPrinting("0d1",   "0d1");
        testPrinting("0d-1",  "0d-1");
        testPrinting("0d2",   "0d2");
        testPrinting("0d-2",  "0d-2"); // should be 0.00 or 0.0d-1 ?
    }

    public void testPrinting(String input, String output)
    {
        IonDecimal value = (IonDecimal) oneValue(input);
        assertEquals(output, value.toString());
    }


    public void testNegativeZero()
    {
        // Yes, Java floating point types can handle -0.0!
        final float  floatNegZero  = Float.parseFloat("-0.");
        final double doubleNegZero = Double.parseDouble("-0.");

        IonDecimal value = (IonDecimal) oneValue("-0.");
        assertEquals(floatNegZero,  value.floatValue());
        assertEquals(doubleNegZero, value.doubleValue());
        assertEquals(Classification.NEGATIVE_ZERO, value.getClassification());

        // But BigDecimal cannot. :(
        BigDecimal dec = value.bigDecimalValue();
        checkDecimal(0, 0, dec);
        assertEquals(0, dec.signum());

        IonDecimal value2 = (IonDecimal) oneValue("-0d2");
//        assertFalse(value2.equals(value));
        assertEquals(floatNegZero,  value2.floatValue());
        assertEquals(doubleNegZero, value2.doubleValue());
        assertEquals(Classification.NEGATIVE_ZERO, value2.getClassification());
        checkDecimal(0, -2, value2.bigDecimalValue());

        value2 = (IonDecimal) oneValue("-0d1");
//        assertFalse(value2.equals(value));
        assertEquals(floatNegZero,  value2.floatValue());
        assertEquals(doubleNegZero, value2.doubleValue());
        assertEquals(Classification.NEGATIVE_ZERO, value2.getClassification());
        checkDecimal(0, -1, value2.bigDecimalValue());
    }

    public void checkDecimal(int unscaled, int scale, BigDecimal actual)
    {
        assertEquals("decimal unscaled value",
                     unscaled, actual.unscaledValue().intValue());
        assertEquals("decimal scale",
                     scale, actual.scale());
    }

    // TODO FIXME ION-97
    public void XXXtestNegativeZeroEquality()
    {
        IonDecimal posZero = (IonDecimal) oneValue("0.");
        IonDecimal negZero = (IonDecimal) oneValue("-0.");

        checkNotEquals(posZero, negZero);
    }

    // TODO FIXME ION-98
    public void XXXtestPrecisionEquality()
    {
        IonDecimal v1 = (IonDecimal) oneValue("1.00");
        IonDecimal v2 = (IonDecimal) oneValue("1.000");

        checkNotEquals(v1, v2);
    }


    public void checkNotEquals(IonValue v1, IonValue v2)
    {
        assertFalse(v1.equals(v2));
        assertFalse(v2.equals(v1));
    }


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


    public void testScale()
    {
        final BigDecimal one_00 = new BigDecimal("1.00");

        assertEquals(1,   one_00.intValue());
        assertEquals(100, one_00.unscaledValue().intValue());
        assertEquals(2,   one_00.scale());

        IonDecimal value = (IonDecimal) oneValue("1.00");
        assertEquals(one_00, value.bigDecimalValue());
    }
}
