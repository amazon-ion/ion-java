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

import org.junit.Ignore;
import org.junit.Test;
import software.amazon.ion.IonFloat;
import software.amazon.ion.IonType;
import software.amazon.ion.NullValueException;



public class FloatTest
    extends IonTestCase
{
    /** A double that's too big for a float */
    public static final double A_DOUBLE = 1D + Float.MAX_VALUE;

    public static void checkNullFloat(IonFloat value)
    {
        assertSame(IonType.FLOAT, value.getType());
        assertTrue("isNullValue is false", value.isNullValue());
        assertFalse(value.isNumericValue());

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


    public void modifyFloat(IonFloat value)
    {
        float  fVal = 123.45F;

        value.setValue(fVal);
        assertEquals(fVal, value.floatValue());
        assertEquals((double) fVal, value.doubleValue());
        assertEquals(fVal, value.bigDecimalValue().floatValue());
        assertTrue(value.isNumericValue());

        value.setValue(A_DOUBLE);
        assertEquals(A_DOUBLE, value.doubleValue());
        assertEquals(A_DOUBLE, value.bigDecimalValue().doubleValue());
        assertTrue(value.isNumericValue());

        value.setValue(null);
        checkNullFloat(value);
    }


    //=========================================================================
    // Test cases

    @Test
    public void testJavaDouble()
    {
        assertTrue(-1200.d == -1200.0d);
        assertTrue(-1200d == -1200.0d);
        assertTrue(-1200d == -1200.000d);
    }


    @Test
    public void testFactoryFloat()
    {
        IonFloat value = system().newNullFloat();
        checkNullFloat(value);
        modifyFloat(value);
    }

    @Test
    public void testTextNullFloat()
    {
        IonFloat value = (IonFloat) oneValue("null.float");
        checkNullFloat(value);
        modifyFloat(value);
    }

    @Test
    public void testFloats()
    {
        IonFloat value = (IonFloat) oneValue("1.0e0");
        assertSame(IonType.FLOAT, value.getType());
        assertFalse(value.isNullValue());
        assertArrayEquals(new String[0], value.getTypeAnnotations());
        assertEquals(1.0F, value.floatValue());
        assertEquals(1.0D, value.doubleValue());

        // TODO more...

        value = (IonFloat) oneValue("a::1.0e0");
        assertFalse(value.isNullValue());
        checkAnnotation("a", value);

        // Ensure that annotation makes it through value mods
        modifyFloat(value);
        checkAnnotation("a", value);

        // TODO test BigDecimal
    }

    @Test
    public void testParsingSpecialFloats()
    {
        IonFloat value = (IonFloat) oneValue("nan");
        checkNan(value);

        value = (IonFloat) oneValue("+inf");
        checkPosInf(value);

        value = (IonFloat) oneValue("-inf");
        checkNegInf(value);
    }

    @Test
    public void testCreatingSpecialFloats()
    {
        IonFloat value = system().newFloat(Double.NaN);
        checkNan(value);

        value = system().newFloat(Double.POSITIVE_INFINITY);
        checkPosInf(value);

        value = system().newFloat(Double.NEGATIVE_INFINITY);
        checkNegInf(value);

        value.setValue(Float.NaN);
        checkNan(value);

        value.setValue(Float.POSITIVE_INFINITY);
        checkPosInf(value);

        value.setValue(Float.NEGATIVE_INFINITY);
        checkNegInf(value);

        value.setValue(Double.NaN);
        checkNan(value);

        value.setValue(Double.POSITIVE_INFINITY);
        checkPosInf(value);

        value.setValue(Double.NEGATIVE_INFINITY);
        checkNegInf(value);
    }

    public void checkNan(IonFloat actual)
    {
        checkSpecial(Float.NaN, Double.NaN, "nan", actual);
    }

    public void checkPosInf(IonFloat actual)
    {
        checkSpecial(Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, "+inf",
                     actual);
        checkSpecial(Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, "+inf",
                     actual.clone());
    }

    public void checkNegInf(IonFloat actual)
    {
        checkSpecial(Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, "-inf",
                     actual);
        checkSpecial(Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, "-inf",
                     actual.clone());
    }

    public void checkSpecial(float expectedFloat,
                             double expectedDouble,
                             String expectedText,
                             IonFloat actual)
    {
        assertEquals(expectedFloat, actual.floatValue());
        assertEquals(expectedDouble, actual.doubleValue());
        assertEquals(expectedText, actual.toString());
        assertFalse(actual.isNumericValue());

        try
        {
            actual.bigDecimalValue();
            fail("expected exception");
        }
        catch (NumberFormatException e) { }
    }

    /**
     * TODO amznlabs/ion-java#64 Un-ignore this when all dependent build pipelines
     * use a version of the JVM that doesn't have this defect.
     *
     * https://blogs.oracle.com/security/entry/security_alert_for_cve-2010-44
     * http://www.exploringbinary.com/java-hangs-when-converting-2-2250738585072012e-308/
     */
    @Test @Ignore
    public void testJavaDblMinBug()
    {
        String breakingValue = "2.2250738585072012e-308";
        double d = Double.valueOf(breakingValue);
        IonFloat f = (IonFloat) system().singleValue(breakingValue);
        assertEquals(d, f.doubleValue());
    }
}
