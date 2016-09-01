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
import java.math.BigInteger;
import org.junit.Test;




public class IntTest
    extends IonTestCase
{
    public static final long A_LONG_INT = 1L + Integer.MAX_VALUE;


    public static void checkNullInt(IonInt value)
    {
        assertSame(IonType.INT, value.getType());
        assertTrue("isNullValue() is false",   value.isNullValue());

        try
        {
            value.intValue();
            fail("Expected NullValueException");
        }
        catch (NullValueException e) { }

        try
        {
            value.longValue();
            fail("Expected NullValueException");
        }
        catch (NullValueException e) { }

        assertNull("toBigInteger() isn't null", value.bigIntegerValue());
    }


    public void modifyInt(IonInt value)
    {
        assertSame(IonType.INT, value.getType());
        value.setValue(123);
        assertFalse(value.isNullValue());
        assertEquals(123, value.intValue());
        assertEquals(123L, value.longValue());
        assertEquals(BigInteger.valueOf(123), value.bigIntegerValue());

        value.setValue(A_LONG_INT);
        assertEquals(A_LONG_INT, value.longValue());
        assertEquals(BigInteger.valueOf(A_LONG_INT), value.bigIntegerValue());

        value.setValue(null);
        checkNullInt(value);
    }


    //=========================================================================
    // Test cases

    @Test
    public void testFactoryInt()
    {
        IonInt value = system().newNullInt();
        checkNullInt(value);
        modifyInt(value);
    }

    @Test
    public void testTextNullInt()
    {
        IonInt value = (IonInt) oneValue("null.int");
        checkNullInt(value);
        modifyInt(value);
    }


    @Test
    public void testNegInt()
    {
        IonInt value = (IonInt) oneValue("-1");
        assertSame(IonType.INT, value.getType());
        assertFalse(value.isNullValue());
        assertArrayEquals(new String[0], value.getTypeAnnotations());
        assertEquals(-1, value.intValue());
        assertEquals(-1L, value.longValue());

        value = (IonInt)oneValue("-1999");
        assertSame(IonType.INT, value.getType());
        assertFalse(value.isNullValue());
        assertArrayEquals(new String[0], value.getTypeAnnotations());
        assertEquals(-1999, value.intValue());
        assertEquals(-1999L, value.longValue());
        value.toString();
    }


    @Test
    public void testInts()
    {
        IonInt value = (IonInt) oneValue("1");
        assertFalse(value.isNullValue());
        assertArrayEquals(new String[0], value.getTypeAnnotations());
        assertEquals(1, value.intValue());
        assertEquals(1L, value.longValue());

        value = (IonInt) oneValue("a::" + Integer.MAX_VALUE);
        assertFalse(value.isNullValue());
        checkAnnotation("a", value);
        assertEquals(Integer.MAX_VALUE, value.intValue());
        assertEquals(Integer.MAX_VALUE, value.longValue());

        // Ensure that annotation makes it through value mods
        modifyInt(value);
        checkAnnotation("a", value);

        value = (IonInt) oneValue("" + Integer.MIN_VALUE);
        assertFalse(value.isNullValue());
        assertArrayEquals(new String[0], value.getTypeAnnotations());
        assertEquals(Integer.MIN_VALUE, value.intValue());
    }


    @Test
    public void testPositiveSign()
    {
        // Array keeps this from parsing as datagram "+ 1"
        badValue("[+1]");
        badValue("[+0]");
    }


    @Test
    public void testNegativeIntRoundTrip()
    {
        IonInt i = system().newInt(-20);
        IonInt result = (IonInt) reload(i);
        assertEquals(-20, result.intValue());
    }


    @Test
    public void testNegativeLongRoundTrip()
    {
        final long v = Long.MIN_VALUE;

        IonInt i = system().newInt(v);
        IonInt result = (IonInt) reload(i);
        assertEquals(v, result.longValue());
    }


    public void testRoundTrip(BigInteger v)
    {
        IonInt i = system().newInt(v);
        IonInt result = (IonInt) reload(i);
        assertEquals(v, result.bigIntegerValue());
    }

    @Test
    public void testNegNumberRoundTrip()
    {
        testRoundTrip(BigInteger.valueOf(Long.MAX_VALUE));
        testRoundTrip(BigInteger.valueOf(0));
        testRoundTrip(BigInteger.valueOf(-98102));
        testRoundTrip(BigInteger.valueOf(Long.MIN_VALUE+1));
        // FIXME: encoder can't handle Long.MIN_VALUE
        // testRoundTrip(BigInteger.valueOf(Long.MIN_VALUE));
    }

    @Test
    public void testLongs()
    {
        IonInt value = (IonInt) oneValue(String.valueOf(A_LONG_INT));
        assertFalse(value.isNullValue());
        assertArrayEquals(new String[0], value.getTypeAnnotations());
        assertEquals(A_LONG_INT, value.longValue());

        value = (IonInt) oneValue("a::" + Long.MAX_VALUE);
        assertFalse(value.isNullValue());
        checkAnnotation("a", value);
        assertEquals(Long.MAX_VALUE, value.longValue());

        // Ensure that annotation makes it through value mods
        modifyInt(value);
        checkAnnotation("a", value);
    }

    private static final BigInteger SUPER_BIG = new BigInteger("10123456789ABCDEF", 16);
    private static final long SUPER_BIG_TRUNC_64 = 0x0123456789ABCDEFL;
    private static final int SUPER_BIG_TRUNC_32 = 0x89ABCDEF;

    @Test
    public void testBigIntegers() {
        IonInt val = system().newInt(SUPER_BIG);
        assertEquals(SUPER_BIG, val.bigIntegerValue());
        assertEquals(SUPER_BIG_TRUNC_64, val.longValue());
        assertEquals(SUPER_BIG_TRUNC_32, val.intValue());

        assertEquals(SUPER_BIG.hashCode(), val.hashCode());

        testRoundTrip(SUPER_BIG);
    }

    @Test
    public void testBigDecimals() {
        BigDecimal big = new BigDecimal(SUPER_BIG.multiply(BigInteger.TEN), 1);
        IonInt val = system().newInt(big);
        assertEquals(SUPER_BIG, val.bigIntegerValue());
        assertEquals(SUPER_BIG_TRUNC_64, val.longValue());
        assertEquals(SUPER_BIG_TRUNC_32, val.intValue());

        assertEquals(SUPER_BIG.hashCode(), val.hashCode());
    }

    @Test
    public void testSetLongAfterSetBig() {
        IonInt val = system().newInt(SUPER_BIG);
        val.setValue(1);
        assertEquals(BigInteger.valueOf(1), val.bigIntegerValue());
    }

    @Test
    public void testStopChars()
    {
        badValue("12/");
    }

    @Test
    public void testHexadecimal()
    {
        checkInt(-3, oneValue("-0x3"));
        checkInt(-3, oneValue("-0x0003"));
    }

    @Test
    public void testIntsFromSuite()
        throws Exception
    {
        IonDatagram values = loadTestFile("good/integer_values.ion");
        // File is a sequence of many timestamp values.

        for (IonValue value : values)
        {
            assertTrue(value instanceof IonInt);
        }

    }


    @Test
    public void testReadOnlyInt()
    {
        IonInt v = system().newInt(1);
        v.makeReadOnly();
        assertEquals(1, v.intValue());

        try {
            v.setValue(2);
            fail("Expected exception for modifying read-only value");
        }
        catch (ReadOnlyValueException e) { }
        assertEquals(1, v.intValue());

        try {
            v.setValue(2L);
            fail("Expected exception for modifying read-only value");
        }
        catch (ReadOnlyValueException e) { }
        assertEquals(1, v.intValue());

        try {
            v.setValue(new Long(2));
            fail("Expected exception for modifying read-only value");
        }
        catch (ReadOnlyValueException e) { }
        assertEquals(1, v.intValue());
    }

    @Test
    public void testBinaryInt()
    {
        assertEquals(system().newInt(4), oneValue("0b0100"));
    }

    @Test
    public void testNegativeBinaryInt()
    {
        assertEquals(system().newInt(-7), oneValue("-0B111"));
    }

    @Test
    public void testIntMaxValueBinaryInt()
    {
        assertEquals(
            system().newInt(Integer.MAX_VALUE),
            binaryInt(Integer.MAX_VALUE));
    }

    @Test
    public void testIntMaxValueBinaryIntAsInt()
    {
        assertEquals(Integer.MAX_VALUE, binaryInt(Integer.MAX_VALUE).intValue());
    }

    @Test
    public void testIntMaxValueBinaryIntAsLong()
    {
        assertEquals(Integer.MAX_VALUE, binaryInt(Integer.MAX_VALUE).longValue());
    }

    @Test
    public void testIntMaxValuePlusOneBinaryInt()
    {
        assertEquals(
            system().newInt((long) Integer.MAX_VALUE + 1),
            binaryInt((long) Integer.MAX_VALUE + 1));
    }

    @Test
    public void testIntMaxValuePluxOneBinaryIntAsInt()
    {
        assertFalse((long) Integer.MAX_VALUE + 1 == (long) binaryInt((long) Integer.MAX_VALUE + 1).intValue());
    }

    @Test
    public void testIntMaxValuePluxOneBinaryIntAsLong()
    {
        assertEquals((long) Integer.MAX_VALUE + 1, binaryInt((long) Integer.MAX_VALUE + 1).longValue());
    }

    @Test
    public void testLongMaxValueBinaryInt()
    {
        assertEquals(
            system().newInt(Long.MAX_VALUE),
            oneValue("0b" + Long.toBinaryString(Long.MAX_VALUE)));
    }

    @Test
    public void testLongMaxValueBinaryIntAsInt()
    {
        assertFalse(Long.MAX_VALUE == (long) binaryInt(Long.MAX_VALUE).intValue());
    }

    @Test
    public void testLongMaxValueBinaryIntAsLong()
    {
        assertEquals(Long.MAX_VALUE, binaryInt(Long.MAX_VALUE).longValue());
    }

    @Test
    public void testLongMaxValuePlusOneBinaryInt()
    {
        BigInteger big = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);

        assertEquals(
            system().newInt(big),
            oneValue("0b" + big.toString(2)));
    }

    @Test
    public void testIntWithUnderscore()
    {
        assertEquals(system().newInt(10), oneValue("1_0"));
    }

    @Test
    public void testIntWithLeadingUnderscoreIsActuallySymbol()
    {
        assertFalse(system().newInt(10).equals(oneValue("_10")));
        assertTrue(system().newSymbol("_10").equals(oneValue("_10")));
    }

    @Test(expected = IonException.class)
    public void testIntWithMultipleUnderscores()
    {
        oneValue("1__0");
    }

    @Test(expected = IonException.class)
    public void testIntWithTrailingUnderscore()
    {
        oneValue("10_");
    }

    @Test(expected = IonException.class)
    public void testBinaryIntWithUnderscoreAfterRadixPrefix()
    {
        oneValue("0x_10");
    }

    @Test
    public void testHexIntWithUnderscore()
    {
        assertEquals(system().newInt(0x4d2), oneValue("0x4_d2"));
    }

    @Test(expected = IonException.class)
    public void testHexIntWithTrailingUnderscore()
    {
        oneValue("0x4d2_");
    }

    private IonInt binaryInt(int i)
    {
        return (IonInt) oneValue("0b" + Integer.toBinaryString(i));
    }

    private IonInt binaryInt(long l)
    {
        return (IonInt) oneValue("0b" + Long.toBinaryString(l));
    }

    @Test
    public void testGetIntegerSizeNull() {
        IonInt nullInt1 = system().newNullInt();
        assertNull(nullInt1.getIntegerSize());
        IonInt nullInt2 = (IonInt)oneValue("null.int");
        assertNull(nullInt2.getIntegerSize());
    }

    @Test
    public void testGetIntegerSizeMutatedValue() {
        IonInt value = system().newInt(0);
        assertEquals(IntegerSize.INT, value.getIntegerSize());
        value.setValue(Long.MAX_VALUE);
        assertEquals(IntegerSize.LONG, value.getIntegerSize());
        value.setValue(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE));
        assertEquals(IntegerSize.BIG_INTEGER, value.getIntegerSize());
        value.setValue(BigDecimal.valueOf(Long.MAX_VALUE));
        assertEquals(IntegerSize.LONG, value.getIntegerSize());
        value.setValue(null);
        assertNull(value.getIntegerSize());
    }

    @Test
    public void testGetIntegerSizeFromBigDecimal() {
        IonInt value = system().newInt(BigDecimal.valueOf(Integer.MIN_VALUE));
        assertEquals(IntegerSize.INT, value.getIntegerSize());
        value = system().newInt(BigDecimal.valueOf(Long.MIN_VALUE));
        assertEquals(IntegerSize.LONG, value.getIntegerSize());
        value = system().newInt(new BigDecimal(BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE)));
        assertEquals(IntegerSize.BIG_INTEGER, value.getIntegerSize());
    }

    @Test
    public void testGetIntegerSizePositiveLongBoundary() {
        testGetIntegerSizeLongBoundary(Long.MAX_VALUE);
    }

    @Test
    public void testGetIntegerSizeNegativeLongBoundary() {
        testGetIntegerSizeLongBoundary(Long.MIN_VALUE);
    }

    @Test
    public void testGetIntegerSizePositiveIntBoundary() {
        testGetIntegerSizeIntBoundary(Integer.MAX_VALUE);
    }

    @Test
    public void testGetIntegerSizeNegativeIntBoundary() {
        testGetIntegerSizeIntBoundary(Integer.MIN_VALUE);
    }

    private void testGetIntegerSizeLongBoundary(long boundaryValue) {
        BigInteger boundary = BigInteger.valueOf(boundaryValue);
        IonInt boundaryIon = (IonInt)oneValue(boundary.toString());
        assertEquals(IntegerSize.LONG, boundaryIon.getIntegerSize());
        assertEquals(boundaryValue, boundaryIon.longValue());

        BigInteger pastBoundary = (boundaryValue < 0) ? boundary.subtract(BigInteger.ONE) : boundary.add(BigInteger.ONE);
        IonInt pastBoundaryIon = (IonInt)oneValue(pastBoundary.toString());
        assertEquals(IntegerSize.BIG_INTEGER, pastBoundaryIon.getIntegerSize());
        assertEquals(pastBoundary, pastBoundaryIon.bigIntegerValue());
    }

    private void testGetIntegerSizeIntBoundary(int boundaryValue) {
        BigInteger boundary = BigInteger.valueOf(boundaryValue);
        IonInt boundaryIon = (IonInt)oneValue(boundary.toString());
        assertEquals(IntegerSize.INT, boundaryIon.getIntegerSize());
        assertEquals(boundaryValue, boundaryIon.intValue());

        BigInteger pastBoundary = (boundaryValue < 0) ? boundary.subtract(BigInteger.ONE) : boundary.add(BigInteger.ONE);
        IonInt pastBoundaryIon = (IonInt)oneValue(pastBoundary.toString());
        assertEquals(IntegerSize.LONG, pastBoundaryIon.getIntegerSize());
        assertEquals(pastBoundary.longValue(), pastBoundaryIon.longValue());
    }

}
