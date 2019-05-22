package com.amazon.ion.impl;

import com.amazon.ion.Timestamp;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class IonBinaryTest {

    @Test
    public void testLenIonBooleanWithTypeDesc() {
        assertEquals(1, IonBinary.lenIonBooleanWithTypeDesc(false));
        assertEquals(1, IonBinary.lenIonBooleanWithTypeDesc(true));
    }

    @Test
    public void testLenIonNullWithTypeDesc() {
        assertEquals(1, IonBinary.lenIonNullWithTypeDesc());
    }

    @Test
    public void testLenVarInt() {
        assertEquals(0, IonBinary.lenVarInt(0L));
        assertEquals(1, IonBinary.lenVarInt(5L));
        assertEquals(2, IonBinary.lenVarInt(2049L));
        assertEquals(3, IonBinary.lenVarInt(524289L));
        assertEquals(4, IonBinary.lenVarInt(8388613L));
        assertEquals(5, IonBinary.lenVarInt(-201326592L));
        assertEquals(5, IonBinary.lenVarInt(201326592L));
        assertEquals(6, IonBinary.lenVarInt(17179869185L));
        assertEquals(7, IonBinary.lenVarInt(140737496743941L));
        assertEquals(8, IonBinary.lenVarInt(422212473454597L));
        assertEquals(9, IonBinary.lenVarInt(72198331534671877L));
        assertEquals(10, IonBinary.lenVarInt(4611686035607257089L));
    }

    @Test
    public void testLenTypeDescWithAppropriateLenField() {
        assertEquals(1,
            IonBinary.lenTypeDescWithAppropriateLenField(8, 1));
        assertEquals(1,
            IonBinary.lenTypeDescWithAppropriateLenField(2, 1));
        assertEquals(1,
            IonBinary.lenTypeDescWithAppropriateLenField(0, 3));
        assertEquals(1,
            IonBinary.lenTypeDescWithAppropriateLenField(0, 291));
    }

    @Test
    public void testLenIonString() {
        assertEquals(0, IonBinary.lenIonString(null));
        assertEquals(0, IonBinary.lenIonString(""));
        assertEquals(4, IonBinary.lenIonString("test"));
        assertEquals(5, IonBinary.lenIonString("other"));
        assertEquals(4, IonBinary.lenIonString("\uD801\uDC37"));
        assertEquals(2, IonBinary.lenIonString("\u007E\u007F"));
        assertEquals(3, IonBinary.lenIonString("\u0001\u0085"));
        assertEquals(4, IonBinary.lenIonString("\u0001\u0901"));
    }

    @Test
    public void testLenUInt() {
        assertEquals(0, IonBinary.lenUInt(0L));
        assertEquals(1, IonBinary.lenUInt(129L));
        assertEquals(2, IonBinary.lenUInt(256L));
        assertEquals(3, IonBinary.lenUInt(8388609L));
        assertEquals(4, IonBinary.lenUInt(2155872257L));
        assertEquals(5, IonBinary.lenUInt(274877907200L));
        assertEquals(6, IonBinary.lenUInt(141012366262528L));
        assertEquals(7, IonBinary.lenUInt(18014400665354241L));
        assertEquals(8, IonBinary.lenUInt(4611686018427388160L));
    }

    @Test
    public void testIsNibbleZero(){
        assertTrue(IonBinary.isNibbleZero(BigDecimal.ZERO));

        assertFalse(IonBinary.isNibbleZero(BigDecimal.ONE));
        assertFalse(IonBinary.isNibbleZero(new BigDecimal(-1)));
        assertFalse(IonBinary.isNibbleZero(BigDecimal.TEN));
    }

    @Test
    public void testLenIonDecimal() {
        assertEquals(0, IonBinary.lenIonDecimal(null));
        assertEquals(2, IonBinary.lenIonDecimal(BigDecimal.ONE));
        assertEquals(2, IonBinary.lenIonDecimal(new BigDecimal(-1)));
        assertEquals(2, IonBinary.lenIonDecimal(new BigDecimal(1.5)));
        assertEquals(2, IonBinary.lenIonDecimal(new BigDecimal(-1.5)));
        assertEquals(2, IonBinary.lenIonDecimal(BigDecimal.TEN));
        assertEquals(2, IonBinary.lenIonDecimal(new BigDecimal(10.5)));
    }

    @Test
    public void testLenIonTimestamp(){
        Timestamp precisionYear = Timestamp.forYear(1999);
        assertEquals(3, IonBinary.lenIonTimestamp(precisionYear));
        Timestamp precisionMonth = Timestamp.forMonth(2001, 8);
        assertEquals(4, IonBinary.lenIonTimestamp(precisionMonth));
        Timestamp precisionDay = Timestamp.forDay(1984, 5, 22);
        assertEquals(5, IonBinary.lenIonTimestamp(precisionDay));
        Timestamp precisionMinute = Timestamp.forMinute(2009, 7, 13, 14, 02, 0);
        assertEquals(7, IonBinary.lenIonTimestamp(precisionMinute));
        Timestamp precisionSecond = Timestamp.forSecond(1997, 3, 28, 9, 15, 57, 0);
        assertEquals(8, IonBinary.lenIonTimestamp(precisionSecond));
        Timestamp precisionSecondOffset = Timestamp.forSecond(1997, 3, 28, 9, 15, 57, 3);
        assertEquals(8, IonBinary.lenIonTimestamp(precisionSecondOffset));
        Timestamp precisionFraction = Timestamp.forSecond(1997, 3, 28, 9, 15, new BigDecimal(57.3849), 0);
        assertEquals(29, IonBinary.lenIonTimestamp(precisionFraction));
    }

    @Test
    public void testLenIonFloat() {
        assertEquals(0, IonBinary.lenIonFloat(0.0));
        assertEquals(8, IonBinary.lenIonFloat(-0.0));
        assertEquals(8, IonBinary.lenIonFloat(1.0));
        assertEquals(8, IonBinary.lenIonFloat(-10));
        assertEquals(8, IonBinary.lenIonFloat(10.5));
        assertEquals(8, IonBinary.lenIonFloat(-10.5));
    }

    @Test
    public void testMakeUTF8IntFromScalar() throws IOException {
        assertEquals(0, IonBinary.makeUTF8IntFromScalar(0));
        assertEquals(-2139058444, IonBinary.makeUTF8IntFromScalar(1105920));
        assertEquals(32962, IonBinary.makeUTF8IntFromScalar(128));
        assertEquals(8421602, IonBinary.makeUTF8IntFromScalar(8192));
        assertEquals(8421614, IonBinary.makeUTF8IntFromScalar(57344));
    }

    @Test
    public void testLenIonInt() {
        assertEquals(0, IonBinary.lenIonInt(0L));
        assertEquals(2, IonBinary.lenIonInt(281L));
        assertEquals(2, IonBinary.lenIonInt(-12555L));
    }

    @Test
    public void testLenIonIntBigInteger() {
        assertEquals(0, IonBinary.lenIonInt(BigInteger.ZERO));
        assertEquals(1, IonBinary.lenIonInt(BigInteger.ONE));
        assertEquals(1, IonBinary.lenIonInt(new BigInteger("-1")));
        assertEquals(1, IonBinary.lenIonInt(BigInteger.TEN));
        assertEquals(1, IonBinary.lenIonInt(new BigInteger("-10")));
        assertEquals(3, IonBinary.lenIonInt(new BigInteger("250001")));
        assertEquals(3, IonBinary.lenIonInt(new BigInteger("-250001")));
    }

    @Test
    public void testLenIntLong() {
        assertEquals(0, IonBinary.lenInt(0L));
        assertEquals(1, IonBinary.lenInt(1L));
        assertEquals(2, IonBinary.lenInt(256L));
        assertEquals(3, IonBinary.lenInt(333333L));
        assertEquals(4, IonBinary.lenInt(345348572L));
        assertEquals(5, IonBinary.lenInt(123123879123L));
        assertEquals(6, IonBinary.lenInt(123812938102812L));
        assertEquals(7, IonBinary.lenInt(22349123712192372L));
        assertEquals(8, IonBinary.lenInt(123712387192831287L));
        assertEquals(9, IonBinary.lenInt(9223372036854775807L));
    }

    @Test
    public void testLenIntBigInteger(){
        assertEquals(0, IonBinary.lenInt(BigInteger.ZERO, false));
        assertEquals(1, IonBinary.lenInt(BigInteger.ZERO, true));
        assertEquals(1, IonBinary.lenInt(BigInteger.TEN, false));
        assertEquals(1, IonBinary.lenInt(BigInteger.TEN, true));
        assertEquals(1, IonBinary.lenInt(new BigInteger("-10"), false));
        assertEquals(1, IonBinary.lenInt(new BigInteger("-10"), true));
    }

    @Test
    public void testLenLenFieldWithOptionalNibble() {
        assertEquals(0, IonBinary.lenLenFieldWithOptionalNibble(0));
        assertEquals(2, IonBinary.lenLenFieldWithOptionalNibble(300));
    }
}
