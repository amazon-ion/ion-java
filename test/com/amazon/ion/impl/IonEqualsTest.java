package com.amazon.ion.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.amazon.ion.IonFloat;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.SystemFactory;
import org.junit.Before;
import org.junit.Test;

public class IonEqualsTest {

    private IonSystem m_ion = null;

    @Before
    public void setup() {
        m_ion = SystemFactory.newSystem();
    }

    private static void assertIonEq(final IonValue v1, final IonValue v2) {
        assertTrue(v1.equals(v2));
    }

    private static void assertNotIonEq(final IonValue v1, final IonValue v2) {
        assertFalse(v1.equals(v2));
    }

    private IonValue ion(final String raw) {
        return m_ion.singleValue(raw);
    }

    private IonFloat ionFloat(final double value) {
        IonFloat f = m_ion.newNullFloat();
        f.setValue(value);
        return f;
    }

    @Test
    public void testRealNull3() {
        assertNotIonEq(ion("null.null"), null);
    }

    @Test
    public void testEqualsNull1() {
        assertIonEq(ion("null"), ion("null"));
    }

    @Test
    public void testEqualsNull2() {
        assertIonEq(ion("null"), ion("null.null"));
    }

    @Test
    public void testEqualsNull3() {
        assertIonEq(ion("a1::null"), ion("a1::null.null"));
    }

    @Test
    public void testEqualsNull4() {
        assertIonEq(ion("a2::a1::null"), ion("a2::a1::null.null"));
    }

    @Test
    public void testNotEqualsNull1() {
        assertNotIonEq(ion("null"), ion("null.bool"));
    }

    @Test
    public void testNotEqualsNull2() {
        assertNotIonEq(ion("null"), ion("null.int"));
    }

    @Test
    public void testNotEqualsNull3() {
        assertNotIonEq(ion("null"), ion("null.float"));
    }

    @Test
    public void testNotEqualsNull4() {
        assertNotIonEq(ion("null"), ion("null.decimal"));
    }

    @Test
    public void testNotEqualsNull5() {
        assertNotIonEq(ion("null"), ion("null.timestamp"));
    }

    @Test
    public void testNotEqualsNull6() {
        assertNotIonEq(ion("null"), ion("null.string"));
    }

    @Test
    public void testNotEqualsNull7() {
        assertNotIonEq(ion("null"), ion("null.symbol"));
    }

    @Test
    public void testNotEqualsNull8() {
        assertNotIonEq(ion("null"), ion("null.clob"));
    }

    @Test
    public void testNotEqualsNull9() {
        assertNotIonEq(ion("a::b::null"), ion("b::a::null"));
    }

    @Test
    public void testEqualsList() {
        assertIonEq(ion("[1, 2, 3]"), ion("[1, 2, 3]"));
    }

    @Test
    public void testEqualsStruct1() {
        assertIonEq(ion("{ a : 1, b : 2 }"), ion("{ b : 2, a : 1 }"));
    }

    @Test
    public void testFloat1() {
        assertNotIonEq(ionFloat(Double.NaN), ionFloat(1.0d));
    }

    @Test
    public void testFloat2() {
        assertIonEq(ionFloat(1.0d), ionFloat(1.0d));
    }

    @Test
    public void testFloat3() {
        assertIonEq(ionFloat(1.1d), ion("1.1e0"));
    }

    @Test
    public void testFloatNaN1() {
        assertIonEq(ionFloat(Double.NaN), ionFloat(Double.NaN));
    }

    @Test
    public void testFloatNaN2() {
        assertIonEq(ionFloat(Double.NaN), ionFloat(Double
                .longBitsToDouble(0x7ff0000000034001L)));
    }

    @Test
    public void testFloatNaN3() {
        assertIonEq(ionFloat(Double.longBitsToDouble(0x7fffffffffffffffL)),
                ionFloat(Double.longBitsToDouble(0xffffffffffffff99L)));
    }

    @Test
    public void testFloatInf1() {
        assertIonEq(ionFloat(Double.POSITIVE_INFINITY), ionFloat(Double
                .longBitsToDouble(0x7ff0000000000000L)));
    }

    @Test
    public void testFloatInf2() {
        assertNotIonEq(ionFloat(Double.POSITIVE_INFINITY),
                ionFloat(Double.NEGATIVE_INFINITY));
    }

    @Test
    public void testFloatInf3() {
        assertIonEq(ionFloat(Double.NEGATIVE_INFINITY), ionFloat(Double
                .longBitsToDouble(0xfff0000000000000L)));
    }

    @Test
    public void testTimeStamp2() {
        assertNotIonEq(ion("2007-10-10"),
                       ion("2007-10-10T00:00:00.0000-00:00"));
    }

    @Test
    public void testTimeStamp3() {
        assertIonEq(ion("1950-01-01T07:30:23Z"),
                    ion("1950-01-01T07:30:23+00:00"));
    }

    @Test
    public void testTimeStamp4() {
        assertNotIonEq(ion("1950-01-01T07:30:23.01Z"),
                       ion("1950-01-01T07:30:23.010Z"));
    }

    @Test
    public void testSameFieldNameStruct7() {
        assertIonEq(ion("{a : 4, a : 4.0}"), ion("{a : 4.0, a : 4}"));
    }

    @Test
    public void testSameFieldNameStruct8() {
        assertIonEq(ion("{a : 4, a : 4.0}"), ion("{a : 4, a : 4.0}"));
    }

    @Test
    public void testSameFieldNameStruct9() {
        assertIonEq(ion("{a : 4, a : 4}"), ion("{a : 4, a : 4}"));
    }

    @Test
    public void testSameFieldNameStruct10() {
        assertNotIonEq(ion("{a : 4, a : a::4.0}"), ion("{a : 4, a : 4.0}"));
    }

    @Test
    public void testSameFieldNameStruct11() {
        assertIonEq(ion("{a : a::4.0, a : 4}"), ion("{a : 4, a : a::4.0}"));
    }

    @Test
    public void testSameFieldNameStruct12() {
        assertNotIonEq(ion("{a : 4, a : 4, a : 4}"), ion("{a : 4, a : 4}"));
    }

    @Test
    public void testSameFieldNameStruct13() {
        assertNotIonEq(ion("{a : 4, a : 4, b : 6}"),
                ion("{a : 4, b : 6, b : 6}"));
    }

    @Test
    public void testSexprEquals12() {
        assertNotIonEq(ion("(1 2 3)"), ion("(1 2 a::3)"));
    }

    @Test
    public void testSexprEquals13() {
        assertNotIonEq(ion("(1 2 3)"), ion("(1 2 3.0)"));
    }

    @Test
    public void testSexprEquals14() {
        assertNotIonEq(ion("(1 2.0e0 3)"), ion("(1 2.0 3)"));
    }

    @Test
    public void testSexprEquals15() {
        assertIonEq(ion("(((((((((((((((((((((())))))))))))))))))))))"),
                ion("(((((((((((((((((((((())))))))))))))))))))))"));
    }

}
