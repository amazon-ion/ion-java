/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion.util;

import com.amazon.ion.IonException;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonList;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonValue;
import org.junit.Ignore;
import org.junit.Test;

public class EquivalenceTest
    extends IonTestCase
{
    private static void assertIonEq(final IonValue left, final IonValue right) {
        assertTrue(Equivalence.ionEquals(left, right));
        assertTrue(Equivalence.ionEquals(right, left));
        Equivalence equivalence = new Equivalence.Builder().build();
        assertTrue(equivalence.ionValueEquals(left, right));
        assertTrue(equivalence.ionValueEquals(right, left));

        // Redundancy check included here, in the case that IonValue#equals()
        // doesn't use Equivalence's implementation anymore.
        if (left != null && right != null) {
            assertEquals(left, right);
            assertEquals(right, left);
        }
    }

    private static void assertNotIonEq(final IonValue left, final IonValue right) {
        assertFalse(Equivalence.ionEquals(left, right));
        assertFalse(Equivalence.ionEquals(right, left));
        Equivalence equivalence = new Equivalence.Builder().build();
        assertFalse(equivalence.ionValueEquals(left, right));
        assertFalse(equivalence.ionValueEquals(right, left));

        // Redundancy check included here, in the case that IonValue#equals()
        // doesn't use Equivalence's implementation anymore.
        if (left != null && right != null) {
            assertFalse(left.equals(right));
            assertFalse(right.equals(left));
        }
    }

    private static void assertIonEqForm(final IonValue left, final IonValue right) {
        assertTrue(Equivalence.ionEqualsByContent(left, right));
        assertTrue(Equivalence.ionEqualsByContent(right, left));
        Equivalence equivalence = new Equivalence.Builder().withStrict(false).build();
        assertTrue(equivalence.ionValueEquals(left, right));
        assertTrue(equivalence.ionValueEquals(right, left));
    }

    private IonValue ion(final String raw) {
        return system().singleValue(raw);
    }

    private IonFloat ionFloat(final double value) {
        return system().newFloat(value);
    }

    @Test
    public void testRealNull1() {
        assertIonEq(null, null);
    }

    @Test
    public void testRealNull2() {
        assertNotIonEq(null, ion("null"));
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
    public void testEqualsFormNull1() {
        assertIonEqForm(ion("null"), ion("b::a::null"));
    }

    @Test
    public void testEqualsFormNull2() {
        assertIonEqForm(ion("a::b::null"), ion("b::a::null"));
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
    public void testEqualsStruct2() {
        assertNotIonEq(ion("{ a : 1, b : 2 }"), ion("{ b : 2 }"));
    }

    @Test
    public void testEqualsStruct3() {
        assertNotIonEq(ion("{ a : 1, b : 2 }"), ion("{}"));
    }

    @Test
    public void testEqualsStruct4() {
        assertNotIonEq(ion("{ a : 1, b : 2 }"), ion("{ a : 1, c : 2 }"));
    }

    @Test
    public void testEqualsStruct5() {
        assertIonEqForm(ion("{ a : a::1, b : 2 }"), ion("{ a : 1, b : 2 }"));
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

    @Test
    public void testLob1() {
        assertIonEq(ion("{{aGVsbG8=}}"),
                    ion("{{aGVsbG8=}}"));
    }

    @Test
    public void testLob2() {
        assertNotIonEq(ion("{{aGVsbG8=}}"),
                       ion("{{aGVibG8=}}"));
    }

    @Test
    public void testLob3() {
        assertNotIonEq(ion("{{Z29vZGJ5ZQ==}}"),
                       ion("{{aGVibG8=}}"));
    }

    @Test
    public void testLob4() {
        assertIonEq(ion("{{'''Hello'''}}"),
                    ion("{{'''Hello'''}}"));
    }

    @Test
    public void testLob5() {
        assertNotIonEq(ion("{{'''Hello'''}}"),
                       ion("{{'''Hello World'''}}"));
    }

    @Test
    public void testLob6() {
        assertIonEq(ion("{{}}"),
                    ion("{{}}"));
    }

    @Test
    public void testString1() {
        assertIonEq(ion("\"hi\""), ion("'''hi'''"));
    }

    @Test
    public void testString2() {
        assertNotIonEq(ion("\"hi\""), ion("'''Hi'''"));
    }

    @Test
    public void testStringSymbol() {
        assertNotIonEq(ion("\"hi\""), ion("'hi'"));
    }

    // TODO amazon-ion/ion-java/issues/58 : Remove the ignore annotation from this test after
    // making the required changes to Equivalence.Field.hashCode.
    @Ignore
    @Test
    public void testFieldEquals1() {
        IonValue v1 = oneValue("1");
        IonValue v2 = oneValue("2");
        IonValue v3 = oneValue("3");

        IonStruct struct = system().newEmptyStruct();
        String fieldName = "a";
        struct.add(fieldName, v1);
        struct.add(fieldName, v2);
        struct.add(fieldName, v3);

        assertEquals(3, struct.size());

        Equivalence.Configuration configuration = new Equivalence.Configuration(new Equivalence.Builder().withStrict(true));
        Equivalence.Field f1 = new Equivalence.Field(v1, configuration, 0);
        Equivalence.Field f2 = new Equivalence.Field(v2, configuration, 0);
        Equivalence.Field f3 = new Equivalence.Field(v3, configuration, 0);

        assertFalse(f1.equals(f2));
        assertFalse(f1.equals(f3));
        assertFalse(f2.equals(f3));

        // Simple test that hashCode() is not badly implemented, so that fields with same
        // field names but are not equals() do not result in same hash codes.
        assertTrue(f1.hashCode() != f2.hashCode());
        assertTrue(f1.hashCode() != f3.hashCode());
    }

    // TODO amazon-ion/ion-java/issues/58 : Remove the ignore annotation from this test after
    // making the required changes to Equivalence.Field.hashCode.
    @Ignore
    @Test
    public void testFieldEquals2() {
        String intOne = "1";
        IonValue v1 = oneValue(intOne);
        IonValue v2 = oneValue(intOne);
        IonValue v3 = oneValue(intOne);

        IonStruct struct = system().newEmptyStruct();
        String fieldName = "a";
        struct.add(fieldName, v1);
        struct.add(fieldName, v2);
        struct.add(fieldName, v3);

        assertEquals(3, struct.size());

        Equivalence.Configuration configuration = new Equivalence.Configuration(new Equivalence.Builder().withStrict(true));
        Equivalence.Field f1 = new Equivalence.Field(v1, configuration, 0);
        Equivalence.Field f2 = new Equivalence.Field(v2, configuration, 0);
        Equivalence.Field f3 = new Equivalence.Field(v3, configuration, 0);

        assertEquals(f1, f2);
        assertEquals(f2, f1); // symmetric

        assertEquals(f1, f3);
        assertEquals(f3, f1); // symmetric

        assertEquals(f2, f3); // transitive
        assertEquals(f3, f2); // symmetric
    }

    @Test
    public void builderWithoutEpsilon() {
        Equivalence equivalence = new Equivalence.Builder().build();
        IonFloat v1 = ionFloat(3.14);
        IonFloat v2 = ionFloat(3.14 + 1e-7);
        assertFalse(equivalence.ionValueEquals(v1, v2));
        assertFalse(equivalence.ionValueEquals(v2, v1));
        IonStruct struct1 = system().newEmptyStruct();
        struct1.add("foo", v1.clone());
        IonStruct struct2 = system().newEmptyStruct();
        struct2.add("foo", v2.clone());
        assertFalse(equivalence.ionValueEquals(struct1, struct2));
        assertFalse(equivalence.ionValueEquals(struct2, struct1));
        IonList list1 = system().newList(v1.clone());
        IonList list2 = system().newList(v2.clone());
        assertFalse(equivalence.ionValueEquals(list1, list2));
        assertFalse(equivalence.ionValueEquals(list2, list1));
    }

    @Test
    public void builderWithEpsilon() {
        Equivalence equivalence = new Equivalence.Builder().withEpsilon(1e-6).build();
        IonFloat v1 = ionFloat(3.14);
        IonFloat v2 = ionFloat(3.14 + 1e-7);
        assertTrue(equivalence.ionValueEquals(v1, v2));
        assertTrue(equivalence.ionValueEquals(v2, v1));
        IonStruct struct1 = system().newEmptyStruct();
        struct1.add("foo", v1.clone());
        IonStruct struct2 = system().newEmptyStruct();
        struct2.add("foo", v2.clone());
        assertTrue(equivalence.ionValueEquals(struct1, struct2));
        assertTrue(equivalence.ionValueEquals(struct2, struct1));
        IonList list1 = system().newList(v1.clone());
        IonList list2 = system().newList(v2.clone());
        assertTrue(equivalence.ionValueEquals(list1, list2));
        assertTrue(equivalence.ionValueEquals(list2, list1));
    }

    @Test
    public void maximumDepthCannotBeNegative() {
        Equivalence.Builder builder = new Equivalence.Builder();
        assertThrows(IllegalArgumentException.class, () -> builder.withMaxComparisonDepth(-1));
    }

    @Test
    public void maximumDepthExceeded() {
        Equivalence equivalence = new Equivalence.Builder().withMaxComparisonDepth(3).build();
        IonStruct struct1 = (IonStruct) system().singleValue("{foo: {bar: {baz: {zar: 123}}}}");
        IonStruct struct2 = struct1.clone();
        assertThrows(IonException.class, () -> equivalence.ionValueEquals(struct1, struct2));
    }

    @Test
    public void maximumDepthNotExceeded() {
        Equivalence equivalence = new Equivalence.Builder().withMaxComparisonDepth(4).build();
        IonStruct struct1 = (IonStruct) system().singleValue("{foo: {bar: {baz: {zar: 123}}}}");
        IonStruct struct2 = struct1.clone();
        assertTrue(equivalence.ionValueEquals(struct1, struct2));
    }
}
