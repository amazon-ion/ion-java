/*
 *  Copyright 2009 Amazon.com.  All rights reserved.
 *  $Id: //brazil/src/shared/platform/IonJava/mainline/test/com/amazon/ion/HashCodeCorrectnessTest.java#1 $
 *  $Change: 2828295 $ $Author: pmarie $ $DateTime: 2009/12/09 00:40:53 $
 */

package com.amazon.ion;

import com.amazon.ion.util.Equivalence;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

/**
 * Test cases for Ion hashes
 */
public class HashCodeCorrectnessTest extends IonTestCase
{
    /**
     * Helper function to construct an integer.
     * @param i Value
     * @return IonInt with value i
     */
    protected IonInt integer(int i)  {
        return system().newInt(i);
    }

    /**
     * Helper function to construct a float.
     * @param value Floating point value
     * @return IonFloat with value
     */
    protected IonFloat ionFloat(double value)  {
        return system().newFloat(value);
    }

    /**
     * Helper function to construct an integer.
     * @param s String value
     * @return IonInt with value s
     */
    protected IonInt integer(String s)  {
        return (IonInt) oneValue(s);
    }

    /**
     * Verify that the relationship between equals() and hashCode() is
     * honored.
     * @param v1 IonValue 1
     * @param v2 IonValue 2
     */
    private static void assertIonEqImpliesHashEq(final IonValue v1,
                                                 final IonValue v2) {
        assertTrue(String.format("Contract between equals and"
                                 + " hashCode violated, v1 %1$s (hash %2$x),"
                                 + " v2 %3$s (hash %4$x)",
                                 v1, v1.hashCode(),
                                 v2, v2.hashCode()),
                   !Equivalence.ionEquals(v1, v2)
                   || v1.hashCode() == v1.hashCode());
    }

    /**
     * Test method for {@link IonLob#hashCode()}
     */
    public void testIonLobHashCode()
    {
        // Some clobs
        IonValue clob1 = oneValue("{{'''The quick brown fox '''\n"
                                  + "'''jumped over the lazy dog.'''}}");
        IonValue clob2 = oneValue("{{'''Jackdaws love my '''\n"
                                  + "'''big sphinx of quartz.'''}}");
        IonValue clob3 = oneValue("{{\"The quick brown fox "
                             + "jumped over the lazy dog.\"}}");
        IonValue clob4 = oneValue("{{\"Jackdaws love my "
                             + "big sphinx of quartz.\"}}");
        assertIonEqImpliesHashEq(clob1, clob1); // equal
        assertIonEqImpliesHashEq(clob1, clob2); // not eq
        assertIonEqImpliesHashEq(clob1, clob3); // equal
        assertIonEqImpliesHashEq(clob1, clob4); // not eq
        assertIonEqImpliesHashEq(clob2, clob2); // equal
        assertIonEqImpliesHashEq(clob2, clob3); // not eq
        assertIonEqImpliesHashEq(clob2, clob4); // equal
        assertIonEqImpliesHashEq(clob3, clob3); // equal
        assertIonEqImpliesHashEq(clob3, clob4); // not eq
        assertIonEqImpliesHashEq(clob4, clob4); // equal
        // Some blobs
        IonValue blob1 = oneValue("{{ VGhlIHF1aWNrIGJyb3duIGZveCBqd"
                                  + "W1wZWQgb3ZlciB0aGUgbGF6eSBkb2cu}}");
        IonValue blob2 = oneValue("{{ SmFja2Rhd3MgbG92ZSBteSBiaWcgc"
                                  + "3BoaW54IG9mIHF1YXJ0ei4=}}");
        IonValue blob3 = oneValue("{{ VGhlIHF1aWNrIGJyb3duIGZveCBqd\n"
                                  + " W1wZWQgb3ZlciB0aGUgbGF6eSBkb2cu}}");
        IonValue blob4 = oneValue("{{ SmFja2Rhd3MgbG92ZSBteSBiaWcgc\n"
                                  + " 3BoaW54IG9mIHF1YXJ0ei4=}}");
        assertIonEqImpliesHashEq(blob1, blob1);
        assertIonEqImpliesHashEq(blob1, blob2);
        assertIonEqImpliesHashEq(blob1, blob3);
        assertIonEqImpliesHashEq(blob1, blob4);
        assertIonEqImpliesHashEq(blob2, blob2);
        assertIonEqImpliesHashEq(blob2, blob3);
        assertIonEqImpliesHashEq(blob2, blob4);
        assertIonEqImpliesHashEq(blob3, blob3);
        assertIonEqImpliesHashEq(blob3, blob4);
        assertIonEqImpliesHashEq(blob4, blob4);
        // And a mashup
        assertIonEqImpliesHashEq(blob1, clob1);
        assertIonEqImpliesHashEq(blob1, clob2);
        assertIonEqImpliesHashEq(clob1, blob3);
        assertIonEqImpliesHashEq(clob1, blob4);
        assertIonEqImpliesHashEq(blob2, clob2);
        assertIonEqImpliesHashEq(clob2, blob3);
        assertIonEqImpliesHashEq(blob2, clob4);
        assertIonEqImpliesHashEq(blob3, clob3);
        assertIonEqImpliesHashEq(blob3, clob4);
        assertIonEqImpliesHashEq(clob4, blob4);
    }

    /**
     * Test method for {@link IonBool#hashCode()}.
     */
    public void testIonBoolHashCode()
    {
        IonValue[] bools = {oneValue("true"), oneValue("true"),
                            oneValue("false"), oneValue("false")};
        for (int i = 0; i < bools.length; ++i)  {
            for (int j = i; j < bools.length; ++j)  {
                assertIonEqImpliesHashEq(bools[i], bools[j]);
            }
        }
    }

    /**
     * Test method for {@link IonInt#hashCode()}.
     */
    public void testIonIntHashCode()
    {
        Set<IonInt> unique_values = new HashSet<IonInt>();
        Set<Integer> unique_hashes = new HashSet<Integer>();
        for (int i = -100 ; i < 100; i += 7)  {
            IonInt vi1 = integer(i);
            IonInt vi2 = integer(i);
            unique_values.add(vi1);
            unique_values.add(vi2);
            unique_hashes.add(vi1.hashCode());
            unique_hashes.add(vi2.hashCode());
            for (int j = -100;  j < 100; j += 5) {
                IonInt vj1 = integer(j);
                IonInt vj2 = integer(j);
                unique_values.add(vj1);
                unique_values.add(vj2);
                unique_hashes.add(vj1.hashCode());
                unique_hashes.add(vj2.hashCode());
                assertIonEqImpliesHashEq(vi1, vi1);
                assertIonEqImpliesHashEq(vi1, vi2);
                assertIonEqImpliesHashEq(vi1, vj1);
                assertIonEqImpliesHashEq(vi1, vj2);
                assertIonEqImpliesHashEq(vi2, vi2);
                assertIonEqImpliesHashEq(vi2, vj1);
                assertIonEqImpliesHashEq(vi2, vj2);
                assertIonEqImpliesHashEq(vj1, vj1);
                assertIonEqImpliesHashEq(vj1, vj2);
                assertIonEqImpliesHashEq(vj2, vj2);
            }
        }
        assertEquals("All distinct values have distinct hashes",
                     unique_values.size(),
                     unique_hashes.size());
    }

    /**
     * Test method for {@link IonDecimal#hashCode()}.
     */
    @Test
    public void testIonDecimalHashCode()
    {
        IonDecimal[] decimals = {decimal("-0.00"), decimal("-0."),
                                 decimal("0."), decimal("0.0000"),
                                 decimal("3.1415926"),
                                 decimal("-3.1415926"),
                                 decimal("3.1415926000"),
                                 decimal("3.1415926001"),
                                 decimal("100.0001"),
                                 decimal("100.00010"),
                                 decimal("100.00010001"),};
        // IonDecimal's are hashed as doubles, so the unique values don't
        // include all the possible precision
        Set<Double> unique_values = new HashSet<Double>();
        Set<Integer> unique_hashes = new HashSet<Integer>();
        for (int i = 0; i < decimals.length; ++i)  {
            unique_values.add(((IonDecimal) decimals[i]).doubleValue());
            unique_hashes.add(decimals[i].hashCode());
            for (int j = i; j < decimals.length; ++j)  {
                assertIonEqImpliesHashEq(decimals[i], decimals[j]);
            }
        }
        assertEquals("All distinct values have distinct hashes",
                     unique_values.size(),
                     unique_hashes.size());
    }

    /**
     * Test method for {@link IonFloat#hashCode()}.
     */
    public void testIonFloatHashCode()
    {
        double foo = 2.17845643324e23;
        double bar = foo + Math.ulp(foo);
        double bletch = bar + Math.ulp(bar);
        IonValue[] floats = {ionFloat(-0.00), ionFloat(-0),
                             ionFloat(0), ionFloat(0.0000),
                             ionFloat(3.1415926),
                             ionFloat(+3.1415926),
                             ionFloat(-3.1415926),
                             ionFloat(3.1415926000),
                             ionFloat(3.1415926001),
                             ionFloat(100.0001),
                             ionFloat(100.00010),
                             ionFloat(100.00010001),
                             ionFloat(Double.MIN_VALUE),
                             ionFloat(Double.MAX_VALUE),
                             ionFloat(-Double.MIN_VALUE),
                             ionFloat(-Double.MAX_VALUE),
                             ionFloat(foo), ionFloat(bar), ionFloat(bletch),
                             ionFloat(foo), ionFloat(bar), ionFloat(bletch)};
        Set<IonValue> unique_values = new HashSet<IonValue>();
        Set<Integer> unique_hashes = new HashSet<Integer>();
        for (int i = 0; i < floats.length; ++i)  {
            unique_values.add(floats[i]);
            unique_hashes.add(floats[i].hashCode());
            for (int j = i; j < floats.length; ++j)  {
                assertIonEqImpliesHashEq(floats[i], floats[j]);
            }
        }
        assertEquals("All distinct values have distinct hashes",
                     unique_values.size(),
                     unique_hashes.size());
    }

    /**
     * Test method for {@link IonTimestamp#hashCode()}.
     */
    @Test
    public void testIonTimestampHashCode()
    {
        IonValue[] timestamps = {oneValue("2007-02-23"),
                                 oneValue("2007-02-23T12:14Z"),
                                 oneValue("2007-02-23T12:14:33.079-08:00"),
                                 oneValue("2007-02-23T20:14:33.079Z"),
                                 oneValue("2007-02-23T20:14:33.079+00:00"),
                                 oneValue("0001-01-01"),
                                 oneValue("0001-01-01T12:14Z"),
                                 oneValue("0001-01-01T12:14:33.079-08:00"),
                                 oneValue("0001-01-01T20:14:33.079Z"),
                                 oneValue("0001-01-01T20:14:33.079+00:00"),
                                 oneValue("1969-02-23"),
                                 oneValue("1969-02-23T"),
                                 oneValue("1969-02-23T00:00-00:00"),
                                 oneValue("1969-02-23T00:00:00-00:00"),
                                 oneValue("1969-02-23T00:00:00.00-00:00"),
                                 oneValue("9999-01-01"),
                                 oneValue("9999-12-31T12:14Z"),
                                 oneValue("9999-12-31T12:14:33.079-08:00"),
                                 oneValue("9999-12-31T20:14:33.079Z"),
                                 oneValue("9999-12-31T20:14:33.079+00:00")};
        Set<IonValue> unique_values = new HashSet<IonValue>();
        Set<Integer> unique_hashes = new HashSet<Integer>();
        for (int i = 0; i < timestamps.length; ++i)  {
            unique_values.add(timestamps[i]);
            unique_hashes.add(timestamps[i].hashCode());
            for (int j = i; j < timestamps.length; ++j)  {
                assertIonEqImpliesHashEq(timestamps[i], timestamps[j]);
            }
        }
        assertEquals("All distinct values have distinct hashes",
                     unique_values.size(),
                     unique_hashes.size());
    }

    /**
     * Test method for {@link IonText#hashCode()}.
     */
    @Test
    public void testIonTextHashCode()
    {
        IonValue[] text = {
                oneValue("foo"), oneValue("\"foo\""),
                oneValue("bar"), oneValue("\"bar\""),
                oneValue("baz"), oneValue("\"baz\""),
                oneValue("qux"), oneValue("\"qux\""),
                oneValue("quux"), oneValue("\"quux\""),
                oneValue("quuux"), oneValue("\"quuux\""),
                oneValue("foo"), oneValue("\"foo\""),
                oneValue("bar"), oneValue("\"bar\""),
                oneValue("baz"), oneValue("\"baz\""),
                oneValue("qux"), oneValue("\"qux\""),
                oneValue("quux"), oneValue("\"quux\""),
                oneValue("quuux"), oneValue("\"quuux\""),
                oneValue("\"Lorem ipsum dolor sit amet, consectetur adipiscing elit\""),
                oneValue("\"Mauris sodales scelerisque dui et ultricies\""),
                oneValue("\"In hac habitasse platea dictumst\""),
                oneValue("\"Proin tincidunt pretium consectetur\""),
                oneValue("\"Vivamus consectetur ligula sit amet turpis bibendum"
                    + " rutrum blandit magna adipiscing\""),
                oneValue("\"Proin erat est, rutrum non elementum vitae, fermentum quis ligula\""),
                oneValue("\"Quisque vitae tortor lectus, aliquam molestie enim\""),
                oneValue("\"Donec elementum malesuada ligula id rutrum\""),
                oneValue("\"Aliquam porta dignissim cursus\""),
                oneValue("\"Suspendisse egestas molestie lacinia\""),
                oneValue("\"Suspendisse fermentum lacus at velit dignissim vel iaculis ante cursus\""),
                oneValue("\"Sed bibendum molestie purus in vestibulum\""),
                oneValue("\"Nulla ipsum augue, pretium in suscipit ut, auctor quis ligula\""),
                oneValue("\"Integer suscipit dignissim enim eget luctus.\""),
                oneValue("\"Quisque tempor ligula eget nibh porttitor porta tincidunt ante placerat\""),
                oneValue("\"Maecenas tempor vehicula eleifend\""),
                oneValue("\"Suspendisse eu tincidunt quam\""),
                oneValue("\"Duis condimentum, est vulputate commodo"
                    + " condimentum, metus mi ultrices arcu, nec"
                    + " commodo ligula neque sit amet ante\""),
                oneValue("\"Sed a sem sit amet elit interdum dignissim sit amet vitae dui\""),
                oneValue("\"Proin suscipit fringilla consequat\""),
                oneValue("\"Donec eget libero augue\""),
                oneValue("\"Aliquam erat volutpat\""),
                oneValue("\"Cras euismod mattis tellus lobortis facilisis\""),
                oneValue("\"Praesent condimentum volutpat odio id rutrum.\""),
                oneValue("Lorem"), oneValue("ipsum"), oneValue("dolor"), oneValue("sit"),
                oneValue("amet"), oneValue("consectetur"), oneValue("adipiscing"),
                oneValue("elit"), oneValue("Mauris"), oneValue("sodales"),
                oneValue("scelerisque"), oneValue("dui"), oneValue("et"), oneValue("ultricies"),
                oneValue("In"), oneValue("hac"), oneValue("habitasse"), oneValue("platea"),
                oneValue("dictumst"), oneValue("Proin"), oneValue("tincidunt"), oneValue("pretium"),
                oneValue("consectetur"), oneValue("Vivamus"), oneValue("consectetur"),
                oneValue("ligula"), oneValue("sit"), oneValue("amet"), oneValue("turpis"),
                oneValue("bibendum"), oneValue("rutrum"), oneValue("blandit"), oneValue("magna"),
                oneValue("adipiscing"), oneValue("Proin"), oneValue("erat"), oneValue("est"),
                oneValue("rutrum"), oneValue("non"), oneValue("elementum"), oneValue("vitae"),
                oneValue("fermentum"), oneValue("quis"), oneValue("ligula"), oneValue("Quisque"),
                oneValue("vitae"), oneValue("tortor"), oneValue("lectus"), oneValue("aliquam"),
                oneValue("molestie"), oneValue("enim"), oneValue("Donec"), oneValue("elementum"),
                oneValue("malesuada"), oneValue("ligula"), oneValue("id"), oneValue("rutrum"),
                oneValue("Aliquam"), oneValue("porta"), oneValue("dignissim"), oneValue("cursus"),
                oneValue("Suspendisse"), oneValue("egestas"), oneValue("molestie"),
                oneValue("lacinia"), oneValue("Suspendisse"), oneValue("fermentum"),
                oneValue("lacus"), oneValue("at"), oneValue("velit"), oneValue("dignissim"),
                oneValue("vel"), oneValue("iaculis"), oneValue("ante"), oneValue("cursus"),
                oneValue("Sed"), oneValue("bibendum"), oneValue("molestie"), oneValue("purus"),
                oneValue("in"), oneValue("vestibulum"), oneValue("Nulla"), oneValue("ipsum"),
                oneValue("augue"), oneValue("pretium"), oneValue("in"), oneValue("suscipit"),
                oneValue("ut"), oneValue("auctor"), oneValue("quis"), oneValue("ligula"),
                oneValue("Integer"), oneValue("suscipit"), oneValue("dignissim"), oneValue("enim"),
                oneValue("eget"), oneValue("luctus"),};
        Set<IonValue> unique_values = new HashSet<IonValue>();
        Set<Integer> unique_hashes = new HashSet<Integer>();
        for (int i = 0; i < text.length; ++i)  {
            unique_values.add(text[i]);
            unique_hashes.add(text[i].hashCode());
            for (int j = i; j < text.length; ++j)  {
                assertIonEqImpliesHashEq(text[i], text[j]);
            }
        }
        assertEquals("All distinct values have distinct hashes",
                     unique_values.size(),
                     unique_hashes.size());
    }

    /**
     * Test method for {@link IonSequence#hashCode()}.
     */
    @Test
    public void testIonSequenceHashCode()
    {
        IonValue[] sequences = {oneValue("[a,b,c]"), oneValue("(a b c)"),
                                oneValue("[b,c,a]"), oneValue("(b c a)"),
                                oneValue("[1,2,3]"), oneValue("(1 2 3)"),
                                oneValue("[997,345,234]"), oneValue("(997 345 234)"),
                                oneValue("[\"a\",\"b\",\"c\"]"),
                                oneValue("(\"a\" \"b\" \"c\")"),
                                oneValue("[\"b\",\"c\",\"a\"]"),
                                oneValue("(\"b\" \"c\" \"a\")"),
                                oneValue("[2007-02-23T12:14:33.079-08:00,"
                                    + "2007-02-23T20:14:33.079Z,"
                                    + "2007-02-23T20:14:33.079+00:00]"),
                                oneValue("(2007-02-23T12:14:33.079-08:00 "
                                    + "2007-02-23T20:14:33.079Z "
                                    + "2007-02-23T20:14:33.079+00:00)"),
                                oneValue("[a,b,c]"), oneValue("(a b c)"),
                                oneValue("[b,c,a]"), oneValue("(b c a)"),
                                oneValue("[1,2,3]"), oneValue("(1 2 3)"),
                                oneValue("[997,345,234]"), oneValue("(997 345 234)"),
                                oneValue("[\"a\",\"b\",\"c\"]"),
                                oneValue("(\"a\" \"b\" \"c\")"),
                                oneValue("[\"b\",\"c\",\"a\"]"),
                                oneValue("(\"b\" \"c\" \"a\")"),
                                oneValue("[2007-02-23T12:14:33.079-08:00,"
                                    + "2007-02-23T20:14:33.079Z,"
                                    + "2007-02-23T20:14:33.079+00:00]")};
        Set<IonValue> unique_values = new HashSet<IonValue>();
        Set<Integer> unique_hashes = new HashSet<Integer>();
        for (int i = 0; i < sequences.length; ++i)  {
            unique_values.add(sequences[i]);
            unique_hashes.add(sequences[i].hashCode());
            for (int j = i; j < sequences.length; ++j)  {
                assertIonEqImpliesHashEq(sequences[i], sequences[j]);
            }
        }
        assertEquals("All distinct values have distinct hashes",
                     unique_values.size(),
                     unique_hashes.size());
    }

    /**
     * Test method for {@link IonStruct#hashCode()}.
     */
    @Test
    public void testIonStructHashCode()
    {
        IonValue[] structs = {oneValue("{a:x,b:y,c:z}"),
                              oneValue("{s:(a b c)}"),
                              oneValue("{a:x,b:y,c:z}"),
                              oneValue("{a:x,c:z,b:y}"),
                              oneValue("{b:y,c:z,a:x}"),
                              oneValue("{b:y,a:x,c:z}"),
                              oneValue("{c:z,a:x,b:y}"),
                              oneValue("{c:z,b:y,a:x}"),
                              oneValue("{x:{a:x,b:y,c:z},"
                                  + "y:{a:x,c:z,b:y},"
                                  + "z:{b:y,c:z,a:x}}"),
                              oneValue("{y:{a:x,b:y,c:z},"
                                  + "z:{a:x,c:z,b:y},"
                                  + "x:{b:y,c:z,a:x}}"),
                              oneValue("{z:{a:x,b:y,c:z},"
                                  + "x:{a:x,c:z,b:y},"
                                  + "y:{b:y,c:z,a:x}}"),
                              oneValue("{x:{a:x,b:y,c:z},"
                                  + "z:{b:y,c:z,a:x},"
                                  + "y:{a:x,c:z,b:y}}"),
                              oneValue("{y:{a:x,b:y,c:z},"
                                  + "x:{b:y,c:z,a:x},"
                                  + "z:{a:x,c:z,b:y}}"),
                              oneValue("{x:{a:x,c:z,b:y},"
                                  + "z:{a:x,b:y,c:z},"
                                  + "y:{b:y,c:z,a:x}}"),
                              oneValue("{s:(a b c)}"),
                              oneValue("{foo:[b,c,a], bar:(b c a)}"),
                              oneValue("{x:\"a\",y:\"b\",z:\"c\"}"),
                              oneValue("{x:\"a\",y:\"b\",z:\"c\",z:\"d\"}"),
                              oneValue("{created:2007-02-23T12:14:33.079-08:00,"
                                  + "expires:2009-02-28T00:00:00.000Z}")};
        Set<IonValue> unique_values = new HashSet<IonValue>();
        Set<Integer> unique_hashes = new HashSet<Integer>();
        for (int i = 0; i < structs.length; ++i)  {
            unique_values.add(structs[i]);
            unique_hashes.add(structs[i].hashCode());
            for (int j = i; j < structs.length; ++j)  {
                assertIonEqImpliesHashEq(structs[i], structs[j]);
            }
        }
        assertEquals("All distinct values have distinct hashes",
                     unique_values.size(),
                     unique_hashes.size());
    }
}
