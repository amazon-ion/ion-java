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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.Test;
import software.amazon.ion.ContainedValueException;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonSexp;
import software.amazon.ion.IonSymbol;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;



public class SexpTest
    extends TrueSequenceTestCase
{
    @Override
    protected IonSexp makeNull()
    {
        return system().newNullSexp();
    }

    @Override
    protected IonSexp makeEmpty()
    {
        return system().newEmptySexp();
    }

    @Override
    protected IonSexp newSequence(Collection<? extends IonValue> children)
    {
        IonSexp sexp = system().newEmptySexp();
        sexp.addAll(children);
        return sexp;
    }

    @Override
    protected <T extends IonValue> IonSexp newSequence(T... elements)
    {
        return system().newSexp(elements);
    }


    @Override
    protected String wrap(String... children)
    {
        StringBuilder buf = new StringBuilder();
        buf.append('(');
        if (children != null)
        {
            for (String child : children)
            {
                buf.append(child);
                buf.append(' ');
            }
        }
        buf.append(')');
        return buf.toString();
    }

    @Override
    protected IonSexp wrapAndParse(String... children)
    {
        String text = wrap(children);
        return (IonSexp) system().singleValue(text);
    }

    @Override
    protected IonSexp wrap(IonValue... children)
    {
        return system().newSexp(children);
    }

    //=========================================================================
    // Test cases

    @Test
    public void testFactoryNullSexp()
    {
        IonSexp value = system().newNullSexp();
        assertSame(IonType.SEXP, value.getType());
        assertNull(value.getContainer());
        testFreshNullSequence(value);
    }

    @Test
    public void testTextNullSexp()
    {
        IonSexp value = (IonSexp) oneValue("null.sexp");
        assertSame(IonType.SEXP, value.getType());
        testFreshNullSequence(value);
    }

    @Test
    public void testMakeNullSexp()
    {
        IonSexp value = (IonSexp) oneValue("(foo+bar)");
        assertSame(IonType.SEXP, value.getType());
        assertFalse(value.isNullValue());
        value.makeNull();
        testFreshNullSequence(value);
    }

    @Test
    public void testClearNonMaterializedSexp()
    {
        IonSexp value = (IonSexp) oneValue("(foo+bar)");
        testClearContainer(value);
    }

    @Test
    public void testEmptySexp()
    {
        IonSexp value = (IonSexp) oneValue("()");
        assertSame(IonType.SEXP, value.getType());
        testEmptySequence(value);
    }

    @Test
    public void testGetTwiceReturnsSame()
    {
        IonSexp value = (IonSexp) oneValue("(a b)");
        IonValue elt1 = value.get(1);
        IonValue elt2 = value.get(1);
        assertSame(elt1, elt2);
    }


    @Test
    public void testTrickyParsing()
        throws Exception
    {
        IonSexp value = (IonSexp) oneValue("(a+b::c)");
        checkSymbol("a", value.get(0));
        checkSymbol("+", value.get(1));
        IonSymbol val3 = (IonSymbol) value.get(2);
        checkAnnotation("b", val3);
        checkSymbol("c", val3);
        assertEquals(3, value.size());
    }

    @Test
    public void testNegativeNumbersInSexp()
        throws Exception
    {
        IonSexp value = (IonSexp) oneValue("(-1)");
        checkInt(-1, value.get(0));

        value = (IonSexp) oneValue("(--1)");
        checkSymbol("--", value.get(0));
        checkInt(1, value.get(1));

        value = (IonSexp) oneValue("(a-1)");
        checkSymbol("a", value.get(0));
        checkInt(-1, value.get(1));
    }

    /** Ensure that triple-quote concatenation works inside sexp. */
    @Test
    public void testConcatenation()
    {
        IonSexp value = (IonSexp) oneValue("(a '''a''' '''b''' \"c\")");
        checkSymbol("a",  value.get(0));
        checkString("ab", value.get(1));
        checkString("c",  value.get(2));
        assertEquals(3, value.size());
    }

    @Test
    public void testSexpIteratorRemove()
    {
        IonSexp value = (IonSexp) oneValue("(a b c)");
        testIteratorRemove(value);
    }

    @Test
    public void testCreatingNullSexp()
    {
        IonSexp sexp1 = system().newNullSexp();
        IonValue sexp2 = reload(sexp1);

        // FIXME ensure sexp1._isPositionLoaded && _isMaterialized
        assertEquals(sexp1, sexp2);
    }

    @Test
    public void testCreatingSexpFromCollection()
    {
        IonSystem system = system();
        List<IonValue> elements = null;

        IonSexp v;
        try {
            v = newSequence(elements);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) {}

        elements = new ArrayList<IonValue>();
        v = newSequence(elements);
        testEmptySequence(v);

        elements.add(system.newString("hi"));
        elements.add(system.newInt(1776));
        v = newSequence(elements);
        assertEquals(2, v.size());
        checkString("hi", v.get(0));
        checkInt(1776, v.get(1));

        try {
            v = newSequence(elements);
            fail("Expected ContainedValueException");
        }
        catch (ContainedValueException e) { }

        elements = new ArrayList<IonValue>();
        elements.add(system.newInt(1776));
        elements.add(null);
        try {
            v = newSequence(elements);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) { }
    }

    @Test
    public void testCreatingSexpFromIntArray()
    {
        IonSystem system = system();
        int[] elements = null;

        IonSexp v = system.newSexp(elements);
        testFreshNullSequence(v);

        elements = new int[0];
        v = system.newSexp(elements);
        testEmptySequence(v);

        elements = new int[]{ 12, 13, 14 };
        v = system.newSexp(elements);
        assertEquals(3, v.size());
        checkInt(12, v.get(0));
        checkInt(13, v.get(1));
        checkInt(14, v.get(2));
    }

    @Test
    public void testCreatingSexpFromLongArray()
    {
        IonSystem system = system();
        long[] elements = null;

        IonSexp v = system.newSexp(elements);
        testFreshNullSequence(v);

        elements = new long[0];
        v = system.newSexp(elements);
        testEmptySequence(v);

        elements = new long[]{ 12, 13, 14 };
        v = system.newSexp(elements);
        assertEquals(3, v.size());
        checkInt(12, v.get(0));
        checkInt(13, v.get(1));
        checkInt(14, v.get(2));
    }

    @Test
    public void testCreatingSexpFromValueArray()
    {
        IonSystem system = system();
        IonValue[] elements = null;

        IonSexp v = system.newSexp(elements);
        testFreshNullSequence(v);

        elements = new IonValue[0];
        v = system.newSexp(elements);
        testEmptySequence(v);

        elements = new IonValue[]{ system.newInt(12), system.newString("hi") };
        v = system.newSexp(elements);
        assertEquals(2, v.size());
        checkInt(12, v.get(0));
        checkString("hi", v.get(1));

        try {
            v = system.newSexp(elements);
            fail("Expected ContainedValueException");
        }
        catch (ContainedValueException e) { }

        // varargs usage
        v = system.newSexp(system.newInt(12), system.newString("hi"));
        assertEquals(2, v.size());
        checkInt(12, v.get(0));
        checkString("hi", v.get(1));

        try {
            v = system.newSexp(system.newInt(12), null);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) { }
    }

    @Test
    public void testCreatingSexpWithString()
    {
        IonSexp sexp1 = system().newNullSexp();
        sexp1.add(system().newString("Hello"));

        IonValue sexp2 = reload(sexp1);

        assertEquals(sexp1, sexp2);

        // Again, starting from () instead of null.sexp
        sexp1 = system().newEmptySexp();
        sexp1.add(system().newString("Hello"));

        sexp2 = reload(sexp1);
        assertEquals(sexp1, sexp2);
    }

    private String[] terminator_test_values = {
                "(1001-12-31'symbol')",
                "(1001-12-31\"string\")",
                "(1001-12-31/* coomment */)",
                "(1001-12-31//other comment\n)",
                "(1001-12-31[data,list])",
                "(1001-12-31{a:struct})",
                "(1001-12-31{{ blob }} )",
                "(1001-12-31{{\"clob\"}})",
                "(1001-12-31)",
                "(1001-12-31(sexp))",
                "(1001-12-31 )",
                "(1001-12-31\n)",
                "(1001-12-31\r)",
                "(1001-12-31\t)"
    };

    @Test
    public void testNumericTerminationCharacters()
    {
        for (String image : terminator_test_values) {
            IonReader r = system().newReader(image);
            TestUtils.deepRead(r);
        }

        // these should parse correctly
        for (String image : terminator_test_values) {
            IonValue value = oneValue(image);
            IonValue value2 = reload(value);
            assertEquals(value, value2);
        }
    }
}
