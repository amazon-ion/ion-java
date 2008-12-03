/* Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;



public class SexpTest
    extends SequenceTestCase
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
        return system().newSexp(children);
    }

    @Override
    protected <T extends IonValue> IonSexp newSequence(T... elements)
    {
        return system().newSexp(elements);
    }

    @Override
    protected String wrap(String v)
    {
        return "(" + v + ")";
    }

    @Override
    protected String wrap(String... children)
    {
        StringBuilder buf = new StringBuilder();
        buf.append('(');
        for (String child : children)
        {
            buf.append(child);
            buf.append(' ');
        }
        buf.append(')');
        return buf.toString();
    }

    @Override
    protected IonSexp wrap(IonValue... children)
    {
        return system().newSexp(children);
    }

    //=========================================================================
    // Test cases

    public void testFactoryNullSexp()
    {
        IonSexp value = system().newNullSexp();
        assertSame(IonType.SEXP, value.getType());
        assertNull(value.getContainer());
        testFreshNullSequence(value);
    }

    public void testTextNullSexp()
    {
        IonSexp value = (IonSexp) oneValue("null.sexp");
        assertSame(IonType.SEXP, value.getType());
        testFreshNullSequence(value);
    }

    public void testMakeNullSexp()
    {
        IonSexp value = (IonSexp) oneValue("(foo+bar)");
        assertSame(IonType.SEXP, value.getType());
        assertFalse(value.isNullValue());
        value.makeNull();
        testFreshNullSequence(value);
    }

    public void testClearNonMaterializedSexp()
    {
        IonSexp value = (IonSexp) oneValue("(foo+bar)");
        testClearContainer(value);
    }

    public void testEmptySexp()
    {
        IonSexp value = (IonSexp) oneValue("()");
        assertSame(IonType.SEXP, value.getType());
        testEmptySequence(value);
    }

    public void testGetTwiceReturnsSame()
    {
        IonSexp value = (IonSexp) oneValue("(a b)");
        IonValue elt1 = value.get(1);
        IonValue elt2 = value.get(1);
        assertSame(elt1, elt2);
    }


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

    /** Ensure that triple-quote concatenation works inside sexp. */
    public void testConcatenation()
    {
        IonSexp value = (IonSexp) oneValue("(a '''a''' '''b''' \"c\")");
        checkSymbol("a",  value.get(0));
        checkString("ab", value.get(1));
        checkString("c",  value.get(2));
        assertEquals(3, value.size());
    }

    public void testSexpIteratorRemove()
    {
        IonSexp value = (IonSexp) oneValue("(a b c)");
        testIteratorRemove(value);
    }

    public void testCreatingNullSexp()
    {
        IonSexp sexp1 = system().newNullSexp();
        IonValue sexp2 = reload(sexp1);

        // FIXME ensure sexp1._isPositionLoaded && _isMaterialized
        assertIonEquals(sexp1, sexp2);
    }

    public void testCreatingSexpFromCollection()
    {
        IonSystem system = system();
        List<IonValue> elements = null;

        IonSexp v = system.newSexp(elements);
        testFreshNullSequence(v);

        elements = new ArrayList<IonValue>();
        v = system.newSexp(elements);
        testEmptySequence(v);

        elements.add(system.newString("hi"));
        elements.add(system.newInt(1776));
        v = system.newSexp(elements);
        assertEquals(2, v.size());
        checkString("hi", v.get(0));
        checkInt(1776, v.get(1));

        try {
            v = system.newSexp(elements);
            fail("Expected ContainedValueException");
        }
        catch (ContainedValueException e) { }

        elements = new ArrayList<IonValue>();
        elements.add(system.newInt(1776));
        elements.add(null);
        try {
            v = system.newSexp(elements);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) { }
    }

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

    public void testCreatingSexpWithString()
    {
        IonSexp sexp1 = system().newNullSexp();
        sexp1.add(system().newString("Hello"));

        IonValue sexp2 = reload(sexp1);

        assertIonEquals(sexp1, sexp2);

        // Again, starting from () instead of null.sexp
        sexp1 = system().newEmptySexp();
        sexp1.add(system().newString("Hello"));

        sexp2 = reload(sexp1);
        assertIonEquals(sexp1, sexp2);
    }

    private String[] termintor_test_values = {
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

    public void testNumericTerminationCharacters()
    {
        IonSexp value = null;
        IonReader scanner = null;

        for (String image : termintor_test_values) {
            scanner = system().newReader(image);
            readAll(scanner);
        }

        // these should parse correctly
        for (String image : termintor_test_values) {
            value = (IonSexp) oneValue(image);
        }
        IonValue value2 = reload(value);
        assertIonEquals(value, value2);
    }
    void readAll(IonReader scanner)
    {
        while (scanner.hasNext()) {
            switch (scanner.next()) {
                case NULL:
                    break;
                case BOOL:
                    scanner.booleanValue();
                    break;
                case INT:
                    scanner.longValue();
                    break;
                case FLOAT:
                    scanner.doubleValue();
                    break;
                case DECIMAL:
                    scanner.bigDecimalValue();
                    break;
                case TIMESTAMP:
                    scanner.dateValue();
                    break;
                case STRING:
                    scanner.stringValue();
                    break;
                case SYMBOL:
                    scanner.stringValue();
                    break;
                case BLOB:
                    scanner.newBytes();
                    break;
                case CLOB:
                    scanner.newBytes();
                    break;
                case STRUCT:
                case LIST:
                case SEXP:
                    scanner.stepIn();
                    readAll(scanner);
                    scanner.stepOut();
                    break;
                case DATAGRAM:
                default:
                    throw new IonException("bad type returned by scanner");
            }
        }
    }
}
