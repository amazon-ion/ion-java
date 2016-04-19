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

import static software.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static software.amazon.ion.impl.PrivateUtils.newSymbolToken;

import org.junit.Test;
import software.amazon.ion.EmptySymbolException;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonList;
import software.amazon.ion.IonSymbol;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.UnsupportedIonVersionException;
import software.amazon.ion.ValueFactory;



public class SymbolTest
    extends TextTestCase
{

    public static void checkNullSymbol(IonSymbol value)
    {
        assertSame(IonType.SYMBOL, value.getType());
        assertTrue("isNullValue() is false",   value.isNullValue());
        assertNull("stringValue() isn't null", value.stringValue());
        assertNull("symbolValue() isn't null", value.symbolValue());
    }

    public void modifySymbol(IonSymbol value)
    {
        String modValue = "sweet!";

        // Make sure the test case isn't starting in the desired state.
        assertFalse(modValue.equals(value.stringValue()));

        value.setValue(modValue);
        checkSymbol(modValue, value);
//        assertTrue(value.intValue() > 0);

        String modValue1 = "dude!";
        value.setValue(modValue1);
        checkSymbol(modValue1, value);
        int sid = value.symbolValue().getSid();

        try {
            value.setValue("");
            fail("Expected EmptySymbolException");
        }
        catch (EmptySymbolException e) { }
        checkSymbol(modValue1, sid, value);

        value.setValue(null);
        checkNullSymbol(value);
    }



    @Override
    protected String wrap(String ionText)
    {
        return "'" + ionText + "'";
    }


    //=========================================================================
    // Test cases

    @Test
    public void testFactorySymbol()
    {
        IonSymbol value = system().newNullSymbol();
        checkNullSymbol(value);
        modifySymbol(value);

        value = system().newSymbol("hello");
    }

    public void testFactorySymbolWithSid(ValueFactory vf)
    {
        final int sid = 99;
        SymbolToken tok = newSymbolToken((String)null, sid);

        IonSymbol value = vf.newSymbol(tok);
        checkUnknownSymbol(99, value);

        tok = newSymbolToken("text", sid);
        value = vf.newSymbol(tok);
        checkSymbol("text", value);
        assertFalse(value.isNullValue());

        // TODO this needs to be well-defined
//        assertEquals(sid, value.getSymbolId());
    }

    @Test
    public void testFactorySymbolWithSid()
    {
        ValueFactory vf = system();
        testFactorySymbolWithSid(vf);

        IonList list = system().newEmptyList();
        vf = list.add();
        testFactorySymbolWithSid(vf);
    }

    @Test
    public void testNullSymbol()
    {
        IonSymbol value = (IonSymbol) oneValue("null.symbol");
        checkNullSymbol(value);
        assertArrayEquals(new String[0], value.getTypeAnnotations());
        modifySymbol(value);

        value = (IonSymbol) oneValue("a::null.symbol");
        checkNullSymbol(value);
        checkAnnotation("a", value);
        modifySymbol(value);
    }


    @Test
    public void testPlusInfinitySymbol()
    {
        system().singleValue("{value:special::'+infinity'}");
    }


    @Test
    public void testSymbols()
    {
        IonSymbol value = (IonSymbol) oneValue("foo");
        checkSymbol("foo", value);
//        assertTrue(value.getSymbolId() > 0);
        modifySymbol(value);

        value = (IonSymbol) oneValue("'foo'");
        checkSymbol("foo", value);
//        assertTrue(value.getSymbolId() > 0);
        modifySymbol(value);

        value = (IonSymbol) oneValue("'foo bar'");
        checkSymbol("foo bar", value);
//        assertTrue(value.getSymbolId() > 0);
        modifySymbol(value);
    }


    @Test
    public void testSyntheticSymbols()
    {
        String symText = "$324";
        IonSymbol value = (IonSymbol) oneValue(symText);
        checkUnknownSymbol(324, value);

        IonDatagram dg = loader().load(symText);
        value = (IonSymbol) dg.get(0);
        checkUnknownSymbol(324, value);

        value.removeFromContainer();
        checkUnknownSymbol(324, value);

        value.makeReadOnly();
        checkUnknownSymbol(324, value);
    }


    @Test
    public void testQuotesOnMediumStringBoundary()
    {
        // Double-quote falls on the boundary.
        checkSymbol("KIM 12\" X 12\"", oneValue("'KIM 12\\\" X 12\\\"'"));
    }


    @Test
    public void testCloneNullSymbol()
    {
        // null.symbol
        IonValue original = system().newSymbol((String) null);
        assertTrue("original should be a Ion null value", original.isNullValue());
        testCloneVariants(original);
    }

    @Test
    public void testCloneSymbolWithKnownText()
    {
        //===== known text, known sid =====
        SymbolToken tok = newSymbolToken("some_text", 99);
        IonSymbol original = system().newSymbol(tok);
        assertFalse("original should not be null", original.isNullValue());
        testCloneVariants(original);

        //===== known text, unknown sid =====
        tok = newSymbolToken("some_text", UNKNOWN_SYMBOL_ID);
        original = system().newSymbol(tok);
        assertFalse("original should not be null", original.isNullValue());
        testCloneVariants(original);
    }

    @Test
    public void testSymbolWithEscapedNewline()
    {
        badValue("'\\\n'");
    }

    @Test(expected = UnsupportedIonVersionException.class)
    public void rejectsUnsupportedVersion_0_0()
    {
        system().singleValue("$ion_0_0");
    }

    @Test(expected = UnsupportedIonVersionException.class)
    public void rejectsUnsupportedVersion_1_1()
    {
        system().singleValue("$ion_1_1");
    }

    @Test(expected = UnsupportedIonVersionException.class)
    public void rejectsUnsupportedVersion_2_0()
    {
        system().singleValue("$ion_2_0");
    }

    @Test(expected = UnsupportedIonVersionException.class)
    public void rejectsUnsupportedVersion_1234_0()
    {
        system().singleValue("$ion_1234_0");
    }
}
