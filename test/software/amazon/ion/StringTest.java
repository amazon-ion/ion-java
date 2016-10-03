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

import org.junit.Test;
import software.amazon.ion.IonString;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.UnexpectedEofException;




public class StringTest
    extends TextTestCase
{
    public static void checkNullString(IonString value)
    {
        assertSame(IonType.STRING, value.getType());
        assertTrue("isNullValue() is false",   value.isNullValue());
        assertNull("stringValue() isn't null", value.stringValue());
    }

    public void modifyString(IonString value)
    {
        String modValue = "sweet!";
        assertFalse(modValue.equals(value.stringValue()));

        value.setValue(modValue);
        assertEquals(modValue, value.stringValue());
        assertFalse(value.isNullValue());

        String modValue1 = "dude!";
        value.setValue(modValue1);
        assertEquals(modValue1, value.stringValue());

        value.setValue(null);
        checkNullString(value);
    }


    @Override
    protected String wrap(String ionText)
    {
        return "\"" + ionText + "\"";
    }


    //=========================================================================
    // Test cases


    @Test
    public void testFactoryString()
    {
        IonString value = system().newNullString();
        checkNullString(value);
        modifyString(value);
    }

    @Test
    public void testTextNullString()
    {
        IonString value = (IonString) oneValue("null.string");
        checkNullString(value);
        modifyString(value);
    }


    @Test
    public void testEmptyStrings()
    {
        IonString value = (IonString) oneValue("\"\"");
        assertSame(IonType.STRING, value.getType());
        checkString("", value);
        value = (IonString) oneValue("''''''");
        checkString("", value);
    }


    @Test
    public void testStringBasics()
    {
        IonString value = (IonString) oneValue("\"hello\"");
        assertSame(IonType.STRING, value.getType());
        assertFalse(value.isNullValue());
        assertEquals("hello", value.stringValue());

        //TODO implement
    }


    @Test
    public void testTruncatedStrings()
    {
        // Short string
        try
        {
            oneValue(" \"1234");
            fail("Expected UnexpectedEofException");
        }
        catch (UnexpectedEofException e) { }


        // Medium string
        try
        {
            oneValue(" \"1234556789ABCDEF");
            fail("Expected UnexpectedEofException");
        }
        catch (UnexpectedEofException e) { }


        // Long string
        try
        {
            oneValue(" '''1234556789ABCDEF");
            fail("Expected UnexpectedEofException");
        }
        catch (UnexpectedEofException e) { }
    }


    @Test
    public void testBackslashEof()
    {
        try
        {
            // EOF right after backslash
            oneValue(" \"\\");
            fail("Expected UnexpectedEofException");
        }
        catch (UnexpectedEofException e) { }
    }


    @Test
    public void testStringEscapes()
    {
        IonString value = (IonString) oneValue(" \"\\\n\"");
        String valString = value.stringValue();
        assertEquals("", valString);

        // Test again with a longer string due to short-string parsing magic.
        value = (IonString) oneValue("\"1234556789ABCDEF\\\nGHI\"");
        valString = value.stringValue();
        assertEquals("1234556789ABCDEFGHI", valString);

        // JSON escapes. See http://www.ietf.org/rfc/rfc4627.txt
        assertEscape('\u0022', '\"');  // quotation mark
        assertEscape('\\',     '\\');  // reverse solidus  u005C
        assertEscape('\u002F', '/');   // solidus
        assertEscape('\u0008', 'b');   // backspace
        assertEscape('\u000C', 'f');   // form feed
        assertEscape('\n',     'n');   // line feed        u000A
        assertEscape('\r',     'r');   // carriage return  u000D
        assertEscape('\u0009', 't');   // tab

        // Non-JSON escapes.

        assertEscape('\u0000', '0');   // nul
        assertEscape('\u0007', 'a');   // bell
        assertEscape('\u000B', 'v');   // vertical tab
        assertEscape('\u003F', '?');   // question mark; thank you C++
        assertEscape('\'',     '\'');  // single quote
    }


    /**
     * No octal, but we do have \0.
     */
    @Test
    public void testOctal000()
    {
        IonString value = (IonString) oneValue("\"0\\0000\"");
        String str = value.stringValue();
        assertEquals(5, str.length());
        assertEquals('0', str.charAt(0));
        assertEquals( 0 , str.charAt(1));
        assertEquals('0', str.charAt(2));
        assertEquals('0', str.charAt(3));
        assertEquals('0', str.charAt(4));
    }

    @Test
    public void testUnicodeCharacters()
    {
        String expected;
        IonString value;

        expected = "\u0123 \u1234 \uceed"; // was \ufeed but that's an illegal unicode scalar
        value = (IonString) oneValue('"'+expected+'"');
        checkString(expected, value);
    }

    @Test
    public void testTrickyEscapes()
    {
        // Plowing them into a long string changes the parsing/encoding flow.
        // This caught a fairly nasty bug so leave it in.
        IonString value = (IonString) oneValue("\"\\0 \\a \\b \\t \\n \\f \\r \\v \\\" \\' \\? \\\\\\\\\\/\"");
        String expected = "\u0000 \u0007 \b \t \n \f \r \u000B \" \' ? \\\\/";
        checkString(expected, value);

        value = (IonString) oneValue("'''\\0 \\a \\b \\t \\n \\f \\r \\v \\\" \\' \\? \\\\\\\\\\/'''");
        checkString(expected, value);
    }

    @Test
    public void testReadStringsFromSuite()
        throws Exception
    {
        Iterable<IonValue> values = loadTestFile("good/strings.ion");
        // File is a sequence of many string values.

        for (IonValue value : values)
        {
            assertTrue(value instanceof IonString);
        }
    }

    @Test
    public void testNewlineInString()
    {
        badValue(" \"123\n456\" ");
        // Get beyond the 13-char threshold.
        badValue(" \"123456789ABCDEF\nGHI\" ");
    }

    @Test
    public void testTopLevelConcatenation()
    {
        IonValue value = oneValue("  '''a''' '''b'''  ");
        checkString("ab", value);
    }

    @Test
    public void testQuotesOnMediumStringBoundary()
    {
        // Double-quote falls on the boundary.
        checkString("KIM 12\" X 12\"", oneValue("\"KIM 12\\\" X 12\\\"\""));
        // Try again with long-string syntax.
        checkString("KIM 12\" X 12\"", oneValue("'''KIM 12\\\" X 12\\\"'''"));
    }


    @Test
    public void testStringClone()
        throws Exception
    {
        testSimpleClone("null.string");
        testSimpleClone("\"\"");
        testSimpleClone("\"root\"");
    }

    @Test
    public void testLongStringIllegalUnicodeControlCharacters() {
        badValue("'''\u0000'''");
        badValue("'''\u0001'''");
        badValue("'''\u0002'''");
        badValue("'''\u0003'''");
        badValue("'''\u0004'''");
        badValue("'''\u0005'''");
        badValue("'''\u0006'''");
        badValue("'''\u0007'''");
        badValue("'''\u0008'''");
        // 0009 is allowed
        // 000A is allowed
        // 000B is allowed
        // 000C is allowed
        // 000D is allowed
        badValue("'''\u000E'''");
        badValue("'''\u000F'''");
        badValue("'''\u0010'''");
        badValue("'''\u0011'''");
        badValue("'''\u0012'''");
        badValue("'''\u0013'''");
        badValue("'''\u0014'''");
        badValue("'''\u0015'''");
        badValue("'''\u0016'''");
        badValue("'''\u0017'''");
        badValue("'''\u0018'''");
        badValue("'''\u0019'''");
        badValue("'''\u001A'''");
        badValue("'''\u001B'''");
        badValue("'''\u001C'''");
        badValue("'''\u001D'''");
        badValue("'''\u001E'''");
        badValue("'''\u001F'''");
    }

    @Test
    public void testShortStringIllegalUnicodeControlCharacters() {
        badValue("\"\u0000\"");
        badValue("\"\u0001\"");
        badValue("\"\u0002\"");
        badValue("\"\u0003\"");
        badValue("\"\u0004\"");
        badValue("\"\u0005\"");
        badValue("\"\u0006\"");
        badValue("\"\u0007\"");
        badValue("\"\u0008\"");
        // 0009 is allowed
        badValue("\"\n\""); // 000A
        // 000B is allowed
        // 000C is allowed
        badValue("\"\r\""); // 000D
        badValue("\"\u000E\"");
        badValue("\"\u000F\"");
        badValue("\"\u0010\"");
        badValue("\"\u0011\"");
        badValue("\"\u0012\"");
        badValue("\"\u0013\"");
        badValue("\"\u0014\"");
        badValue("\"\u0015\"");
        badValue("\"\u0016\"");
        badValue("\"\u0017\"");
        badValue("\"\u0018\"");
        badValue("\"\u0019\"");
        badValue("\"\u001A\"");
        badValue("\"\u001B\"");
        badValue("\"\u001C\"");
        badValue("\"\u001D\"");
        badValue("\"\u001E\"");
        badValue("\"\u001F\"");
    }
}
