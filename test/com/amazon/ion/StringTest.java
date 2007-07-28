/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;




public class StringTest
    extends IonTestCase
{
    public static void checkNullString(IonString value)
    {
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


    //=========================================================================
    // Test cases


    public void testFactoryString()
    {
        IonString value = system().newString();
        checkNullString(value);
        modifyString(value);
    }

    public void testTextNullString()
    {
        IonString value = (IonString) oneValue("null.string");
        checkNullString(value);
        modifyString(value);
    }


    public void testEmptyStrings()
    {
        IonString value = (IonString) oneValue("\"\"");
        checkString("", value);
        value = (IonString) oneValue("''''''");
        checkString("", value);
    }


    public void testStringBasics()
    {
        IonString value = (IonString) oneValue("\"hello\"");
        assertFalse(value.isNullValue());
        assertEquals("hello", value.stringValue());

        //TODO implement
    }


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


    public void testStringEscapes()
    {
        try
        {
            // EOF right after backslash
            oneValue(" \"\\");
            fail("Expected UnexpectedEofException");
        }
        catch (UnexpectedEofException e) { }

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
        assertEscape('\u001B', 'e');   // escape
        assertEscape('\u000B', 'v');   // vertical tab
        assertEscape('\u003F', '?');   // question mark; thank you C++
        assertEscape('\'',     '\'');  // single quote


        // TODO test escape groups uHHHH UHHHHHHHH xHH
    }


    /**
     * No octal, but we do have \0.
     */
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

    public void testUnicodeCharacters()
    {
        String expected;
        IonString value;

        expected = "\u0123 \u1234 \ufeed";
        value = (IonString) oneValue('"'+expected+'"');
        checkString(expected, value);

        expected = "\u0000 \u0007 \u0012 \u0123 \u1234 \ufeed";
        value = (IonString) oneValue('"'+expected+'"');
        checkString(expected, value);

    }

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

    public void testReadStringsFromSuite()
        throws Exception
    {
        Iterable<IonValue> values = readTestFile("good/strings.ion");
        // File is a sequence of many string values.

        for (IonValue value : values)
        {
            assertTrue(value instanceof IonString);
        }
    }

    public void testNewlineInString()
    {
        badValue(" \"123\n456\" ");
        // Get beyond the 13-char threshold.
        badValue(" \"123456789ABCDEF\nGHI\" ");
    }

    public void testTopLevelConcatenation()
    {
        IonValue value = oneValue("  '''a''' '''b'''  ");
        checkString("ab", value);
    }
    
    public void testQuotesOnMediumStringBoundary() 
    {
        // Double-quote falls on the boundary.
        checkString("KIM 12\" X 12\"", oneValue("\"KIM 12\\\" X 12\\\"\""));
        // Try again with long-string syntax.
        checkString("KIM 12\" X 12\"", oneValue("'''KIM 12\\\" X 12\\\"'''"));
    }

}
