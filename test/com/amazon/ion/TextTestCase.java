// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 *
 */
public abstract class TextTestCase
    extends IonTestCase
{
    protected abstract String wrap(String ionText);

    protected String printed(String ionText)
    {
        return wrap(ionText);
    }

    protected String unwrap(IonValue value)
    {
        return ((IonText) value).stringValue();
    }

    public void testUnicodeEscapes()
    {
        final String u = "\\" + "u";
        final String U = "\\" + "U";

        testEscape("\\0", 1);

        testEscape( "\\x01", 1);

        // U+007F is a control character
        testEscape( "\\x7f", 1);
        testEscape( "\\xff", 1);
        testEscape(u + "0110", 1);
        testEscape(u + "ffff", 1);

        testEscape(U + "0001d110", 2);

        // The largest legal code point
        testEscape(U + "0010ffff", 2);
    }

    public void testEscape(String ionText, int codeUnitCount)
    {
        String ionData = wrap(ionText);
        IonValue value = oneValue(ionData);
        assertEquals(codeUnitCount, unwrap(value).length());

        String printed = printed(ionText);
        assertEquals(printed, value.toString());

        // Try again after shoving the escape beyond the "short string" size
        String spacer = "1234567890123456";
        ionText = spacer + ionText;
        codeUnitCount += spacer.length();

        ionData = wrap(ionText);
        value = oneValue(ionData);
        assertEquals(codeUnitCount, unwrap(value).length());

        printed = printed(ionText);
        assertEquals(printed, value.toString());
    }
}
