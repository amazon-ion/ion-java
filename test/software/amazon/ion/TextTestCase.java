/*
 * Copyright 2009-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import software.amazon.ion.IonText;
import software.amazon.ion.IonValue;

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

    @Test
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
