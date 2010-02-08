// Copyright (c) 2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 *
 */
public class IonSystemTest
    extends IonTestCase
{
    /**
     * Ensure that singleValue() can handle Unicode.
     */
    public void testSingleValueUtf8()
    {
        String utf8Text = "Tivoli Audio Model One weiß/silber";
        String data = '"' + utf8Text + '"';

        IonString v = (IonString) system().singleValue(data);
        assertEquals(utf8Text, v.stringValue());

        byte[] binary = encode(data);
        v = (IonString) system().singleValue(binary);
        assertEquals(utf8Text, v.stringValue());
    }
}
