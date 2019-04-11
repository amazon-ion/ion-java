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

package com.amazon.ion;

import com.amazon.ion.impl._Private_Utils;
import org.junit.After;
import org.junit.Test;

public class SurrogateEscapeTest extends IonTestCase {

    private StringBuilder buf = new StringBuilder();

    private IonDatagram load() {
        byte[] utf8 = _Private_Utils.utf8(buf.toString());
        return loader().load(utf8);
    }

    private IonReader reader() {
        byte[] utf8 = _Private_Utils.utf8(buf.toString());
        return system().newReader(utf8);
    }

    private void assertSingleCodePoint(final int expectedCode, final String str) {
        final int codePointCount = str.codePointCount(0, str.length());
        assertEquals("String is not single code point " +
                     TestUtils.hexDump(str), 1, codePointCount);

        final int code = str.codePointAt(0);
        assertEquals(String.format("Expected %x, was %x", expectedCode, code),
                     expectedCode, code);
    }

    private void assertSingletonCodePoint(final int expectedCode) {
        final IonDatagram dg = load();
        assertEquals(1, dg.size());

        assertSingleCodePoint(expectedCode, ((IonString) dg.get(0)).stringValue());

        final IonReader reader = reader();
        if (! _Private_Utils.READER_HASNEXT_REMOVED) {
            assertTrue(reader.hasNext());
        }
        assertEquals(IonType.STRING, reader.next());
        assertSingleCodePoint(expectedCode, reader.stringValue());
    }


    @Override @After
    public void tearDown()
        throws Exception
    {
        buf = null;
        super.tearDown();
    }

    @Test
    public void testLoadLiteralNonBmp() {
        buf.append("'''")
           .append('\uDAF7')
           .append('\uDE56')
           .append("'''")
           ;
        assertSingletonCodePoint(0x000CDE56);
    }

    @Test
    public void testLoadEscapeNonBmp() {
        buf.append("'''")
           .append('\\')
           .append("U000CDE56")
           .append("'''")
           ;
        assertSingletonCodePoint(0x000CDE56);
    }
}
