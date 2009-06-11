// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import com.amazon.ion.system.SystemFactory;
import java.io.UnsupportedEncodingException;
import org.junit.Before;
import org.junit.Test;

public class SurrogateEscapeTest extends IonTestCase {
    // TODO when text reader is ready, run tests on those
    private static final boolean RUN_TEXT_READER_ASSERTS = false;

    private final IonSystem ion = SystemFactory.newSystem();
    private final StringBuilder buf = new StringBuilder();

    private IonDatagram load() {
        try {
            return loader().load(buf.toString().getBytes("UTF-8"));
        } catch (final UnsupportedEncodingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private IonReader reader() {
        try {
            return ion.newReader(buf.toString().getBytes("UTF-8"));
        } catch (final UnsupportedEncodingException e) {
            throw new IllegalArgumentException(e);
        }
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

        if (RUN_TEXT_READER_ASSERTS) {
            final IonReader reader = reader();
            assertTrue(reader.hasNext());
            assertEquals(IonType.STRING, reader.next());
            assertSingleCodePoint(expectedCode, reader.stringValue());
        }
    }

    @Override
    @Before
    public void setUp()
        throws Exception
    {
        super.setUp();
        buf.setLength(0);
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
        // JIRA ION-63
        buf.append("'''")
           .append('\\')
           .append("U000CDE56")
           .append("'''")
           ;
        assertSingletonCodePoint(0x000CDE56);
    }
}
