package com.amazon.ion;

import com.amazon.ion.system.SystemFactory;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import org.apache.commons.io.HexDump;
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
    
    private String hexDump(final String str) {
        try {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            HexDump.dump(str.getBytes("UTF-16BE"), 0L, out, 0);
            return out.toString("US-ASCII");
        } catch (final RuntimeException e) {
            throw e;
        } catch (final Exception e) {
            throw new IllegalArgumentException(e);
        }
        
    }
    
    private void assertSingleCodePoint(final int expectedCode, final String str) {
        final int codePointCount = str.codePointCount(0, str.length());
        assertEquals("String is not single code point " + hexDump(str), 1, codePointCount);
        
        final int code = str.codePointAt(0);
        assertEquals(String.format("Expected %x, was %x", expectedCode, code), expectedCode, code);
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
    
    @Before
    public void setUp() {
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
