// Copyright (c) 2011-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import static com.amazon.ion.Symtabs.printLocalSymtab;
import static com.amazon.ion.impl._Private_Utils.intIterator;
import static com.amazon.ion.junit.IonAssert.checkNullSymbol;

import com.amazon.ion.BinaryTest;
import com.amazon.ion.IonType;
import com.amazon.ion.ReaderMaker;
import com.amazon.ion.junit.Injected.Inject;
import com.amazon.ion.junit.IonAssert;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import org.junit.Test;

/**
 *
 */
public class ReaderTest
    extends ReaderTestCase
{
    @Inject("readerMaker")
    public static final ReaderMaker[] READER_MAKERS = ReaderMaker.values();


    @Test
    public void testNullInt()
    {
        {
            read("null.int");
            assertEquals(IonType.INT, in.next());
            assertTrue(in.isNullValue());
            assertEquals(null, in.bigIntegerValue());
        }
        {
            for (final String hex : Arrays.asList("E0 01 00 EA 2F", "E0 01 00 EA 3F")) {
                read(BinaryTest.hexToBytes(hex));
                assertEquals(IonType.INT, in.next());
                assertTrue(in.isNullValue());
                assertEquals(null, in.bigIntegerValue());
            }
        }
    }

    @Test
    public void testStepInOnNull() throws IOException
    {
        read("null.list null.sexp null.struct");

        assertEquals(IonType.LIST, in.next());
        assertTrue(in.isNullValue());
        in.stepIn();
        expectEof();
        in.stepOut();

        assertEquals(IonType.SEXP, in.next());
        assertTrue(in.isNullValue());
        in.stepIn();
        expectEof();
        in.stepOut();

        assertEquals(IonType.STRUCT, in.next());
        assertTrue(in.isNullValue());
        in.stepIn();
        expectEof();
        in.stepOut();
        expectEof();
    }

    @Test // Traps ION-133
    public void testStepOut() throws IOException
    {
        read("{a:{b:1,c:2},d:false}");

        in.next();
        in.stepIn();
        expectNextField("a");
        in.stepIn();
        expectNextField("b");
        in.stepOut(); // skip c
        expectNoCurrentValue();
        expectNextField("d");
        expectEof();
    }


    @Test
    public void testIterateTypeAnnotationIds()
    throws Exception
    {
        String ionText =
            printLocalSymtab("ann", "ben")
            + "ann::ben::null";

        read(ionText);

        in.next();
        Iterator<Integer> typeIds = in.iterateTypeAnnotationIds();
        IonAssert.assertIteratorEquals(intIterator(10, 11), typeIds);
        expectEof();
    }

    @Test
    public void testStringValueOnNull()
        throws Exception
    {
        read("null.string null.symbol");

        in.next();
        expectString(null);
        in.next();
        checkNullSymbol(in);
    }

    @Test
    public void testStringValueOnNonText()
        throws Exception
    {
        // All non-text types
        read("null true 1 1e2 1d2 2011-12-01T {{\"\"}} {{}} [] () {}");

        while (in.next() != null)
        {
            try {
                in.stringValue();
                fail("expected exception on " + in.getType());
            }
            catch (IllegalStateException e) { }
        }
    }

    @Test
    public void testSymbolValue()
        throws Exception
    {
        read("null.symbol sym");
        in.next();
        checkNullSymbol(in);
        in.next();
        IonAssert.checkSymbol("sym", in);
    }

    @Test
    public void testSymbolValueOnNonSymbol()
        throws Exception
    {
        // All non-symbol types
        read("null true 1 1e2 1d2 2011-12-01T \"\" {{\"\"}} {{}} [] () {}");

        while (in.next() != null)
        {
            try {
                in.symbolValue();
                fail("expected exception on " + in.getType());
            }
            catch (IllegalStateException e) { }
        }
    }

    @Test
    public void testSymbolIdOnNonSymbol()
        throws Exception
    {
        // All non-symbol types
        read("null true 1 1e2 1d2 2011-12-01T \"\" {{\"\"}} {{}} [] () {}");

        while (in.next() != null)
        {
            try {
                in.getSymbolId();
                fail("expected exception on " + in.getType());
            }
            catch (IllegalStateException e) { }
        }
    }

    @Test
    public void testIntValueOnNonNumber()
    {
        // All non-numeric types
        read("null true 2011-12-01T \"\" sym {{\"\"}} {{}} [] () {}");

        while (in.next() != null)
        {
            IonType type = in.getType();

            try {
                in.intValue();
                fail("expected exception from intValue on" + type);
            }
            catch (IllegalStateException e) { }

            try {
                in.longValue();
                fail("expected exception from longValue on " + type);
            }
            catch (IllegalStateException e) { }

            try {
                in.bigIntegerValue();
                fail("expected exception from bigIntegerValue on " + type);
            }
            catch (IllegalStateException e) { }
        }
    }
}
