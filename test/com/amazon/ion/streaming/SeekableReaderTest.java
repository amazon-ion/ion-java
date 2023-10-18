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

package com.amazon.ion.streaming;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.ReaderMaker;
import com.amazon.ion.Span;
import com.amazon.ion.TestUtils;
import com.amazon.ion.impl._Private_Utils;
import com.amazon.ion.junit.Injected.Inject;
import com.amazon.ion.junit.IonAssert;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.SimpleCatalog;
import org.junit.Assert;
import org.junit.Test;

/**
 * @see NonSeekableReaderTest
 */
public class SeekableReaderTest
    extends ReaderFacetTestCase
{
    @Inject("readerMaker")
    public static final ReaderMaker[] READER_MAKERS =
        ReaderMaker.valuesExcluding(NON_SEEKABLE_READERS);


    private void checkSpans(IonDatagram dg, Span[] positions)
    {
        for (int i = dg.size() - 1; i >= 0; i--)
        {
            hoist(positions[i]);
            IonType dg_type = dg.get(i).getType();
            IonType span_type = in.next();
            if (dg_type.equals(span_type) == false) {
                assertEquals(dg_type, span_type);
            }
            expectTopLevel();
            IonAssert.assertIonEquals(dg.get(i), system().newValue(in));
        }
        expectTopEof();
    }

    @Test
    public void testTrivialSpan()
    {
        String text = "null";
        read(text);
        in.next();
        Span s = sr.currentSpan();
        expectTopEof();

        hoist(s);
        assertSame(IonType.NULL, in.next());
        expectTopLevel();
        expectTopEof();
    }

    @Test
    public void testWalkingBackwards()
    {
        String text =
            "null true 3 4e0 5.0 6666-06-06T '7' \"8\" {{\"\"}} {{}} [] () {}";

        IonDatagram dg = loader().load(text);

        Span[] positions = new Span[dg.size()];

        read(text);
        for (int i = 0; i < dg.size(); i++)
        {
            assertEquals(dg.get(i).getType(), in.next());
            positions[i] = sr.currentSpan();
        }
        expectEof();

        checkSpans(dg, positions);


        // Collect spans *after* extracting scalar body.

        read(text);
        for (int i = 0; i < dg.size(); i++)
        {
            IonType t =  in.next();
            assertEquals(dg.get(i).getType(),t);
            if (! IonType.isContainer(t))
            {
                TestUtils.consumeCurrentValue(in);
            }
            positions[i] = sr.currentSpan();
        }
        expectTopEof();

        checkSpans(dg, positions);
    }


    @Test
    public void testHoistingWithinContainers()
    {
        read("{f:v,g:[c, (d), e], /* h */ $0:null} s");

        in.next();
        in.stepIn();
            in.next();
            Span fPos = sr.currentSpan();
            assertEquals("v", in.stringValue());
            in.next();
            Span gPos = sr.currentSpan();
            in.stepIn();
                in.next();
                assertEquals("c", in.stringValue());
                Span cPos = sr.currentSpan();
            in.stepOut();
            in.next();
            Span hPos = sr.currentSpan();
            expectEof();
        in.stepOut();
        in.next();
        Span sPos = sr.currentSpan();
        expectTopEof();


        hoist(fPos);
        IonType in_type = in.next();
        assertEquals(IonType.SYMBOL, in_type);
        expectTopLevel();
        assertEquals("v", in.stringValue());
        expectTopEof();

        hoist(cPos);
        in.next();
        expectTopLevel();
        assertEquals("c", in.stringValue());
        expectTopEof();

        hoist(gPos);
        assertEquals(IonType.LIST, in.next());
        expectTopLevel();
        in.stepIn();
            in.next();
            assertEquals("c", in.stringValue());
            assertEquals(IonType.SEXP, in.next()); // (d)
            in.stepIn();
                in.next();
                assertEquals("d", in.stringValue());
                expectEof();
            in.stepOut();
            assertEquals(IonType.SYMBOL, in.next());
            assertEquals("e", in.stringValue());
            expectEof();
        in.stepOut();
        expectTopEof();

        hoist(hPos);
        assertEquals(IonType.NULL, in.next());
        expectTopLevel();

        hoist(fPos);
        check().noFieldName();
        assertEquals(IonType.SYMBOL, in.next());
        expectTopLevel();
        assertEquals("v", in.stringValue());
        expectTopEof();

        hoist(sPos);
        assertEquals(IonType.SYMBOL, in.next());
        expectTopLevel();
        assertEquals("s", in.stringValue());
        expectTopEof();
    }

    @Test
    public void testHoistingLongValue()
    {
        // This value is "long" in that it has a length subfield in the prefix.
        String text = " \"123456789012345\" ";
        read(text);

        in.next();
        Span pos = sr.currentSpan();
        expectTopEof();

        hoist(pos);
        assertEquals(IonType.STRING, in.next());
        expectTopEof();
    }

    @Test
    public void testHoistingOrderedStruct()
    throws IOException
    {
        File file = getTestdataFile("good/structOrdered.10n");
        byte[] binary = _Private_Utils.loadFileBytes(file);

        read(binary);

        in.next();
        Span pos = sr.currentSpan();
        expectTopEof();

        hoist(pos);
        assertEquals(IonType.STRUCT, in.next());
        expectTopEof();
    }


    @Test
    public void testHoistingAnnotatedTopLevelValue()
        throws IOException
    {
        read("a::v");
        in.next();
        Span span = sr.currentSpan();
        expectTopEof();

        hoist(span);
        assertSame(IonType.SYMBOL, in.next());
        expectTopLevel();
        Assert.assertArrayEquals(new String[]{"a"}, in.getTypeAnnotations());
        expectTopEof();
    }


    @Test
    public void testHoistingAnnotatedContainedValue()
        throws IOException
    {
        read("[a::v]");
        in.next();
        in.stepIn();
        in.next();
        Span span = sr.currentSpan();
        in.stepOut();
        in.next();

        hoist(span);
        assertSame(IonType.SYMBOL, in.next());
        expectTopLevel();
        Assert.assertArrayEquals(new String[]{"a"}, in.getTypeAnnotations());
        expectTopEof();
    }

    @Test
    public void testHoistingAcrossSymbolTableBoundary()
        throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (IonWriter writer = IonBinaryWriterBuilder.standard().withLocalSymbolTableAppendEnabled().build(out)) {
            writer.writeInt(123);
            writer.finish();
            writer.writeSymbol("abc");
            writer.finish();
            writer.writeSymbol("def");
            writer.flush();
            writer.writeSymbol("ghi");
            writer.writeSymbol("jkl");
            writer.finish();
            writer.writeSymbol("mno");
        }
        if (myReaderMaker.sourceIsText()) {
            read(out.toByteArray());
        } else {
            in = getStreamingMode().newIonReader(new SimpleCatalog(), out.toByteArray());
            initFacets();
        }

        in.next();
        Span integer = sp.currentSpan();
        in.next();
        Span abc = sp.currentSpan();
        in.next();
        Span def = sp.currentSpan();
        in.next();
        Span ghi = sp.currentSpan();
        in.next();
        Span jkl = sp.currentSpan();
        in.next();
        Span mno = sp.currentSpan();
        in.next();

        hoist(jkl);
        assertSame(IonType.SYMBOL, in.next());
        assertEquals("jkl", in.stringValue());

        hoist(ghi);
        assertSame(IonType.SYMBOL, in.next());
        assertEquals("ghi", in.stringValue());

        hoist(integer);
        assertSame(IonType.INT, in.next());
        assertEquals(123, in.intValue());

        hoist(def);
        assertSame(IonType.SYMBOL, in.next());
        assertEquals("def", in.stringValue());

        hoist(mno);
        assertSame(IonType.SYMBOL, in.next());
        assertEquals("mno", in.stringValue());

        hoist(abc);
        assertSame(IonType.SYMBOL, in.next());
        assertEquals("abc", in.stringValue());

        expectTopEof();
    }


    //========================================================================
    // Failure cases
}
