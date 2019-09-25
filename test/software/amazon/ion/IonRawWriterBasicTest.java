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

package software.amazon.ion;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import software.amazon.ion.facet.Facets;
import software.amazon.ion.impl.bin._Private_IonManagedWriter;
import software.amazon.ion.impl.bin._Private_IonRawWriter;
import software.amazon.ion.system.IonReaderBuilder;
import software.amazon.ion.system.IonSystemBuilder;
import software.amazon.ion.system.SimpleCatalog;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests the raw string writing APIs and basic behavior of the raw symbol ID
 * writing APIs provided by the {@link _Private_IonRawWriter} and {@link IonBinaryWriter}
 * facets.
 * @see IonRawWriterSymbolsTest
 */
@SuppressWarnings({"deprecation", "javadoc"})
public class IonRawWriterBasicTest
{
    private static final IonSystem system = IonSystemBuilder.standard().build();

    private static IonWriter writer(OutputStream out, SymbolTable...imports)
    {
        // Only the binary writer supports the managed/raw facets.
        return system.newBinaryWriter(out, imports);
    }

    static List<String> getStrings(String file)
    {
        List<String> strings = new ArrayList<String>();
        IonReader reader;
        try
        {
            reader = IonReaderBuilder.standard().build(new FileInputStream(IonTestCase.getTestdataFile(file)));
        }
        catch (FileNotFoundException e)
        {
            // convert to unchecked exception so this plays well in static initializers
            throw new RuntimeException(e);
        }
        while (reader.next() != null)
        {
            try
            {
                strings.add(reader.stringValue());
            }
            catch (UnknownSymbolException e)
            {
                // If the input file contains unknown symbol text, e.g. $123,
                // just skip it.
            }
        }
        return strings;
    }

    static abstract class Roundtrip {

        private final SymbolTable[] imports;
        private final SimpleCatalog catalog;

        public Roundtrip(SymbolTable...imports)
        {
            this.imports = imports;
            catalog = new SimpleCatalog();
            for (SymbolTable imported : imports)
            {
                catalog.putTable(imported);
            }
        }

        public void test() throws IOException
        {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            IonWriter writer = writer(out, imports);
            _Private_IonManagedWriter managedWriter = writer.asFacet(_Private_IonManagedWriter.class);
            _Private_IonRawWriter rawWriter = managedWriter.getRawWriter();
            write(managedWriter, rawWriter);
            writer.close();
            read(IonReaderBuilder.standard().withCatalog(catalog).build(out.toByteArray()));
        }

        abstract void write(_Private_IonManagedWriter managedWriter, _Private_IonRawWriter rawWriter) throws IOException;
        abstract void read(IonReader reader);
    }

    private void testWriteEncodedString(final String str, final int padding) throws IOException
    {
        new Roundtrip()
        {
            int written = 0;

            @Override
            void write(_Private_IonManagedWriter managedWriter, _Private_IonRawWriter rawWriter)
                throws IOException
            {
                byte[] encoded = str.getBytes("UTF-8");
                int encodedLength = encoded.length;

                rawWriter.writeString(encoded, 0, encodedLength);
                written++;

                byte[] block = new byte[encodedLength + padding];
                for (int i = 0; i < 3; i++)
                {
                    // Write from buffer with 'padding' extra bytes. 'shift' indicates
                    // the position at which the actual data starts.
                    int shift = padding * i / 2;
                    System.arraycopy(encoded, 0, block, shift, encodedLength);
                    rawWriter.writeString(block, shift, encodedLength);
                    written++;
                }
            }

            @Override
            void read(IonReader reader)
            {
                int read = 0;
                while (reader.next() != null)
                {
                    assertEquals(IonType.STRING, reader.getType());
                    assertEquals(str, reader.stringValue());
                    read++;
                }
                assertEquals(written, read);
            }

        }.test();
    }

    @Test
    public void testWriteEncodedStrings() throws IOException
    {
        int i = 0;
        for (String str : getStrings("good/strings.ion"))
        {
            testWriteEncodedString(str, i * 2);
            i++;
        }
    }

    @Test
    public void testWriteNullString() throws IOException
    {
        new Roundtrip()
        {

            @Override
            void write(_Private_IonManagedWriter managedWriter, _Private_IonRawWriter rawWriter)
                throws IOException
            {
                rawWriter.writeString(null, 1, 2); // position arguments ignored
            }

            @Override
            void read(IonReader reader)
            {
                assertEquals(IonType.STRING, reader.next());
                assertTrue(reader.isNullValue());
                assertNull(reader.stringValue());
            }

        }.test();
    }

    private _Private_IonRawWriter basicRawWriter()
    {
        IonWriter writer = writer(new ByteArrayOutputStream());
        _Private_IonManagedWriter managedWriter = Facets.assumeFacet(_Private_IonManagedWriter.class, writer);
        return managedWriter.getRawWriter();
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testRawWriteIVMFails() throws IOException
    {
        _Private_IonRawWriter rawWriter = basicRawWriter();
        rawWriter.addTypeAnnotationSymbol(10);
        rawWriter.writeSymbolToken(2); // Symbol 2 with an annotation is not IVM
        rawWriter.stepIn(IonType.STRUCT);
        rawWriter.setFieldNameSymbol(11);
        rawWriter.writeSymbolToken(2); // Symbol 2 below top level is not IVM
        rawWriter.stepOut();
        thrown.expect(IonException.class);
        rawWriter.writeSymbolToken(2); // Top-level symbol 2 is IVM
    }

    @Test
    public void testRawSetFieldNameOutsideStructFails()
    {
        _Private_IonRawWriter rawWriter = basicRawWriter();
        thrown.expect(IonException.class);
        rawWriter.setFieldNameSymbol(10);
    }

    @Test
    public void testRawSetTypeAnnotationSymbolsOverwrites() throws IOException
    {
        new Roundtrip()
        {

            @Override
            void write(_Private_IonManagedWriter managedWriter, _Private_IonRawWriter rawWriter)
                throws IOException
            {
                managedWriter.requireLocalSymbolTable();
                rawWriter.addTypeAnnotationSymbol(4);
                rawWriter.addTypeAnnotationSymbol(5);
                rawWriter.setTypeAnnotationSymbols(6, 7);
                rawWriter.writeInt(42); // dummy value
            }

            @Override
            void read(IonReader reader)
            {
                reader.next();
                SymbolToken[] annotations = reader.getTypeAnnotationSymbols();
                assertEquals(2, annotations.length);
                int[] annotationSids = { annotations[0].getSid(), annotations[1].getSid() };
                assertArrayEquals(new int[]{6, 7}, annotationSids);
            }

        }.test();
    }
}
