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

import static software.amazon.ion.IonRawWriterBasicTest.getStrings;
import static org.junit.Assert.assertEquals;

import software.amazon.ion.IonRawWriterBasicTest.Roundtrip;
import software.amazon.ion.impl.bin._Private_IonManagedWriter;
import software.amazon.ion.impl.bin._Private_IonRawWriter;
import software.amazon.ion.junit.Injected;
import software.amazon.ion.junit.Injected.Inject;
import software.amazon.ion.system.IonSystemBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests the raw symbol ID writing APIs of the {@link _Private_IonRawWriter} and
 * {@link IonBinaryWriter} facets.
 * @see IonRawWriterBasicTest
 */
@SuppressWarnings({"deprecation", "javadoc"})
@RunWith(Injected.class)
public class IonRawWriterSymbolsTest
{

    private static final IonSystem system = IonSystemBuilder.standard().build();

    private enum SymbolCombo
    {

        /**
         * Seralize and deserialize all symbols as symbol values.
         */
        VALUES
        {

            @Override
            public List<String> serialize(_Private_IonRawWriter writer) throws IOException
            {
                List<String> writtenSymbols = new ArrayList<String>();
                for (int i = 0; i < UNIQUE_SYMBOLS; i++)
                {
                    writtenSymbols.addAll(serializeNext(writer, null));
                }
                return writtenSymbols;
            }

            @Override
            public void deserialize(IonReader reader, List<String> expectedSymbols)
            {
                Iterator<String> expectedSymbolsIterator = expectedSymbols.iterator();
                for (int i = 0; i < UNIQUE_SYMBOLS; i++)
                {
                    assertEquals(IonType.SYMBOL, reader.next());
                    deserializeNext(reader, expectedSymbolsIterator, null);
                }
            }

            @Override
            public List<String> serializeNext(_Private_IonRawWriter writer, SymbolCombo delegate) throws IOException
            {
                assert delegate == null;
                String symbol = circularSymbolsList.next();
                writer.writeSymbolToken(getLocalSid(symbol));
                return Collections.singletonList(symbol);
            }

            @Override
            public void deserializeNext(IonReader reader, Iterator<String> expectedSymbols, SymbolCombo delegate)
            {
                assert delegate == null;
                assertEquals(expectedSymbols.next(), reader.symbolValue().getText());
            }

        },
        /**
         * Serialize and deserialize all symbols as field names.
         */
        FIELDS
        {

            @Override
            public List<String> serialize(_Private_IonRawWriter writer)
                throws IOException
            {
                List<String> writtenSymbols = new ArrayList<String>();
                writer.stepIn(IonType.STRUCT);
                for (int i = 0; i < UNIQUE_SYMBOLS; i++)
                {
                    writtenSymbols.addAll(serializeNext(writer, null));
                }
                writer.stepOut();
                return writtenSymbols;
            }

            @Override
            public void deserialize(IonReader reader,
                                    List<String> expectedSymbols)
            {
                reader.next();
                reader.stepIn();
                Iterator<String> expectedSymbolsIterator = expectedSymbols.iterator();
                for (int i = 0; i < UNIQUE_SYMBOLS; i++)
                {
                    reader.next();
                    deserializeNext(reader, expectedSymbolsIterator, null);
                }
                reader.stepOut();
            }

            @Override
            protected List<String> serializeNext(_Private_IonRawWriter writer,
                                           SymbolCombo delegate)
                throws IOException
            {
                List<String> writtenSymbols = new ArrayList<String>();
                String symbol = circularSymbolsList.next();
                writtenSymbols.add(symbol);
                writer.setFieldNameSymbol(getLocalSid(symbol));
                if (delegate == null) {
                    writer.writeInt(42); // dummy value
                }
                else
                {
                    writtenSymbols.addAll(delegate.serializeNext(writer, null));
                }
                return writtenSymbols;
            }

            @Override
            public void deserializeNext(IonReader reader, Iterator<String> expectedSymbols, SymbolCombo delegate)
            {
                assertEquals(expectedSymbols.next(), reader.getFieldName());
                if (delegate != null)
                {
                    delegate.deserializeNext(reader, expectedSymbols, null);
                }
            }

        },
        /**
         * Serialize and deserialize all symbols as annotations.
         */
        ANNOTATIONS
        {

            @Override
            public List<String> serialize(_Private_IonRawWriter writer)
                throws IOException
            {
                List<String> writtenSymbols = new ArrayList<String>();
                for (int numAnnotations = 1; numAnnotations < UNIQUE_SYMBOLS; numAnnotations++)
                {
                    if (numAnnotations <= 2)
                    {
                        // Tests addTypeAnnotationSymbol called alone and repetitively
                        for (int i = 0; i < numAnnotations; i++)
                        {
                            String symbol = circularSymbolsList.next();
                            writtenSymbols.add(symbol);
                            writer.addTypeAnnotationSymbol(getLocalSid(symbol));
                        }
                    }
                    else
                    {
                        // tests setTypeAnnotationSymbols
                        int[] symbolIds = new int[numAnnotations];
                        for (int i = 0; i < numAnnotations; i++)
                        {
                            String symbol = circularSymbolsList.next();
                            writtenSymbols.add(symbol);
                            symbolIds[i] = getLocalSid(symbol);
                            writer.setTypeAnnotationSymbols(symbolIds);
                        }
                    }
                    writer.writeInt(numAnnotations); // dummy value
                }
                return writtenSymbols;
            }

            @Override
            public void deserialize(IonReader reader,
                                    List<String> expectedSymbols)
            {
                Iterator<String> expectedSymbolsIterator = expectedSymbols.iterator();
                for (int numAnnotations = 1; numAnnotations < UNIQUE_SYMBOLS; numAnnotations++)
                {
                    reader.next();
                    for (String annotation : reader.getTypeAnnotations())
                    {
                        assertEquals(expectedSymbolsIterator.next(), annotation);
                    }
                }
            }

            // NOTE: for ANNOTATIONS, the *Next methods only add a single
            // annotation, for a cleaner interface. Multiple annotations on
            // a single value are tested by the *serialize methods.

            @Override
            protected List<String> serializeNext(_Private_IonRawWriter writer,
                                           SymbolCombo delegate)
                throws IOException
            {
                List<String> writtenSymbols = new ArrayList<String>();
                String symbol = circularSymbolsList.next();
                writtenSymbols.add(symbol);
                writer.addTypeAnnotationSymbol(getLocalSid(symbol));
                if (delegate == null)
                {
                    writer.writeInt(42); //dummy value
                }
                else
                {
                    writtenSymbols.addAll(delegate.serializeNext(writer, null));
                }
                return writtenSymbols;
            }

            @Override
            public void deserializeNext(IonReader reader, Iterator<String> expectedSymbols, SymbolCombo delegate)
            {
                assertEquals(expectedSymbols.next(), reader.getTypeAnnotations()[0]);
                if (delegate != null)
                {
                    delegate.deserializeNext(reader, expectedSymbols, null);
                }
            }

        },
        /**
         * Serialize and deserialize symbols as symbol values with annotations.
         */
        ANNOTATIONS_AND_VALUES
        {

            @Override
            public List<String> serialize(_Private_IonRawWriter writer)
                throws IOException
            {
                List<String> writtenSymbols = new ArrayList<String>();
                for (int i = 0; i < UNIQUE_SYMBOLS; i++)
                {
                    writtenSymbols.addAll(serializeNext(writer, null));
                }
                return writtenSymbols;
            }

            @Override
            public void deserialize(IonReader reader,
                                    List<String> expectedSymbols)
            {
                Iterator<String> expectedSymbolsIterator = expectedSymbols.iterator();
                for (int i = 0; i < UNIQUE_SYMBOLS; i++)
                {
                    reader.next();
                    deserializeNext(reader, expectedSymbolsIterator, null);
                }
            }

            @Override
            protected List<String> serializeNext(_Private_IonRawWriter writer,
                                                 SymbolCombo delegate)
                throws IOException
            {

                return ANNOTATIONS.serializeNext(writer, VALUES);
            }

            @Override
            public void deserializeNext(IonReader reader, Iterator<String> expectedSymbols, SymbolCombo delegate)
            {
                ANNOTATIONS.deserializeNext(reader, expectedSymbols, VALUES);
            }

        },
        /**
         * Serialize and deserialize symbols as symbol values within a struct
         * (i.e. with field name symbols).
         */
        FIELDS_AND_VALUES
        {

            @Override
            public List<String> serialize(_Private_IonRawWriter writer)
                throws IOException
            {
                List<String> writtenSymbols = new ArrayList<String>();
                writer.stepIn(IonType.STRUCT);
                for (int i = 0; i < UNIQUE_SYMBOLS; i++)
                {
                    writtenSymbols.addAll(serializeNext(writer, null));
                }
                writer.stepOut();
                return writtenSymbols;
            }

            @Override
            public void deserialize(IonReader reader,
                                    List<String> expectedSymbols)
            {
                reader.next();
                reader.stepIn();
                Iterator<String> expectedSymbolsIterator = expectedSymbols.iterator();
                for (int i = 0; i < UNIQUE_SYMBOLS; i++)
                {
                    reader.next();
                    deserializeNext(reader, expectedSymbolsIterator, null);
                }
                reader.stepOut();
            }

            @Override
            protected List<String> serializeNext(_Private_IonRawWriter writer,
                                           SymbolCombo delegate)
                throws IOException
            {
                return FIELDS.serializeNext(writer, VALUES);
            }

            @Override
            protected void deserializeNext(IonReader reader,
                                           Iterator<String> expectedSymbols,
                                           SymbolCombo delegate)
            {
                FIELDS.deserializeNext(reader, expectedSymbols, VALUES);
            }

        },
        /**
         * Serialize and deserialize symbols as symbol values with annotations
         * within a struct (i.e. with field name symbols).
         */
        FIELDS_AND_ANNOTATIONS_AND_VALUES
        {

            @Override
            public List<String> serialize(_Private_IonRawWriter writer)
                throws IOException
            {
                List<String> writtenSymbols = new ArrayList<String>();
                writer.stepIn(IonType.STRUCT);
                for (int i = 0; i < UNIQUE_SYMBOLS; i++)
                {
                    writtenSymbols.addAll(serializeNext(writer, null));
                }
                writer.stepOut();
                return writtenSymbols;
            }

            @Override
            public void deserialize(IonReader reader,
                                    List<String> expectedSymbols)
            {
                reader.next();
                reader.stepIn();
                Iterator<String> expectedSymbolsIterator = expectedSymbols.iterator();
                for (int i = 0; i < UNIQUE_SYMBOLS; i++)
                {
                    reader.next();
                    deserializeNext(reader, expectedSymbolsIterator, null);
                }
                reader.stepOut();
            }

            @Override
            protected List<String> serializeNext(_Private_IonRawWriter writer,
                                                 SymbolCombo delegate)
                throws IOException
            {
                return FIELDS.serializeNext(writer, ANNOTATIONS_AND_VALUES);
            }

            @Override
            protected void deserializeNext(IonReader reader,
                                           Iterator<String> expectedSymbols,
                                           SymbolCombo delegate)
            {
                FIELDS.deserializeNext(reader, expectedSymbols, ANNOTATIONS_AND_VALUES);
            }

        };

        // An endless and centralized supply of symbols. Useful for chaining.
        private static final Iterator<String> circularSymbolsList = new Iterator<String>()
            {

                private int i = 0;

                public boolean hasNext()
                {
                    return true;
                }

                public String next()
                {
                    return symbolsList.get(i++ % symbolsList.size());
                }

                public void remove()
                {
                    throw new UnsupportedOperationException();
                }

        };

        public abstract List<String> serialize(_Private_IonRawWriter writer) throws IOException;
        public abstract void deserialize(IonReader reader, List<String> expectedSymbols);
        protected abstract List<String> serializeNext(_Private_IonRawWriter writer, SymbolCombo delegate) throws IOException;
        protected abstract void deserializeNext(IonReader reader, Iterator<String> expectedSymbols, SymbolCombo delegate);
    }

    private static final List<String> symbolsList = getStrings("good/symbols.ion");
    private static final int UNIQUE_SYMBOLS = symbolsList.size();
    private static final SymbolTable sharedTable = system.newSharedSymbolTable("shared", 1, symbolsList.iterator());

    private static int getLocalSid(String symbolText)
    {
        return sharedTable.findSymbol(symbolText) + system.getSystemSymbolTable().getMaxId();
    }

    @Inject("symbolCombo")
    public static SymbolCombo[] SYMBOL_COMBOS = SymbolCombo.values();

    private SymbolCombo mySymbolCombo;

    public void setSymbolCombo(SymbolCombo symbolCombo)
    {
        mySymbolCombo = symbolCombo;
    }

    @Test
    public void testDirectWriteSymbolId() throws Exception
    {
        new Roundtrip(sharedTable)
        {

            List<String> expectedSymbols = null;

            @Override
            void write(_Private_IonManagedWriter managedWriter, _Private_IonRawWriter rawWriter)
                throws IOException
            {
                managedWriter.requireLocalSymbolTable(); // starts the LST
                expectedSymbols = mySymbolCombo.serialize(rawWriter);
            }

            @Override
            void read(IonReader reader)
            {
                mySymbolCombo.deserialize(reader, expectedSymbols);
            }

        }.test();

    }

}
