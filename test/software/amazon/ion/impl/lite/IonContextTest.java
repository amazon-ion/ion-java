/*
 * Copyright 2013-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion.impl.lite;

import static software.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static software.amazon.ion.Symtabs.CATALOG;

import org.junit.Test;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonList;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonSymbol;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonTestCase;
import software.amazon.ion.IonValue;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.Symtabs;
import software.amazon.ion.impl.lite.ContainerlessContext;
import software.amazon.ion.impl.lite.IonContainerLite;
import software.amazon.ion.impl.lite.IonContext;
import software.amazon.ion.impl.lite.IonValueLite;
import software.amazon.ion.junit.Injected.Inject;


public class IonContextTest
    extends IonTestCase
{

    @Inject("inputContextMaker")
    public static final InputContextMaker[] INPUT_CONTEXT_MAKERS =
        InputContextMaker.values();

    @Inject("outputContext")
    public static final OutputContext[] OUTPUT_CONTEXTS =
        OutputContext.values();

    private InputContextMaker    myInputContextMaker;
    private OutputContext        myOutputContext;

    //==========================================================================
    // Setters/Getters for injected values

    public InputContextMaker getInputContextMaker()
    {
        return myInputContextMaker;
    }

    public void setInputContextMaker(InputContextMaker inputContextMaker)
    {
        myInputContextMaker = inputContextMaker;
    }

    public OutputContext getOutputContext()
    {
        return myOutputContext;
    }

    public void setOutputContext(OutputContext outputContext)
    {
        myOutputContext = outputContext;
    }

    //=========================================================================

    /**
     * Abstracts the various ways that an {@link IonValue} can be created with
     * different {@link IonContext}s. We specifically want to load the DOM
     * from binary data, so it has symbol IDs for every symbol token.
     */
    public enum InputContextMaker
    {
        INPUT_TOP_LEVEL_CONTEXT
        {
            @Override
            public IonValue newIonValue(IonSystem system, String ionText)
            {
                byte[] valBytes = system.getLoader().load(ionText).getBytes();

                IonValue val = system.singleValue(valBytes);

                if (val instanceof IonValueLite)
                {
                    IonContext context = ((IonValueLite) val).getContext();
                    assertTrue("Expected StubContext but was " + context,
                               context instanceof ContainerlessContext);
                }

                return val;
            }
        },
        INPUT_ION_CONTAINER_CONTEXT
        {
            @Override
            public IonValue newIonValue(IonSystem system, String ionText)
            {
                // Embed our test value inside a list and encode the result.
                IonValue val = system.singleValue(ionText);
                IonList list = system.newList(val);
                IonDatagram dg = system.newDatagram(list);
                byte[] valBytes = dg.getBytes();

                dg = system.getLoader().load(valBytes);
                list = (IonList) dg.get(0);
                val = list.get(0);

                if (val instanceof IonValueLite)
                {
                    IonContext context = ((IonValueLite) val).getContext();
                    assertTrue("Expected IonContainerLite but was " + context,
                               context instanceof IonContainerLite);
                }

                val.removeFromContainer();
                return val;
            }
        },
        INPUT_ION_SYSTEM_CONTEXT
        {
            @Override
            public IonValue newIonValue(IonSystem system, String ionText)
            {
                byte[] valBytes = system.getLoader().load(ionText).getBytes();

                IonDatagram datagram = system.getLoader().load(valBytes);

                IonValue val = datagram.get(0);
                val.removeFromContainer();

                if (val instanceof IonValueLite)
                {
                    IonContext context = ((IonValueLite) val).getContext();
                    assertTrue("Expected StubContext but was " + context,
                               context instanceof ContainerlessContext);
                }

                return val;
            }
        };

        public abstract IonValue newIonValue(IonSystem system, String ionText);
    }

    //=========================================================================

    /**
     * Abstracts the various places that a value can be added to a DOM.
     */
    public enum OutputContext
    {
        FRESH_DATAGRAM
        {
            @Override
            public void addToContainer(IonSystem system, IonValue value,
                                       boolean usePadding)
            {
                IonDatagram dg = system.newDatagram();
                if (usePadding) {
                    dg.add().newSymbol("padding");
                }
                dg.add(value);
            }
        },
        FRESH_DATAGRAM_WITH_IMPORT
        {
            @Override
            public void addToContainer(IonSystem system, IonValue value,
                                       boolean usePadding)
            {
                SymbolTable symtab = CATALOG.getTable(Symtabs.FRED_NAME);
                IonDatagram dg = system.newDatagram(symtab);
                if (usePadding) {
                    dg.add().newSymbol("padding");
                }
                dg.add(value);
            }
        },
        ENCODED_DATAGRAM
        {
            @Override
            public void addToContainer(IonSystem system, IonValue value,
                                       boolean usePadding)
            {
                IonDatagram dg = system.newDatagram();
                if (usePadding) {
                    dg.add().newSymbol("padding");
                }

                byte[] bytes = dg.getBytes();

                dg = system.getLoader().load(bytes);

                dg.add(value);
            }
        },
        ENCODED_DATAGRAM_WITH_IMPORT
        {
            @Override
            public void addToContainer(IonSystem system, IonValue value,
                                       boolean usePadding)
            {
                SymbolTable symtab = CATALOG.getTable(Symtabs.FRED_NAME);
                IonDatagram dg = system.newDatagram(symtab);
                if (usePadding) {
                    dg.add().newSymbol("padding");
                }

                byte[] bytes = dg.getBytes();

                dg = system.getLoader().load(bytes);

                dg.add(value);
            }
        },
        FRESH_STRUCT
        {
            @Override
            public void addToContainer(IonSystem system, IonValue value,
                                       boolean usePadding)
            {
                IonStruct c = system.newEmptyStruct();
                if (usePadding) {
                    c.add("pad").newSymbol("padding");
                }
                c.add("value", value);
            }
        },
        TOP_STRUCT
        {
            @Override
            public void addToContainer(IonSystem system, IonValue value,
                                       boolean usePadding)
            {
                IonDatagram dg = system.newDatagram();
                if (usePadding) {
                    dg.add().newSymbol("padding");
                }
                IonStruct c = dg.add().newEmptyStruct();
                if (usePadding) {
                    c.add("pad").newSymbol("padding");
                }
                c.add("value", value);
            }
        },
        ENCODED_STRUCT
        {
            @Override
            public void addToContainer(IonSystem system, IonValue value,
                                       boolean usePadding)
            {
                IonDatagram dg = system.newDatagram();
                if (usePadding) {
                    dg.add().newSymbol("padding");
                }
                IonStruct c = dg.add().newEmptyStruct();
                if (usePadding) {
                    c.add("pad").newSymbol("padding");
                }
                byte[] bytes = dg.getBytes();

                dg = system.getLoader().load(bytes);
                c = (IonStruct) dg.get(usePadding? 1 : 0);
                c.add("value", value);
            }
        },
        FRESH_LIST
        {
            @Override
            public void addToContainer(IonSystem system, IonValue value,
                                       boolean usePadding)
            {
                IonList c = system.newEmptyList();
                if (usePadding) {
                    c.add().newSymbol("padding");
                }
                c.add(value);
            }
        },
        TOP_LIST
        {
            @Override
            public void addToContainer(IonSystem system, IonValue value,
                                       boolean usePadding)
            {
                IonDatagram dg = system.newDatagram();
                if (usePadding) {
                    dg.add().newSymbol("padding");
                }
                IonList c = dg.add().newEmptyList();
                if (usePadding) {
                    c.add().newSymbol("padding");
                }
                c.add(value);
            }
        },
        ENCODED_LIST
        {
            @Override
            public void addToContainer(IonSystem system, IonValue value,
                                       boolean usePadding)
            {
                IonDatagram dg = system.newDatagram();
                if (usePadding) {
                    dg.add().newSymbol("padding");
                }
                IonList c = dg.add().newEmptyList();
                if (usePadding) {
                    c.add().newSymbol("padding");
                }

                byte[] bytes = dg.getBytes();

                dg = system.getLoader().load(bytes);
                c = (IonList) dg.get(usePadding ? 1 : 0);
                c.add(value);
            }
        };

        public abstract void
        addToContainer(IonSystem system, IonValue value, boolean usePadding);
    }


    //=========================================================================


    private void checkSymbolToken(String expectedText,
                                  SymbolTable symtab, SymbolToken tok)
    {
        String text = tok.assumeText();
        assertEquals(expectedText, text);

        int sid = tok.getSid();

        String msg = "text:" + text + " sid:" + sid;

        if (sid != UNKNOWN_SYMBOL_ID)
        {
            assertEquals(msg, text, symtab.findKnownSymbol(sid));
            assertEquals(msg, sid,  symtab.findSymbol(text));
        }
    }


    private void testInsertion(boolean usePadding)
        throws Exception
    {
        // String representation of the IonValue (input IonContext) to be added
        // into an IonContainer (output IonContext).
        // We cover local, system, and shared symbols here.
        String ionText = "ann1::{a:sym, b:name, c:ann2::fred_1}";

        IonStruct struct = (IonStruct)
            getInputContextMaker().newIonValue(system(), ionText);

        getOutputContext().addToContainer(system(), struct, usePadding);

        SymbolTable symtab = struct.getSymbolTable();

        checkSymbolToken("ann1", symtab, struct.getTypeAnnotationSymbols()[0]);

        IonValue elt = struct.get("a");
        checkSymbolToken("a",      symtab, elt.getFieldNameSymbol());
        checkSymbolToken("sym",    symtab, ((IonSymbol) elt).symbolValue());

        elt = struct.get("b");
        checkSymbolToken("b",      symtab, elt.getFieldNameSymbol());
        checkSymbolToken("name",   symtab, ((IonSymbol) elt).symbolValue());

        elt = struct.get("c");
        checkSymbolToken("c",      symtab, elt.getFieldNameSymbol());
        checkSymbolToken("ann2",   symtab, elt.getTypeAnnotationSymbols()[0]);
        checkSymbolToken("fred_1", symtab, ((IonSymbol) elt).symbolValue());
    }

    @Test
    public void testInsertionWithoutPadding()
        throws Exception
    {
        testInsertion(false);
    }

    /**
     * This attempts to force the input and output contexts to have distinct
     * symbol IDs.
     */
    @Test
    public void testInsertionWithPadding()
        throws Exception
    {
        testInsertion(true);
    }
}
