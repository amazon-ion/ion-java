// Copyright (c) 2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static com.amazon.ion.Symtabs.CATALOG;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonList;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Symtabs;
import com.amazon.ion.junit.Injected.Inject;
import org.junit.Test;


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

    //==========================================================================

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
                    assertTrue("Expected TopLevelContext but was " + context,
                               context instanceof TopLevelContext);
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
                    assertTrue("Expected IonSystemLite but was " + context,
                               context instanceof IonSystemLite);
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
            public void addToContainer(IonSystem system, IonValue value)
            {
                IonDatagram dg = system.newDatagram();
                dg.add().newSymbol("padding");
                dg.add(value);
            }
        },
        FRESH_DATAGRAM_WITH_IMPORT
        {
            @Override
            public void addToContainer(IonSystem system, IonValue value)
            {
                SymbolTable symtab = CATALOG.getTable(Symtabs.FRED_NAME);
                IonDatagram dg = system.newDatagram(symtab);
                dg.add().newSymbol("padding");
                dg.add(value);
            }
        },
        ENCODED_DATAGRAM
        {
            @Override
            public void addToContainer(IonSystem system, IonValue value)
            {
                IonDatagram dg = system.newDatagram();
                dg.add().newSymbol("padding");
                byte[] bytes = dg.getBytes();

                dg = system.getLoader().load(bytes);

                dg.add(value);
            }
        },
        ENCODED_DATAGRAM_WITH_IMPORT
        {
            @Override
            public void addToContainer(IonSystem system, IonValue value)
            {
                SymbolTable symtab = CATALOG.getTable(Symtabs.FRED_NAME);
                IonDatagram dg = system.newDatagram(symtab);
                dg.add().newSymbol("padding");
                byte[] bytes = dg.getBytes();

                dg = system.getLoader().load(bytes);

                dg.add(value);
            }
        },
        FRESH_STRUCT
        {
            @Override
            public void addToContainer(IonSystem system, IonValue value)
            {
                IonStruct c = system.newEmptyStruct();
                c.add("pad").newSymbol("padding");
                c.add("value", value);
            }
        },
        TOP_STRUCT
        {
            @Override
            public void addToContainer(IonSystem system, IonValue value)
            {
                IonDatagram dg = system.newDatagram();
                dg.add().newSymbol("padding");
                IonStruct c = dg.add().newEmptyStruct();
                c.add("pad").newSymbol("padding");
                c.add("value", value);
            }
        },
        ENCODED_STRUCT
        {
            @Override
            public void addToContainer(IonSystem system, IonValue value)
            {
                IonDatagram dg = system.newDatagram();
                dg.add().newSymbol("padding");
                IonStruct c = dg.add().newEmptyStruct();
                c.add("pad").newSymbol("padding");
                byte[] bytes = dg.getBytes();

                dg = system.getLoader().load(bytes);
                c = (IonStruct) dg.get(1);
                c.add("value", value);
            }
        },
        FRESH_LIST
        {
            @Override
            public void addToContainer(IonSystem system, IonValue value)
            {
                IonList c = system.newEmptyList();
                c.add().newSymbol("padding");
                c.add(value);
            }
        },
        TOP_LIST
        {
            @Override
            public void addToContainer(IonSystem system, IonValue value)
            {
                IonDatagram dg = system.newDatagram();
                dg.add().newSymbol("padding");
                IonList c = dg.add().newEmptyList();
                c.add().newSymbol("padding");
                c.add(value);
            }
        },
        ENCODED_LIST
        {
            @Override
            public void addToContainer(IonSystem system, IonValue value)
            {
                IonDatagram dg = system.newDatagram();
                dg.add().newSymbol("padding");
                IonList c = dg.add().newEmptyList();
                c.add().newSymbol("padding");
                byte[] bytes = dg.getBytes();

                dg = system.getLoader().load(bytes);
                c = (IonList) dg.get(1);
                c.add(value);
            }
        };

        public abstract void
        addToContainer(IonSystem system, IonValue value);
    }


    //==========================================================================


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


    @Test
    public void testInsertion()
        throws Exception
    {
        // String representation of the IonValue (input IonContext) to be added
        // into an IonContainer (output IonContext).
        // We cover local, system, and shared symbols here.
        String ionText = "ann1::{a:sym, b:name, c:ann2::fred_1}";

        IonStruct struct = (IonStruct)
            getInputContextMaker().newIonValue(system(), ionText);

        getOutputContext().addToContainer(system(), struct);

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
}
