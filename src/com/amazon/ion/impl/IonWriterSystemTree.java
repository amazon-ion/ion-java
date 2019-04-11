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

package com.amazon.ion.impl;

import static com.amazon.ion.impl._Private_Utils.valueIsLocalSymbolTable;

import com.amazon.ion.IonBlob;
import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonClob;
import com.amazon.ion.IonContainer;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonDecimal;
import com.amazon.ion.IonException;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.ValueFactory;
import com.amazon.ion.system.IonWriterBuilder.InitialIvmHandling;
import com.amazon.ion.system.IonWriterBuilder.IvmMinimizing;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;


/**
 * Provides a concrete implementation of the IonWriter where
 * the output of the this writer is one or more IonValues,
 * typically an IonDatagram with contents.
 */
final class IonWriterSystemTree
    extends IonWriterSystem
{

    /** Factory for constructing local symbol tables. Not null. */
    private final LocalSymbolTableAsStruct.Factory _lst_factory;

    private final ValueFactory _factory;

    /** Used to construct new local symtabs. May be null */
    private final IonCatalog    _catalog;

    private final int           _initialDepth;

    private boolean             _in_struct;

    /**
     * The container into which we are currently appending values.
     * Never null.
     */
    private IonContainer        _current_parent;
    private int                 _parent_stack_top = 0;
    private IonContainer[]      _parent_stack = new IonContainer[10];


    /**
     * @param defaultSystemSymbolTable must not be null.
     * @param catalog may be null.
     * @param rootContainer must not be null.
     */
    protected IonWriterSystemTree(SymbolTable defaultSystemSymbolTable,
                                  IonCatalog catalog,
                                  IonContainer rootContainer,
                                  InitialIvmHandling initialIvmHandling)
    {
        super(defaultSystemSymbolTable, initialIvmHandling,
              IvmMinimizing.ADJACENT);

        _factory = rootContainer.getSystem();
        _lst_factory = (LocalSymbolTableAsStruct.Factory)((_Private_ValueFactory)_factory).getLstFactory();
        _catalog = catalog;
        _current_parent = rootContainer;
        _in_struct = (_current_parent instanceof IonStruct);

        int depth = 0;
        if (! (rootContainer instanceof IonDatagram)) {
            IonContainer c = rootContainer;
            do {
                depth++;
                c = c.getContainer();
            } while (c != null);
        }
        _initialDepth = depth;
    }

    //
    // informational methods
    //

    @Override
    public int getDepth()
    {
        return _parent_stack_top + _initialDepth;
    }

    protected IonType getContainer()
    {
        IonType containerType;
        if (_parent_stack_top > 0) {
            containerType = _parent_stack[_parent_stack_top-1].getType();
        }
        else {
            containerType = IonType.DATAGRAM;
        }
        return containerType;
    }
    public boolean isInStruct() {
        return _in_struct;
    }


    //
    // helpers
    //
    protected IonValue get_root()
    {
        IonValue container;
        if (_parent_stack_top > 0) {
            container = _parent_stack[0];
        }
        else {
            container = _current_parent;
        }
        return container;
    }

    /**
     * @param newParent must not be null.
     */
    private void pushParent(IonContainer newParent)
    {
        int oldlen = _parent_stack.length;
        if (_parent_stack_top >= oldlen) {
            int newlen = oldlen * 2;
            IonContainer[] temp = new IonContainer[newlen];
            System.arraycopy(_parent_stack, 0, temp, 0, oldlen);
            _parent_stack = temp;
        }
        _parent_stack[_parent_stack_top++] = _current_parent;
        _current_parent = newParent;
        _in_struct = (_current_parent instanceof IonStruct);
    }

    private void popParent()
    {
        if (_parent_stack_top < 1) {
            throw new IllegalStateException(IonMessages.CANNOT_STEP_OUT);
        }

        _parent_stack_top--;
        _current_parent = _parent_stack[_parent_stack_top];
        _in_struct = (_current_parent instanceof IonStruct);
    }

    private void append(IonValue value)
    {
        try
        {
            super.startValue();
        }
        catch (IOException e)
        {
            throw new IonException(e); // Shouldn't happen
        }

        if (hasAnnotations()) {
            SymbolToken[] annotations = getTypeAnnotationSymbols();
            // TODO this makes an extra copy of the array
            ((_Private_IonValue)value).setTypeAnnotationSymbols(annotations);
            this.clearAnnotations();
        }

        if (_in_struct) {
            SymbolToken sym = assumeFieldNameSymbol();
            IonStruct struct = (IonStruct) _current_parent;
            struct.add(sym, value);
            this.clearFieldName();
        }
        else {
            ((IonSequence)_current_parent).add(value);
        }
    }

    public void stepIn(IonType containerType) throws IOException
    {
        IonContainer v;
        switch (containerType)
        {
            case LIST:   v = _factory.newEmptyList();   break;
            case SEXP:   v = _factory.newEmptySexp();   break;
            case STRUCT: v = _factory.newEmptyStruct(); break;
            default:
                throw new IllegalArgumentException();
        }

        append(v);
        pushParent(v);
    }

    public void stepOut() throws IOException
    {
        _Private_IonValue prior = (_Private_IonValue)_current_parent;
        popParent();

        if (_current_parent instanceof IonDatagram
            && valueIsLocalSymbolTable(prior))
        {
            // We just finish writing a symbol table!
            SymbolTable symbol_table = _lst_factory.newLocalSymtab(_catalog, (IonStruct) prior);
            setSymbolTable(symbol_table);
        }
    }


    @Override
    void writeIonVersionMarkerAsIs(SymbolTable systemSymtab)
        throws IOException
    {
        startValue();
        IonValue root = get_root();
        ((_Private_IonDatagram)root).appendTrailingSymbolTable(systemSymtab);
        endValue();
    }


    @Override
    void writeLocalSymtab(SymbolTable symtab)
        throws IOException
    {
        IonValue root = get_root();
        ((_Private_IonDatagram)root).appendTrailingSymbolTable(symtab);

        super.writeLocalSymtab(symtab);
    }

    @Override
    final SymbolTable inject_local_symbol_table() throws IOException
    {
        return _lst_factory.newLocalSymtab(getSymbolTable());
    }

    //========================================================================


    public void writeNull(IonType type)
        throws IOException
    {
        IonValue v = _factory.newNull(type);
        append(v);
    }

    public void writeBool(boolean value)
        throws IOException
    {
        IonValue v = _factory.newBool(value);
        append(v);
    }

    public void writeInt(int value)
        throws IOException
    {
        IonValue v = _factory.newInt(value);
        append(v);
    }

    public void writeInt(long value)
        throws IOException
    {
        IonValue v = _factory.newInt(value);
        append(v);
    }

    public void writeInt(BigInteger value)
        throws IOException
    {
        IonValue v = _factory.newInt(value);
        append(v);
    }

    public void writeFloat(double value)
        throws IOException
    {
        IonFloat v = _factory.newNullFloat();
        v.setValue(value);
        append(v);
    }

    @Override
    public void writeDecimal(BigDecimal value) throws IOException
    {
        IonDecimal v = _factory.newNullDecimal();
        v.setValue(value);
        append(v);
    }

    public void writeTimestamp(Timestamp value) throws IOException
    {
        IonTimestamp v =  _factory.newTimestamp(value);
        append(v);
    }

    public void writeString(String value)
        throws IOException
    {
        IonString v = _factory.newString(value);
        append(v);
    }


    @Override
    void writeSymbolAsIs(int symbolId)
    {
        String name = getSymbolTable().findKnownSymbol(symbolId);
        SymbolTokenImpl is = new SymbolTokenImpl(name, symbolId);
        IonSymbol v = _factory.newSymbol(is);
        append(v);
    }

    @Override
    public void writeSymbolAsIs(String value)
    {
        IonSymbol v = _factory.newSymbol(value);
        append(v);
    }

    public void writeClob(byte[] value, int start, int len)
        throws IOException
    {
        IonClob v = _factory.newClob(value, start, len);
        append(v);
     }

    public void writeBlob(byte[] value, int start, int len)
        throws IOException
    {
        IonBlob v = _factory.newBlob(value, start, len);
        append(v);
    }

    public void flush()
    {
        // flush is not meaningful for a tree writer
    }

    public void close()
    {
        // close is not meaningful for a tree writer
    }

}
