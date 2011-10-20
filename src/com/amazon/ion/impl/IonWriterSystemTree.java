// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl.UnifiedSymbolTable.makeNewLocalSymbolTable;

import com.amazon.ion.IonBlob;
import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonClob;
import com.amazon.ion.IonContainer;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonDecimal;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.Timestamp;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;


/**
 * Provides a concrete implementation of the IonWriter where
 * the output of the this writer is one or more IonValues,
 * typically an IonDatagram with contents.
 */
class IonWriterSystemTree
    extends IonWriterBaseImpl
{
    private final IonSystem _system;

    /** Used to construct new local symtabs. May be null */
    private final IonCatalog    _catalog;
    private boolean             _in_struct;
    private IonContainer        _current_parent;
    private int                 _parent_stack_top = 0;
    private IonContainer[]      _parent_stack = new IonContainer[10];

    private boolean             _has_binary_image;
    private IonWriter           _binary_image;
    private byte[]              _byte_image;

    /**
     * @param sys
     * @param catalog may be null.
     * @param rootContainer
     */
    protected IonWriterSystemTree(IonSystem sys, IonCatalog catalog,
                                  IonContainer rootContainer)
    {
        super(sys, sys.getSystemSymbolTable());
        _system = sys;
        _catalog = catalog;
        _current_parent = rootContainer;
        _in_struct = (_current_parent instanceof IonStruct);
    }

    // not public
    IonSystem getSystem()
    {
        return _system;
    }

    @Override
    protected void finishSystemContext() throws IOException
    {
        setSymbolTable(_default_system_symbol_table);
    }

    //
    // informational methods
    //
    public int getDepth()
    {
        return _parent_stack_top;
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
    void clearBinaryImage()
    {
        if (_has_binary_image) {
            _binary_image = null;
            _byte_image = null;
            _has_binary_image = false;
        }
    }

    @Override
    UnifiedSymbolTable inject_local_symbol_table() throws IOException
    {
        // no catalog since it doesn't matter as this is a
        // pure local table, with no imports
        // we let the system writer handle this work
        UnifiedSymbolTable symbols
            = makeNewLocalSymbolTable(_system, _system.getSystemSymbolTable());
        return symbols;
    }

    void pushParent(IonContainer newParent) {
        if (_current_parent == null) {
            // TODO document this behavior
            if (_parent_stack_top != 0) {
                throw new IllegalStateException();
            }
            _current_parent = _system.newDatagram(newParent);
        }
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

    IonValue popParent()
    {
        IonValue prior = _current_parent;

        if (_parent_stack_top < 1) {
            throw new IllegalStateException(IonMessages.CANNOT_STEP_OUT);
        }

        _parent_stack_top--;
        _current_parent = _parent_stack[_parent_stack_top];
        _in_struct = (_current_parent instanceof IonStruct);

        return prior;
    }

    void append(IonValue value)
    {
        clearBinaryImage(); // if we append anything the binary image is invalidated

        int annotation_count = this._annotation_count;
        if (annotation_count > 0) {
            String[] annotations = this.getTypeAnnotations();
            value.setTypeAnnotations(annotations);
            this.clearAnnotations();
        }
        // if they didn't give us a parent, we have to assume they
        // want a datagram :)
        if (_current_parent == null) {
            _current_parent = _system.newDatagram();
        }
        if (_symbol_table != null) {
            ((IonValuePrivate)_current_parent).setSymbolTable(_symbol_table);
            _symbol_table = null;
        }
        if (_in_struct) {
            String name = this.getFieldName();
            ((IonStruct)_current_parent).add(name, value);
            this.clearFieldName();
        }
        else {
            ((IonSequence)_current_parent).add(value);
        }
    }

    protected void appendSymbolTableValue(IonStruct symbol_table_struct)
    {
        assert(symbol_table_struct != null);
        append(symbol_table_struct);
    }

    public void stepIn(IonType containerType) throws IOException
    {
        IonContainer v;
        switch (containerType)
        {
            case LIST:   v = _system.newEmptyList();   break;
            case SEXP:   v = _system.newEmptySexp();   break;
            case STRUCT: v = _system.newEmptyStruct(); break;
            default:
                throw new IllegalArgumentException();
        }

        append(v);
        pushParent(v);
    }

    public void stepOut() throws IOException
    {
        IonValue prior = popParent();

        if (_current_parent instanceof IonDatagram
            && UnifiedSymbolTable.valueIsLocalSymbolTable(prior))
        {
            SymbolTable symbol_table =
                makeNewLocalSymbolTable(_system, _catalog, (IonStruct) prior);
            setSymbolTable(symbol_table);
            clearBinaryImage(); // changing the symbol table is likely to invalidate the image - so to be safe we do
        }
    }

    public void writeNull(IonType type)
        throws IOException
    {
        IonValue v = null;
        switch (type) {
            case NULL:
                v = _system.newNull();
                break;
            case BOOL:
                v = _system.newNullBool();
                break;
            case INT:
                v = _system.newNullInt();
                break;
            case FLOAT:
                v = _system.newNullFloat();
                break;
            case DECIMAL:
                v = _system.newNullDecimal();
                break;
            case TIMESTAMP:
                v = _system.newNullTimestamp();
                break;
            case STRING:
                v = _system.newNullString();
                break;
            case SYMBOL:
                v = _system.newNullSymbol();
                break;
            case BLOB:
                v = _system.newNullBlob();
                break;
            case CLOB:
                v = _system.newNullClob();
                break;
            case STRUCT:
                v = _system.newNullStruct();
                break;
            case LIST:
                v = _system.newNullList();
                break;
            case SEXP:
                v = _system.newNullSexp();
                break;
            default:
                throw new IllegalArgumentException();
        }
        append(v);
    }

    public void writeBool(boolean value)
        throws IOException
    {
        IonValue v = _system.newBool(value);
        append(v);
    }

    public void writeInt(int value)
        throws IOException
    {
        IonValue v = _system.newInt(value);
        append(v);
    }

    public void writeInt(long value)
        throws IOException
    {
        IonValue v = _system.newInt(value);
        append(v);
    }

    public void writeInt(BigInteger value)
        throws IOException
    {
        IonValue v = _system.newInt(value);
        append(v);
    }

    public void writeFloat(double value)
        throws IOException
    {
        IonFloat v = _system.newNullFloat();
        v.setValue(value);
        append(v);
    }

    @Override
    public void writeDecimal(BigDecimal value) throws IOException
    {
        IonDecimal v = _system.newNullDecimal();
        v.setValue(value);
        append(v);
    }

    public void writeTimestamp(Timestamp value) throws IOException
    {
        IonTimestamp v = _system.newNullTimestamp();
        if (value != null) {
            v.setValue(value);
        }
        append(v);
    }

    public void writeString(String value)
        throws IOException
    {
        IonString v = _system.newString(value);
        append(v);
    }

    public void writeIonVersionMarker() throws IOException
    {
        SymbolTable system_symbols = _system.getSystemSymbolTable();
        writeSymbol(system_symbols.getIonVersionId());
        setSymbolTable(system_symbols);
    }

    public void writeSymbol(int symbolId)
        throws IOException
    {
        String name = null;
        if (_symbol_table != null) {
            _symbol_table.findKnownSymbol(symbolId);
        }
        if (name == null) {
            throw new IllegalArgumentException("undefined symbol id");
        }
        writeSymbol(name);
    }

    public void writeSymbol(String value)
        throws IOException
    {
        IonSymbol v = _system.newSymbol(value);
        append(v);
    }

    public void writeClob(byte[] value, int start, int len)
        throws IOException
    {
        IonClob v = _system.newClob(value, start, len);
        append(v);
     }

    public void writeBlob(byte[] value, int start, int len)
        throws IOException
    {
        IonBlob v = _system.newBlob(value, start, len);
        append(v);
    }

    public void flush()
    {
        // flush is not meaningful for a tree writer
        return;
    }

    public void close()
    {
        // close is not meaningful for a tree writer
        return;
    }

}
