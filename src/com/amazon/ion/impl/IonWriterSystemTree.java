// Copyright (c) 2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonBlob;
import com.amazon.ion.IonClob;
import com.amazon.ion.IonContainer;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonDecimal;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonIterationType;
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
public class IonWriterSystemTree
    extends IonWriterBaseImpl
{
    boolean             _in_struct;
    IonContainer        _current_parent;
    int                 _parent_stack_top = 0;
    IonContainer[]      _parent_stack = new IonContainer[10];

    boolean             _has_binary_image;
    IonWriter           _binary_image;
    byte[]              _byte_image;

    protected IonWriterSystemTree(IonSystem sys, IonContainer rootContainer)
    {
        super(sys);
        //_sys = sys;
        _current_parent = rootContainer;
        _in_struct = (_current_parent instanceof IonStruct);
    }

    //
    // informational methods
    //
    public IonIterationType getIterationType()
    {
        return IonIterationType.SYSTEM_ION_VALUE;
    }
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
            for (int ii=0; ii<annotation_count; ii++) {
                String annotation = annotations[ii];
                value.addTypeAnnotation(annotation);
            }
            this.clearAnnotations();
        }
        // if they didn't give us a parent, we have to assume they
        // want a datagram :)
        if (_current_parent == null) {
            _current_parent = _system.newDatagram();
        }
        if (_in_struct) {
            String name = this.getFieldName();
            ((IonStruct)_current_parent).add(name, value);
            this.clearFieldName();
        }
        else {
            if (_current_parent instanceof IonDatagram) {
                ((IonValueImpl)value).setSymbolTable(_symbol_table);
            }
            ((IonSequence)_current_parent).add(value);
        }
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

        if (_current_parent instanceof IonDatagram && prior instanceof IonStruct)
        {
            if (UnifiedSymbolTable.valueIsLocalSymbolTable(prior))
            {
                SymbolTable symbol_table;
                symbol_table = UnifiedSymbolTable.makeNewLocalSymbolTable(
                                    _system.getSystemSymbolTable()
                                  , (IonStruct) prior
                                  , _system.getCatalog()
                                );
                setSymbolTable(symbol_table);
                clearBinaryImage(); // changing the symbol table is likely to invalidate the image - so to be safe we do
            }
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

    public void flush() throws IOException
    {
        // flush is not meaningful for a tree writer
        return;
    }

}
