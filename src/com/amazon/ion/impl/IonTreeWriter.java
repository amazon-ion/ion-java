/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;



import com.amazon.ion.IonBlob;
import com.amazon.ion.IonClob;
import com.amazon.ion.IonContainer;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonDecimal;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.TtTimestamp;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;

/**
 * Provides a concrete implementation of the IonWriter where
 * the output of the this writer is one or more IonValues,
 * typically an IonDatagram with contents.
 */
public final class IonTreeWriter
    extends IonBaseWriter
{
    IonSystem           _sys;

    boolean             _in_struct;
    IonContainer        _current_parent;
    int                 _parent_stack_top = 0;
    IonContainer[]      _parent_stack = new IonContainer[10];

    public IonTreeWriter(IonSystem sys) {
        _sys = sys;
    }

    public IonTreeWriter(IonSystem sys, IonContainer rootContainer) {
        _sys = sys;
        _current_parent = rootContainer;
        _in_struct = (_current_parent instanceof IonStruct);
        setSymbolTable(rootContainer.getSymbolTable());
    }

    void initialize_symbol_table() {
        super.setSymbolTable(_sys.newLocalSymbolTable());
    }

    void pushParent(IonContainer newParent) {
        if (_current_parent == null) {
            if (_parent_stack_top != 0) {
                throw new IllegalStateException();
            }
            _current_parent = _sys.newDatagram(newParent);
        }
        if (_parent_stack_top >= _parent_stack.length) {
            int newlen = _parent_stack.length * 2;
            IonContainer[] temp = new IonContainer[newlen];
            System.arraycopy(_parent_stack, 0, temp, 0, _parent_stack_top);
            _parent_stack = temp;
        }
        _parent_stack[_parent_stack_top++] = _current_parent;
        _current_parent = newParent;
        _in_struct = (_current_parent instanceof IonStruct);
    }

    void popParent() {
        if (_parent_stack_top < 1) {
            throw new IllegalStateException();
        }
        _parent_stack_top--;
        _current_parent = _parent_stack[_parent_stack_top];
        _in_struct = (_current_parent instanceof IonStruct);
    }

    public boolean isInStruct() {
        return _in_struct;
    }

    void append(IonValue value) {
        int annotation_count = this._annotation_count;
        if (annotation_count > 0) {
            String[] annotations = this.get_annotations_as_strings();
            for (int ii=0; ii<annotation_count; ii++) {
                String annotation = annotations[ii];
                value.addTypeAnnotation(annotation);
            }
            this.clearAnnotations();
        }
        // if they didn't give us a parent, we have to assume they
        // want a datagram :)
        if (_current_parent == null) {
            _current_parent = _sys.newDatagram(value);
        }
        else {
            if (_in_struct) {
                String name = this.get_field_name_as_string();
                ((IonStruct)_current_parent).add(name, value);
                this.clearFieldName();
            }
            else {
                ((IonSequence)_current_parent).add(value);
            }
        }
    }



    public void stepIn(IonType containerType) throws IOException
    {
        IonContainer v;
        switch (containerType)
        {
            case LIST:   v = _sys.newEmptyList();   break;
            case SEXP:   v = _sys.newEmptySexp();   break;
            case STRUCT: v = _sys.newEmptyStruct(); break;
            default:
                throw new IllegalArgumentException();
        }

        append(v);
        pushParent(v);
    }


    public void stepOut() throws IOException
    {
        popParent();
    }


    public void writeNull()
        throws IOException
    {
        IonValue v = _sys.newNull();
        append(v);
    }

    public void writeNull(IonType type)
        throws IOException
    {
        IonValue v = null;
        switch (type) {
            case NULL:
                v = _sys.newNull();
                break;
            case BOOL:
                v = _sys.newNullBool();
                break;
            case INT:
                v = _sys.newNullInt();
                break;
            case FLOAT:
                v = _sys.newNullFloat();
                break;
            case DECIMAL:
                v = _sys.newNullDecimal();
                break;
            case TIMESTAMP:
                v = _sys.newNullTimestamp();
                break;
            case STRING:
                v = _sys.newNullString();
                break;
            case SYMBOL:
                v = _sys.newNullSymbol();
                break;
            case BLOB:
                v = _sys.newNullBlob();
                break;
            case CLOB:
                v = _sys.newNullClob();
                break;
            case STRUCT:
                v = _sys.newNullStruct();
                break;
            case LIST:
                v = _sys.newNullList();
                break;
            case SEXP:
                v = _sys.newNullSexp();
                break;
        }
        append(v);
    }

    public void writeBool(boolean value)
        throws IOException
    {
        IonValue v = _sys.newBool(value);
        append(v);
    }

    public void writeInt(byte value)
        throws IOException
    {
        IonValue v = _sys.newInt(value);
        append(v);
    }

    public void writeInt(short value)
        throws IOException
    {
        IonValue v = _sys.newInt(value);
        append(v);
    }

    public void writeInt(int value)
        throws IOException
    {
        IonValue v = _sys.newInt(value);
        append(v);
    }

    public void writeInt(long value)
        throws IOException
    {
        IonValue v = _sys.newInt(value);
        append(v);
    }

    public void writeFloat(float value)
        throws IOException
    {
        IonFloat v = _sys.newNullFloat();
        v.setValue(value);
        append(v);
    }

    public void writeFloat(double value)
        throws IOException
    {
        IonFloat v = _sys.newNullFloat();
        v.setValue(value);
        append(v);
    }

    public void writeDecimal(BigDecimal value)
        throws IOException
    {
        IonDecimal v = _sys.newNullDecimal();
        v.setValue(value);
        append(v);
    }

    public void writeTimestamp(TtTimestamp value) throws IOException
    {
        IonTimestamp v = _sys.newNullTimestamp();
        if (value != null) {
            v.setValue(value);
        }
        append(v);
    }

    public void writeTimestamp(Date value, Integer localOffset)
        throws IOException
    {
        IonTimestamp v = _sys.newUtcTimestamp(value);
        v.setLocalOffset(localOffset);        // FIXME failure if value==null
        append(v);
    }

    public void writeTimestampUTC(Date value)
        throws IOException
    {
        IonTimestamp v = _sys.newUtcTimestamp(value);
        append(v);
    }

    public void writeString(String value)
        throws IOException
    {
        IonValue v = _sys.newString(value);
        append(v);
    }

    public void writeSymbol(int symbolId)
        throws IOException
    {
        if (_symbol_table == null) {
            initialize_symbol_table();
        }

        String name = _symbol_table.findKnownSymbol(symbolId);
        if (name == null) {
            throw new IllegalArgumentException("undefined symbol id");
        }
        writeSymbol(name);
    }

    public void writeSymbol(String value)
        throws IOException
    {
        IonValue v = _sys.newSymbol(value);
        append(v);
    }

    public void writeClob(byte[] value)
        throws IOException
    {
        IonClob v = _sys.newNullClob();
        v.setBytes(value);
        append(v);
    }

    public void writeClob(byte[] value, int start, int len)
        throws IOException
    {
        IonClob v = _sys.newNullClob();
        byte[] bytes = new byte[len];
        System.arraycopy(value, start, bytes, 0, len);
        v.setBytes(bytes);
        append(v);
     }

    public void writeBlob(byte[] value)
        throws IOException
    {
        IonBlob v = _sys.newNullBlob();
        v.setBytes(value);
        append(v);
    }

    public void writeBlob(byte[] value, int start, int len)
        throws IOException
    {
        IonBlob v = _sys.newNullBlob();
        byte[] bytes = new byte[len];
        System.arraycopy(value, start, bytes, 0, len);
        v.setBytes(bytes);
        append(v);
    }

    public IonValue getContentAsIonValue()
        throws IOException
    {
        IonValue v = null;
        if (_parent_stack_top != 0) {
            throw new IllegalStateException("not all containers are closed");
        }
        v = _current_parent;
        if (!(v instanceof IonDatagram)) {
            v = this._sys.newDatagram(v);
        }
        return v;
    }
    public byte[] getBytes()
        throws IOException
    {
        IonValue v = getContentAsIonValue();
        if (!(v instanceof IonDatagram)) {
            throw new IllegalStateException("bytes are only available on datagrams");
        }

        IonDatagram dg = (IonDatagram)v;
        int len = dg.byteSize();
        byte[] bytes = new byte[len];
        dg.getBytes(bytes);

        return bytes;
    }

    public int getBytes(byte[] bytes, int offset, int maxlen)
        throws IOException
    {
        IonValue v = getContentAsIonValue();
        if (!(v instanceof IonDatagram)) {
            throw new IllegalStateException("bytes are only available on datagrams");
        }

        IonDatagram dg = (IonDatagram)v;
        int len = dg.getBytes(bytes, offset);
        if (len > maxlen) {
            throw new IllegalStateException("byte output overflowed max len!");
        }
        return len;
    }


    public int writeBytes(SimpleByteBuffer.SimpleByteWriter out) // OutputStream out)
        throws IOException
    {
        IonValue v = getContentAsIonValue();
        if (!(v instanceof IonDatagram)) {
            throw new IllegalStateException("bytes are only available on datagrams");
        }

        IonDatagram dg = (IonDatagram)v;
        int len = dg.getBytes(out);
        return len;
    }

}
