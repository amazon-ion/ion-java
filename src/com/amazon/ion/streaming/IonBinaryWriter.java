/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.streaming;

import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.BlockedBuffer;
import com.amazon.ion.impl.IonBinary;
import com.amazon.ion.impl.IonTimestampImpl;
import com.amazon.ion.impl.IonBinary.BufferManager;
import com.amazon.ion.impl.IonConstants;
import com.amazon.ion.impl.IonTokenReader.Type.timeinfo;
import com.amazon.ion.streaming.SimpleByteBuffer.SimpleByteWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Iterator;

/**
 * implementation of the IonWrite where the output is
 * Ion binary formatted bytes.  This will include a local
 * symbol table in the output if a symbol table is necessary.
 */
public final class IonBinaryWriter
    extends IonBaseWriter
{
static final boolean _verbose_debug = false;
    
    
    BufferManager _manager;
    IonBinary.Writer _writer;
    
    boolean     _in_struct;
    SymbolTable _system_symbols;
    
    int    _patch_count = 0;
    int [] _patch_lengths = new int[10];
    int [] _patch_offsets = new int[10];
    int [] _patch_types = new int[10];
    boolean [] _patch_in_struct = new boolean[10];
    
    int    _top;
    int [] _patch_stack = new int[10];
    
    public IonBinaryWriter() {
        _manager = new BufferManager();
        _writer = _manager.openWriter();
    }
    public boolean isInStruct() {
        return this._in_struct;
    }
    void push(int typeid) {
        int pos = _writer.position();
        if (_top >= _patch_stack.length) {
            growStack();
        }
        if (_patch_count >= _patch_lengths.length) {// _patch_list.length) {
            growList();
        }
        _patch_stack[_top++] = _patch_count;
        
        _patch_lengths[_patch_count] = 0; 
        _patch_offsets[_patch_count] = pos;
        _patch_types[_patch_count] = typeid;
        _patch_in_struct[_patch_count] = _in_struct;
        _patch_count++;
    }
    void growStack() {
        int newlen = _patch_stack.length * 2;
        int[] temp = new int[newlen];
        System.arraycopy(_patch_stack, 0, temp, 0, _top - 1);
        _patch_stack = temp;
    }
    void growList() {
        int newlen = _patch_lengths.length * 2; // _patch_list.length * 2;
        int[] temp1 = new int[newlen];
        int[] temp2 = new int[newlen];
        int[] temp3 = new int[newlen];
        boolean[] temp4 = new boolean[newlen];

        System.arraycopy(_patch_lengths,   0, temp1, 0, _patch_count);  
        System.arraycopy(_patch_offsets,   0, temp2, 0, _patch_count);
        System.arraycopy(_patch_types,     0, temp3, 0, _patch_count);
        System.arraycopy(_patch_in_struct, 0, temp4, 0, _patch_count);
        
        _patch_lengths   = temp1;  
        _patch_offsets   = temp2;
        _patch_types     = temp3;
        _patch_in_struct = temp4;

    }
    void patch(int addedLength) {
        for (int ii = 0; ii < _top; ii++) {
            _patch_lengths[_patch_stack[ii]] += addedLength;
        }
    }
    
    void pop() {
        // first grab the length since this container will now be
        // closed and fixed, we'll back patch it's len 'o len into
        // it's parents -- after we pop it off the stack
        int len = topLength();
        int lenolen = IonBinary.lenLenFieldWithOptionalNibble(len);
        _top--;
        if (lenolen > 0) {
            patch(lenolen);
        }
    }
    int topLength() {
        return _patch_lengths[_patch_stack[_top - 1]];
    }
    int topPosition(){
        return _patch_offsets[_patch_stack[_top - 1]];
    }
    int topType() {
        return _patch_types[_patch_stack[_top - 1]];
    }
    boolean topInStruct() {
        if (_top == 0) return false;
        boolean in_struct = _patch_in_struct[_patch_stack[_top - 1]];
        return in_struct;
    }
    
    void startValue() throws IOException {
        
        // start a local symbol table if necessary
        if (_symbol_table == null) {
            _symbol_table = UnifiedSymbolTable.getSystemSymbolTableInstance();
            _system_symbols = _symbol_table;
            _no_local_symbols = true;
        }
        
        // write field name
        if (_in_struct) {
            int sid = super.get_field_name_as_int();
            if (sid < 1) {
                throw new IllegalStateException();
            }
            patch(_writer.writeVarUInt7Value(sid, true));
            super.clearFieldName();
        }

        // write annotations
        int len = 0;
        int sid_count = super._annotation_count;
        if (sid_count > 0) {
            int[] sids = super.get_annotations_as_ints();

            // FIRST add up the length
            for (int ii=0; ii<sid_count; ii++) {
                if (sids[ii] < 1) {
                    throw new IllegalStateException();
                }
                len += IonBinary.lenVarUInt7(sids[ii]);
            }

            // THEN write the td byte, optional annotations, this is before the caller
            //      writes out the actual values (plain value) td byte and varlen
            //      an annotation is just like any parent collection header in that it needs
            //      to be in our stack for length patching purposes
            patch(1);
            push(IonConstants.tidTypedecl);
            _writer.write((byte)(IonConstants.tidTypedecl << 4));
            
            len += _writer.writeVarUInt7Value(len, true);                              /// CAS late night added "len +="
            for (int ii=0; ii<sid_count; ii++) {
                // note that len already has the sum of the actual lengths
                // added into it so that we could write it out in front
                _writer.writeVarUInt7Value(sids[ii], true);
            }
            patch(len);
            super.clearAnnotations();
        }
    }
    
    void closeValue() {
        if (_top > 0) {
            // check for annotations, which we need to pop off now
            // since once we close a value out, we won't need to path
            // the annotations it (might have) had
            if (topType() == IonConstants.tidTypedecl) {
                pop();
            }
        }
    }

    public void startList() throws IOException
    {
        startValue();
        patch(1);
        _in_struct = false;
        push(IonConstants.tidList);        
        _writer.writeByte((byte)(IonConstants.tidList << 4));
    }
    public void startSexp() throws IOException
    {
        startValue();
        patch(1);
        _in_struct = false;
        push(IonConstants.tidSexp);        
        _writer.writeByte((byte)(IonConstants.tidSexp << 4));
    }
    public void startStruct() throws IOException
    {
        startValue();
        patch(1);
        _in_struct = true;
        push(IonConstants.tidStruct);        
        _writer.writeByte((byte)(IonConstants.tidStruct << 4));
    }

    public void closeList()
    {
        pop();
        closeValue();
        _in_struct = this.topInStruct();
    }
    public void closeSexp()
    {
        pop();
        closeValue();
        _in_struct = this.topInStruct();
    }
    public void closeStruct()
    {
        pop();
        closeValue();
        _in_struct = this.topInStruct();
    }

    @Override
    public void writeFieldname(String name)
    {
        if (!_in_struct) {
            throw new IllegalStateException();
        }
        super.writeFieldname(name);
    }

    @Override
    public void writeFieldnameId(int id)
    {
        if (!_in_struct) {
            throw new IllegalStateException();
        }
        super.writeFieldnameId(id);
    }

    public void writeNull() throws IOException
    {
        startValue();
        _writer.write(IonConstants.tidNull << 4);
        patch(1);
    }
    public void writeNull(IonType type) throws IOException
    {
        startValue();
        int tid = -1;
        switch (type) {
        case NULL: tid = IonConstants.tidNull; break;
        case BOOL: tid = IonConstants.tidBoolean; break;
        case INT: tid = IonConstants.tidPosInt; break;
        case FLOAT: tid = IonConstants.tidFloat; break;
        case DECIMAL: tid = IonConstants.tidDecimal; break;
        case TIMESTAMP: tid = IonConstants.tidTimestamp; break;
        case SYMBOL: tid = IonConstants.tidSymbol; break;
        case STRING: tid = IonConstants.tidString; break;
        case BLOB: tid = IonConstants.tidBlob; break;
        case CLOB: tid = IonConstants.tidClob; break;
        case SEXP: tid = IonConstants.tidSexp; break;
        case LIST: tid = IonConstants.tidList; break;
        case STRUCT: tid = IonConstants.tidStruct; break;
        }
        _writer.write((tid << 4) | IonConstants.lnIsNullAtom);
        patch(1);
    }
    public void writeBool(boolean value) throws IOException
    {
        startValue();
        int ln = value ? IonConstants.lnBooleanTrue : IonConstants.lnBooleanFalse; 
        _writer.write((IonConstants.tidBoolean << 4) | ln);
        patch(1);
    }
    public void writeInt(byte value) throws IOException
    {
        startValue();
        int len = IonBinary.lenIonInt(value);
        if (value < 0) {
            _writer.write((IonConstants.tidNegInt << 4) | len);
            _writer.writeVarUInt8Value(-value, len);
        }
        else {
            _writer.write((IonConstants.tidPosInt << 4) | len);
            _writer.writeVarUInt8Value(value, len);
        }
        patch(1 + len);
    }
    public void writeInt(short value) throws IOException
    {
        startValue();
        int len = IonBinary.lenIonInt(value);
        if (value < 0) {
            _writer.write((IonConstants.tidNegInt << 4) | len);
            _writer.writeVarUInt8Value(-value, len);
        }
        else {
            _writer.write((IonConstants.tidPosInt << 4) | len);
            _writer.writeVarUInt8Value(value, len);
        }
        patch(1 + len);
    }
    public void writeInt(int value) throws IOException
    {
        startValue();
        int len = IonBinary.lenIonInt(value);
        if (value < 0) {
            _writer.write((IonConstants.tidNegInt << 4) | len);
            _writer.writeVarUInt8Value(-value, len);
        }
        else {
            _writer.write((IonConstants.tidPosInt << 4) | len);
            _writer.writeVarUInt8Value(value, len);
        }
        patch(1 + len);
    }
    public void writeInt(long value) throws IOException
    {
        startValue();
        int len = IonBinary.lenIonInt(value);
        if (value < 0) {
            _writer.write((IonConstants.tidNegInt << 4) | len);
            _writer.writeVarUInt8Value(-value, len);
        }
        else {
            _writer.write((IonConstants.tidPosInt << 4) | len);
            _writer.writeVarUInt8Value(value, len);
        }
        patch(1 + len);
    }
    public void writeFloat(float value) throws IOException
    {
        double dvalue = value;
        startValue();
        int len = IonBinary.lenIonFloat(dvalue);
        _writer.write((IonConstants.tidFloat << 4) | len);
        len = _writer.writeFloatValue(dvalue);
        patch(1 + len);
    }
    public void writeFloat(double value) throws IOException
    {
        startValue();
        int len = IonBinary.lenIonFloat(value);
        _writer.write((IonConstants.tidFloat << 4) | len);
        len = _writer.writeFloatValue(value);
        patch(1 + len);
    }
    public void writeDecimal(BigDecimal value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.DECIMAL);
            return;
        }
        startValue();
        int patch_len = 1;
        int len = IonBinary.lenIonDecimal(value);
        int ln = (len < IonConstants.lnIsVarLen) ? len : IonConstants.lnIsVarLen; 
        _writer.write((IonConstants.tidDecimal << 4) | ln);
        if (ln == IonConstants.lnIsVarLen) {
            patch_len += _writer.writeVarUInt7Value(len, true);
        }
        patch_len += _writer.writeDecimalContent(value);
        patch(patch_len);
    }

    public void writeTimestampUTC(Date value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.TIMESTAMP);
            return;
        }
        timeinfo di = new timeinfo();
        di.d = value;
        di.localOffset = null; // IonTimestampImpl.UTC_OFFSET;
        
        startValue();
        int patch_len = 1;
        int len = IonBinary.lenIonTimestamp(di);
        int ln = (len < IonConstants.lnIsVarLen) ? len : IonConstants.lnIsVarLen; 
        _writer.write((IonConstants.tidTimestamp << 4) | ln);
        if (ln == IonConstants.lnIsVarLen) {
            patch_len += _writer.writeVarUInt7Value(len, true);
        }
        patch_len += _writer.writeTimestamp(di);
        patch(patch_len);
    }
    
    public void writeTimestamp(Date value, int localOffset) throws IOException
    {
        if (value == null) {
            writeNull(IonType.TIMESTAMP);
            return;
        }
        timeinfo di = new timeinfo();
        di.d = value;
        di.localOffset = localOffset;
        
        startValue();
        int patch_len = 1;
        int len = IonBinary.lenIonTimestamp(di);
        int ln = (len < IonConstants.lnIsVarLen) ? len : IonConstants.lnIsVarLen; 
        _writer.write((IonConstants.tidTimestamp << 4) | ln);
        if (ln == IonConstants.lnIsVarLen) {
            patch_len += _writer.writeVarUInt7Value(len, true);
        }
        patch_len += _writer.writeTimestamp(di);
        patch(patch_len);
    }

    public void writeString(String value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.STRING);
            return;
        }
        
        startValue();
        int patch_len = 1;
        int len = IonBinary.lenIonString(value);
        int ln = (len < IonConstants.lnIsVarLen) ? len : IonConstants.lnIsVarLen; 
        _writer.write((IonConstants.tidString << 4) | ln);
        if (ln == IonConstants.lnIsVarLen) {
            patch_len += _writer.writeVarUInt7Value(len, true);
        }
        patch_len += _writer.writeStringData(value);
        patch(patch_len);
    }

    public void writeSymbol(int symbolId) throws IOException
    {
        startValue();
        int patch_len = 1;
        int len = IonBinary.lenVarUInt8(symbolId);
        _writer.write((IonConstants.tidSymbol << 4) | len);
        patch_len += _writer.writeVarUInt8Value(symbolId, len);
        patch(patch_len);
    }

    public void writeSymbol(String value) throws IOException
    {
        int symbolId = makeSymbol(value);
        writeSymbol(symbolId);
    }
    int makeSymbol(String name) {
        int sid = _symbol_table.findSymbol(name);
        if (sid > 0) return sid;
        
        sid = _system_symbols.findSymbol(name);
        if (sid > 0) return sid;
        
        if (_no_local_symbols) {
            _symbol_table = new UnifiedSymbolTable((UnifiedSymbolTable)_system_symbols);
            _no_local_symbols = false;
        }
        
        sid = _symbol_table.addSymbol(name);
        return sid;
    }


    public void writeClob(byte[] value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.CLOB);
            return;
        }
        int len = value.length;
        writeClob(value, 0, len);
    }

    public void writeClob(byte[] value, int start, int len) throws IOException
    {
        startValue();
        int patch_len = 1;
        int ln = (len < IonConstants.lnIsVarLen) ? len : IonConstants.lnIsVarLen; 
        _writer.write((IonConstants.tidClob << 4) | ln);
        if (ln == IonConstants.lnIsVarLen) {
            patch_len += _writer.writeVarUInt7Value(len, true);
        }
        _writer.write(value, start, len);
        patch_len += len;
        patch(patch_len);
    }

    public void writeBlob(byte[] value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.BLOB);
            return;
        }
        int len = value.length;
        writeBlob(value, 0, len);
    }

    public void writeBlob(byte[] value, int start, int len) throws IOException
    {
        startValue();
        int patch_len = 1;
        int ln = (len < IonConstants.lnIsVarLen) ? len : IonConstants.lnIsVarLen; 
        _writer.write((IonConstants.tidBlob << 4) | ln);
        if (ln == IonConstants.lnIsVarLen) {
            patch_len += _writer.writeVarUInt7Value(len, true);
        }
        _writer.write(value, start, len);
        patch_len += len;
        patch(patch_len);
    }

    static final int bool_true = (IonConstants.tidBoolean << 4) | IonConstants.lnBooleanTrue;
    static final int bool_false = (IonConstants.tidBoolean << 4) | IonConstants.lnBooleanFalse;
    @Override
    public void writeBoolList(boolean[] values) throws IOException
    {
        int len = values.length;
        startValue();
        int patch_len = 1;
        int ln = (len < IonConstants.lnIsVarLen) ? len : IonConstants.lnIsVarLen; 
        _writer.write((IonConstants.tidList << 4) | ln);
        if (ln == IonConstants.lnIsVarLen) {
            patch_len += _writer.writeVarUInt7Value(len, true);
        }
        for (int ii=0; ii<len; ii++) {
            _writer.write(values[ii] ? bool_true : bool_false);
        }
        patch_len += len;
        patch(patch_len);
    }
    static final int int_tid_0 = (IonConstants.tidPosInt << 4);
    static final int int_tid_pos = (IonConstants.tidPosInt << 4) | 1;
    static final int int_tid_neg = (IonConstants.tidNegInt << 4) | 1;
    @Override
    public void writeIntList(byte[] values) throws IOException
    {
        startValue();
        int patch_len = 1;
        int len = 0;
        for (int ii=0; ii<values.length; ii++) {
            len++;
            if (values[ii] != 0) len++;
        }
        int ln = (len < IonConstants.lnIsVarLen) ? len : IonConstants.lnIsVarLen; 
        _writer.write((IonConstants.tidList << 4) | ln);
        if (ln == IonConstants.lnIsVarLen) {
            patch_len += _writer.writeVarUInt7Value(len, true);
        }
        for (int ii=0; ii<len; ii++) {
            int v = values[ii];
            if (v == 0) {
                _writer.write(int_tid_0);
            }
            else if (v < 0) {
                _writer.write(int_tid_neg);
                _writer.writeVarUInt8Value(-v, 1);
            }
            else {
                _writer.write(int_tid_pos);
                _writer.writeVarUInt8Value(v, 1);
            }
        }
        patch_len += len;
        patch(patch_len);    
    }
    @Override
    public void writeIntList(short[] values) throws IOException
    {
        startValue();
        int patch_len = 1;
        int len = 0;
        for (int ii=0; ii<values.length; ii++) {
            len++;
            len += IonBinary.lenIonInt(values[ii]);
        }
        
        int ln = (len < IonConstants.lnIsVarLen) ? len : IonConstants.lnIsVarLen; 
        _writer.write((IonConstants.tidList << 4) | ln);
        if (ln == IonConstants.lnIsVarLen) {
            patch_len += _writer.writeVarUInt7Value(len, true);
        }
        
        for (int ii=0; ii<len; ii++) {
            int v = values[ii];
            ln = IonBinary.lenIonInt(v);
            if (v == 0) {
                _writer.write(int_tid_0);
            }
            else if (v < 0) {
                _writer.write((IonConstants.tidNegInt << 4) | ln);
                _writer.writeVarUInt8Value(-v, ln);
            }
            else {
                _writer.write((IonConstants.tidPosInt << 4) | ln);
                _writer.writeVarUInt8Value(v, ln);
            }
        }
        patch_len += len;
        patch(patch_len); 
    }
    @Override
    public void writeIntList(int[] values) throws IOException
    {
        startValue();
        int patch_len = 1;
        int len = 0;
        for (int ii=0; ii<values.length; ii++) {
            len++;
            len += IonBinary.lenIonInt(values[ii]);
        }
        
        int ln = (len < IonConstants.lnIsVarLen) ? len : IonConstants.lnIsVarLen; 
        _writer.write((IonConstants.tidList << 4) | ln);
        if (ln == IonConstants.lnIsVarLen) {
            patch_len += _writer.writeVarUInt7Value(len, true);
        }
        
        for (int ii=0; ii<len; ii++) {
            int v = values[ii];
            ln = IonBinary.lenIonInt(v);
            if (v == 0) {
                _writer.write(int_tid_0);
            }
            else if (v < 0) {
                _writer.write((IonConstants.tidNegInt << 4) | ln);
                _writer.writeVarUInt8Value(-v, ln);
            }
            else {
                _writer.write((IonConstants.tidPosInt << 4) | ln);
                _writer.writeVarUInt8Value(v, ln);
            }
        }
        patch_len += len;
        patch(patch_len);
    }
    @Override
    public void writeIntList(long[] values) throws IOException
    {
        startValue();
        int patch_len = 1;
        int len = 0;
        for (int ii=0; ii<values.length; ii++) {
            len++;
            len += IonBinary.lenIonInt(values[ii]);
        }
        
        int ln = (len < IonConstants.lnIsVarLen) ? len : IonConstants.lnIsVarLen; 
        _writer.write((IonConstants.tidList << 4) | ln);
        if (ln == IonConstants.lnIsVarLen) {
            patch_len += _writer.writeVarUInt7Value(len, true);
        }
        
        for (int ii=0; ii<len; ii++) {
            long v = values[ii];
            ln = IonBinary.lenIonInt(v);
            if (v == 0) {
                _writer.write(int_tid_0);
            }
            else if (v < 0) {
                _writer.write((IonConstants.tidNegInt << 4) | ln);
                _writer.writeVarUInt8Value(-v, ln);
            }
            else {
                _writer.write((IonConstants.tidPosInt << 4) | ln);
                _writer.writeVarUInt8Value(v, ln);
            }
        }
        patch_len += len;
        patch(patch_len); 
    }
    @Override
    public void writeFloatList(float[] values) throws IOException
    {
        startValue();
        int patch_len = 1;
        
        int len = 0;
        for (int ii=0; ii<values.length; ii++) {
            len++;
            len += IonBinary.lenIonFloat(values[ii]);
        }
        
        int ln = (len < IonConstants.lnIsVarLen) ? len : IonConstants.lnIsVarLen; 
        _writer.write((IonConstants.tidList << 4) | ln);
        if (ln == IonConstants.lnIsVarLen) {
            patch_len += _writer.writeVarUInt7Value(len, true);
        }
        
        for (int ii=0; ii<len; ii++) {
            double v = values[ii];
            ln = IonBinary.lenIonFloat(v);
            _writer.write((IonConstants.tidFloat << 4) | ln);
            _writer.writeFloatValue(v);
        }
        patch_len += len;
        
        patch(patch_len); 
    }
    @Override
    public void writeFloatList(double[] values) throws IOException
    {
        startValue();
        int patch_len = 1;
        
        int len = 0;
        for (int ii=0; ii<values.length; ii++) {
            len++;
            len += IonBinary.lenIonFloat(values[ii]);
        }
        
        int ln = (len < IonConstants.lnIsVarLen) ? len : IonConstants.lnIsVarLen; 
        _writer.write((IonConstants.tidList << 4) | ln);
        if (ln == IonConstants.lnIsVarLen) {
            patch_len += _writer.writeVarUInt7Value(len, true);
        }
        
        for (int ii=0; ii<len; ii++) {
            double v = values[ii];
            ln = IonBinary.lenIonFloat(v);
            _writer.write((IonConstants.tidFloat << 4) | ln);
            _writer.writeFloatValue(v);
        }
        patch_len += len;
        
        patch(patch_len); 
    }
    @Override
    public void writeStringList(String[] values) throws IOException
    {
        String s;
        
        startValue();
        int patch_len = 1;
        
        int len = 0;
        for (int ii=0; ii<values.length; ii++) {
            len++;
            s = values[ii];
            if (s != null) {
                int vlen = IonBinary.lenIonString(s);
                if (vlen >= IonConstants.lnIsVarLen) {
                    len += IonBinary.lenVarUInt7(vlen);
                }
                len += vlen;
            }
        }
        
        int ln = (len < IonConstants.lnIsVarLen) ? len : IonConstants.lnIsVarLen; 
        _writer.write((IonConstants.tidList << 4) | ln);
        if (ln == IonConstants.lnIsVarLen) {
            patch_len += _writer.writeVarUInt7Value(len, true);
        }
        
        for (int ii=0; ii<len; ii++) {
            s = values[ii];
            if (s == null) {
                _writer.write((IonConstants.tidString << 4) | IonConstants.lnIsNullAtom);
            }
            else {
                int vlen = IonBinary.lenIonString(s);
                if (vlen < IonConstants.lnIsVarLen) {
                    _writer.write((IonConstants.tidString << 4) | vlen);
                }
                else {
                    _writer.write((IonConstants.tidString << 4) | IonConstants.lnIsVarLen);
                    len += IonBinary.lenVarUInt7(vlen);
                }
                _writer.writeStringData(s);
            }
        }
        patch_len += len;
        
        patch(patch_len); 
    }
    
    public int getOutputLen()  throws IOException
    {
        int buffer_length = _manager.buffer().size();
        int patch_amount = 0;
        
        for (int patch_idx = 0; patch_idx < _patch_count; patch_idx ++) { 
            // int vlen = _patch_list[patch_idx + IonBinaryWriter.POSITION_OFFSET];
            int vlen = _patch_lengths[patch_idx];
            int ln = IonBinary.lenVarUInt7(vlen);
            if (vlen >= IonConstants.lnIsVarLen) {
                patch_amount += ln;
            }
        }
        
        int symbol_table_length = 0;
        if (!_no_local_symbols) {
            symbol_table_length = lenSymbolTable();
        }
        
        int total_length = IonConstants.BINARY_VERSION_MARKER_SIZE + buffer_length + patch_amount + symbol_table_length;
        
        return total_length;
    }
    
    public byte[] getBytes() throws IOException
    {
        int total_length = getOutputLen();
        
        byte[] bytes = new byte[total_length];
        SimpleByteBuffer outbuf = new SimpleByteBuffer(bytes);
        SimpleByteWriter writer = (SimpleByteWriter) outbuf.getWriter();
        int written_len = writeBytes(writer); 
        if (written_len != total_length) {
            throw new IllegalStateException("expected and actual lengths written didn't match");
        }

        return bytes;
    }
    
    public int getBytes(byte[] bytes, int offset, int maxlen) throws IOException
    {
        SimpleByteBuffer outbuf = new SimpleByteBuffer(bytes, offset, maxlen);
        SimpleByteWriter writer = (SimpleByteWriter) outbuf.getWriter();
        int total_length = writeBytes(writer);
        if (maxlen < total_length) {
            throw new IllegalStateException("actual length written overran max buffer length");
        }

        return total_length;
    }
    public int writeBytes(OutputStream userstream) throws IOException
    {
        int total_written = 0;
        ByteWriterOutputStream iout = new ByteWriterOutputStream(userstream);
        
        iout.write(IonConstants.BINARY_VERSION_MARKER_1_0, 0, IonConstants.BINARY_VERSION_MARKER_1_0.length);
        total_written += IonConstants.BINARY_VERSION_MARKER_1_0.length;
        
        if (!_no_local_symbols) {
            total_written += writeSymbolTable(iout);
        }
        
        int pos = 0;
        BlockedBuffer.BlockedByteInputStream bufferstream = 
            new BlockedBuffer.BlockedByteInputStream(_manager.buffer());
        
        int buffer_length = _manager.buffer().size();
        int patch_idx = 0;
        int patch_pos;
        if (patch_idx < _patch_count) {
            patch_pos = _patch_offsets[patch_idx]; // _patch_list[patch_idx + IonBinaryWriter.POSITION_OFFSET];
        }
        else {
            patch_pos = buffer_length + 1;
        }
        
        while (pos < buffer_length) {
            if (pos < patch_pos) {
                int len;
                if (patch_pos > buffer_length) {
                    len = buffer_length - pos ;
                }
                else {
                    len = patch_pos - pos ;
                }
                
                pos += bufferstream.writeTo(iout, len);
                total_written += len;
                if (pos >= buffer_length) break;
            }
            int vlen = _patch_lengths[patch_idx]; //_patch_list[patch_idx + IonBinaryWriter.POSITION_OFFSET];
            int ptd = _patch_types[patch_idx]; // _patch_list[patch_idx + IonBinaryWriter.TID_OFFSET];
            //if (ptd < 0) {
            //    ptd = -ptd;
            //}
            //ptd = ptd - 1;
            total_written += iout.writeTypeDescWithLength(ptd, vlen);

            // skip the typedesc byte we have written here
            pos += bufferstream.skip(1);
            
            // find the next patch point, if there's one left
            //patch_idx += LIST_WIDTH;
            patch_idx++;
            if (patch_idx < _patch_count) {
                patch_pos = _patch_offsets[patch_idx]; // _patch_list[patch_idx + IonBinaryWriter.POSITION_OFFSET];
            }
            else {
                patch_pos = buffer_length + 1;
            }
        }
        return total_written;
    }
    
    int lenSymbolTable() throws IOException
    {
        return writeSymbolTable(null);
    }
    int writeSymbolTable(ByteWriterOutputStream out) throws IOException
    {
        int name_len, ver_len, max_id_len, symbol_list_len, import_len;
        
        // first calculate the length of the bits and pieces we will be
        // writing out in the second phase.  We do this all in one big
        // hairy method so that we can remember the lengths of most of
        // these bits and pieces so that we have to recalculate them as
        // we go to write out the typedesc headers when we write out the
        // values themselves.
        String name = super.getSymbolTableName();
        int max_id = 0;
        if (name != null) {
            name_len = IonBinary.lenIonStringWithTypeDesc(name);
            ver_len = IonBinary.lenVarInt(super.getSymbolTableVersion());
            
            // unless there's a name (i.e. this is a shared table) the
            // max id value is of no use
            max_id = super.getSymbolTableMaxId();
            if (max_id > 0) {
                // +2 is 1 for symbol sid, 1 for typedesc byte
                max_id_len = IonBinary.lenVarUInt8(max_id) + 2;
            }
            else {
                max_id_len = 0;
            }
        }
        else {
            name_len = 0;
            ver_len = 0;
            max_id_len = 0;
        }
        
        int import_header_len = 0;
        import_len = 0;
        UnifiedSymbolTable [] imports = super.getSymbolTableImportedTables();
        int [] import_lens = null;
        if (imports != null && imports.length > 0) {
            import_lens = new int[imports.length];
            for (int ii=0; ii< imports.length; ii++) {
                import_lens[ii] += lenSymbolTableReferenceContent(imports[ii]);
                import_len += import_lens[ii];
                import_len += IonBinary.lenLenFieldWithOptionalNibble(import_lens[ii]);
                import_len++; // for type id (which isn't include in the above len)
            }
            import_header_len = 2;  // fieldid(imports) + typedesc for array
            if (import_len >= IonConstants.lnIsVarLen) {
                import_header_len += IonBinary.lenVarUInt7(import_len);
            }
        }
        
        int symbol_list_content_len = 0;
        symbol_list_len = 0;

        Iterator<UnifiedSymbolTable.Symbol> syms = super.getSymbolTableSymbols();
        while (syms.hasNext()) {
            UnifiedSymbolTable.Symbol s = syms.next();
            if (s == null) continue;
            int s_len = IonBinary.lenVarUInt7(s.sid);
            s_len += IonBinary.lenIonStringWithTypeDesc(s.name);
            symbol_list_content_len += s_len;
        }
        if (symbol_list_content_len > 0) {
            symbol_list_len = 2; // fldid + typedesc
            symbol_list_len += symbol_list_content_len;
            if (symbol_list_len >= IonConstants.lnIsVarLen) {
                symbol_list_len += IonBinary.lenVarUInt7(symbol_list_len);
            }
        }
        
        int content_len = name_len + ver_len + max_id_len + symbol_list_len;
        content_len += import_header_len + import_len;
        int content_len_len = 0;
        if (content_len >= IonConstants.lnIsVarLen) {
            content_len_len = IonBinary.lenVarUInt7(content_len);
        }

        // $ion_symbol_table::{ ... }
        // <anntd(1)>{<total_len?>}<ann_len(1)><anns(1)> <td(1)>{<contentlen>}<content>
        int total_len = 3 + content_len_len + content_len; 
        int initial_header_len = 1;
        if (total_len >= IonConstants.lnIsVarLen) {
            initial_header_len += IonBinary.lenVarUInt7(total_len);
        }
        
        // trick to just get the length
        if (out == null) {
            return initial_header_len + total_len;
        }
        
        // now that we know how long most everything is
        // we can write it out in one forward pass
        int total_len_written = 0;
        
        total_len_written += out.writeTypeDescWithLength(IonConstants.tidTypedecl, total_len);
        
        total_len_written += out.writeVarUInt(1, true); // length of the 1 annotation
        total_len_written += out.writeVarUInt(UnifiedSymbolTable.ION_1_0_SID, true);
        
        total_len_written += out.writeTypeDescWithLength(IonConstants.tidStruct, content_len);
        
        name = super.getSymbolTableName();
        if (name != null) {
            total_len_written += out.writeVarUInt(UnifiedSymbolTable.NAME_SID, true);
            total_len_written += out.writeTypeDescWithLength(IonConstants.tidString,
                                                             IonBinary.lenIonString(name));
            total_len_written += out.writeString(name);
            
            // if there's no name, there's no need for a version
            // -2 is to remove the cost of the typedesc byte and the fieldsid length
            total_len_written += out.writeVarUInt(UnifiedSymbolTable.VERSION_SID, true);
            total_len_written += out.writeTypeDescWithLength(IonConstants.tidPosInt, ver_len - 2);  
            total_len_written += out.writeInt(super.getSymbolTableVersion(), true);
        
            if (max_id > 0) {
                total_len_written += out.writeVarUInt(UnifiedSymbolTable.MAX_ID_SID, true);
                total_len_written += out.writeTypeDescWithLength(IonConstants.tidPosInt, max_id_len - 1);  
                total_len_written += out.writeInt(super.getSymbolTableMaxId(), true);
            }
        }
        
        // now write imports (if we have any)
        int written_import_len = 0;
        if (imports != null && import_lens != null && import_len > 0) {
            written_import_len += out.writeVarUInt(UnifiedSymbolTable.IMPORTS_SID, true);
            written_import_len += out.writeTypeDescWithLength(IonConstants.tidList, import_len);  
            for (int ii=0; ii<imports.length; ii++) {
                written_import_len += writeSymbolTableReference(out, imports[ii], import_lens[ii]);
            }
            if (written_import_len != import_len + import_header_len) {
                throw new IllegalStateException("expected length of import references and written length of the same don't match!");
            }
            total_len_written += written_import_len;
        }
        
        // and finally write the local symbols
        int written_symbols_header_len = 0;
        int written_symbol_list_content_len = 0;
        
        if (symbol_list_content_len > 0) {
            written_symbols_header_len += out.writeVarUInt(UnifiedSymbolTable.SYMBOLS_SID, true);
            written_symbols_header_len += out.writeTypeDescWithLength(IonConstants.tidStruct, symbol_list_content_len);
            syms = super.getSymbolTableSymbols();
            while (syms.hasNext()) {
                UnifiedSymbolTable.Symbol s = syms.next();
                int s_len = IonBinary.lenIonString(s.name); 
                int t_len = out.writeVarUInt(s.sid, true);
                t_len += out.writeTypeDescWithLength(IonConstants.tidString, s_len);
                t_len += out.writeString(s.name); 
                written_symbol_list_content_len += t_len;
            }
            if (written_symbol_list_content_len != symbol_list_content_len) {
                throw new IllegalStateException("expected length of local symbols and written length of the same don't match!");
            }
        }
        total_len_written += written_symbols_header_len;
        total_len_written += written_symbol_list_content_len;
        
        if (total_len_written != initial_header_len + total_len) {
            throw new IllegalStateException("expected length of symbol table and written length of symbol table don't match!");
        }
        return total_len_written;
    }
    
    int writeSymbolTableReference(ByteWriterOutputStream out
                                  , UnifiedSymbolTable table
                                  , int content_len) 
        throws IOException 
    {
        // $ion_imports:{name:"symbol table name", ver:1, max_is:3}
        
        int header_len = out.writeTypeDescWithLength(IonConstants.tidStruct, content_len);
        int value_len;
        int tdlen;
        
        int content_len_written = out.writeVarUInt(UnifiedSymbolTable.NAME_SID, true);
        String name = table.getName();
        value_len = IonBinary.lenIonString(name);
        tdlen = out.writeTypeDescWithLength(IonConstants.tidString, value_len);
        if (value_len != out.writeString(name)) {
            throw new IllegalStateException("write for referenced symbol table name has a mismatched length");
        }
        content_len_written += tdlen;
        content_len_written += value_len;
        
        content_len_written += out.writeVarUInt(UnifiedSymbolTable.VERSION_SID, true);
        int version = table.getVersion();
        value_len = IonBinary.lenVarUInt8(version);
        tdlen = out.writeTypeDescWithLength(IonConstants.tidPosInt, value_len);  
        if (value_len != out.writeLong(version, true)) {
            throw new IllegalStateException("write for version has a mismatched length");
        }
        content_len_written += tdlen; 
        content_len_written += value_len;
        
        content_len_written += out.writeVarUInt(UnifiedSymbolTable.MAX_ID_SID, true);
        int max_id = table.getMaxId();
        value_len = IonBinary.lenVarUInt8(max_id);
        tdlen = out.writeTypeDescWithLength(IonConstants.tidPosInt, value_len);  
        if (value_len != out.writeLong(max_id, true)) {
            throw new IllegalStateException("write for max id has a mismatched length");
        }
        content_len_written += tdlen; 
        content_len_written += value_len;
        
        if (content_len_written != content_len) {
            throw new IllegalStateException("write for the import symbol table reference doesn't match expected length");
        }
        
        return content_len_written + header_len;
    }
            
    int lenSymbolTableReferenceContent(UnifiedSymbolTable table) 
    {
        // $ion_imports:{name:"symbol table name", ver:1, max_is:3}
        int value_len;
        
        int content_len_written = 1; // out.writeVarUInt(UnifiedSymbolTable.NAME_SID, true);
        String name = table.getName();
        value_len = IonBinary.lenIonStringWithTypeDesc(name);
        content_len_written += value_len;
        
        content_len_written += 1; // out.writeVarUInt(UnifiedSymbolTable.VERSION_SID, true);
        int version = table.getVersion();
        value_len = IonBinary.lenVarUInt8(version);
        content_len_written += 1 + value_len; // +1 for td byte
        
        content_len_written += 1; // out.writeVarUInt(UnifiedSymbolTable.MAX_ID_SID, true);
        int max_id = table.getMaxId();
        value_len = IonBinary.lenVarUInt8(max_id);
        content_len_written += 1 + value_len; // +1 for td byte
        
        return content_len_written;
    }
}

