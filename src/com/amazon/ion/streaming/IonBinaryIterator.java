/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.streaming;

import com.amazon.ion.IonBlob;
import com.amazon.ion.IonClob;
import com.amazon.ion.IonDecimal;
import com.amazon.ion.IonException;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonList;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.SystemSymbolTable;
import com.amazon.ion.impl.IonConstants;
import com.amazon.ion.impl.IonTokenReader;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.EmptyStackException;
import java.util.Iterator;

/**
 * implements an iterator over a Ion binary buffer.
 */
public class IonBinaryIterator
    extends IonIterator
{
// TODO------------------------------------------------------------------------------------------
static final boolean _verbose_debug = false;
static String getTidAsString(int tid) {
    IonType event = null;
    switch (tid) {
        case IonConstants.tidNull:      // 0
            event = IonType.NULL;
            break;
        case IonConstants.tidBoolean:   // 1
            event = IonType.BOOL;
            break;
        case IonConstants.tidPosInt:    // 2
        case IonConstants.tidNegInt:    // 3
            event = IonType.INT;
            break;
        case IonConstants.tidFloat:     // 4
            event = IonType.FLOAT;
            break;
        case IonConstants.tidDecimal:   // 5
            event = IonType.DECIMAL;
            break;
        case IonConstants.tidTimestamp: // 6
            event = IonType.TIMESTAMP;
            break;
        case IonConstants.tidSymbol:    // 7
            event = IonType.SYMBOL;
            break;
        case IonConstants.tidString:    // 8
            event = IonType.STRING;
            break;
        case IonConstants.tidClob:      // 9
            event = IonType.CLOB;
            break;
        case IonConstants.tidBlob:      // 10 A
            event = IonType.BLOB;
            break;
        case IonConstants.tidList:      // 11 B
            event = IonType.LIST;
            break;
        case IonConstants.tidSexp:      // 12 C
            event = IonType.SEXP;
            break;
        case IonConstants.tidStruct:    // 13 D
            event = IonType.STRUCT;
            break;
    }
    return event+"";
}
    
    SimpleByteBuffer    _buffer;
    ByteReader          _reader;
    UnifiedSymbolTable  _symbols;
    boolean             _expect_symbol_table;
    
    boolean _eof;
    int     _local_end;
    
    static final int S_INVALID          = 0;
    static final int S_BEFORE_TID       = 1;
    static final int S_AFTER_TID        = 2;
    static final int S_BEFORE_CONTENTS  = 3;
    
    int     _state; // 0=before tid, 1=after tid, 2=before contents

    boolean _in_struct;
    int     _annotation_start;  
    IonType _value_type;
    int     _value_field_id;
    int     _value_tid;
    int     _value_len;
    
    // local stack for stepInto() and stepOut()
    int         _top;
    int[]       _next_position_stack;
    boolean[]   _in_struct_stack;
    int[]       _local_end_stack;
    UnifiedSymbolTable[] _symbol_stack;
    
    IonBinaryIterator() {}
    
    IonBinaryIterator(byte[] buf, int start, int len) 
    {
        this(  null
             , new SimpleByteBuffer(buf, start, len)
             , UnifiedSymbolTable.getSystemSymbolTableInstance()
        );
    }
    IonBinaryIterator(IonType parent, UnifiedSymbolTable symboltable, byte[] buf, int start, int len) 
    {
        this(  parent
             , new SimpleByteBuffer(buf, start, len)
             , symboltable
        );
    }
    IonBinaryIterator(IonType parent, SimpleByteBuffer ssb, UnifiedSymbolTable symboltable) 
    {
        _buffer = ssb;
        _reader = _buffer.getReader();
        _local_end = _buffer._eob;
        _state = S_BEFORE_TID;
        _symbols = symboltable;
        _expect_symbol_table = (parent == null || parent.equals(IonType.SEXP));
    }
    @Override
    public boolean hasNext()
    {
        if (_eof) return false;
        if (_state == S_AFTER_TID) return true;
        
        if (_state == S_BEFORE_CONTENTS) {
            // if we stepped into the value with next() and then
            // decided never read the value itself we have to skip
            // over the value contents here
            _reader.skip(_value_len);
        }

        int value_start = _reader.position(); 
        if (value_start >= this._local_end) {
            _eof = true;
            return false;
        }
        
        int td;
        try {
            if (_in_struct) {
                _value_field_id = _reader.readVarUInt();
            }
            else {
                _value_field_id = -1;
            }
            
            td = _reader.read();
            if (td == ByteReader.EOF) {
                _eof = true;
                return false;
            }
            
            if (_expect_symbol_table 
            && ((td >>> 4) & 0xf) == IonConstants.tidTypedecl
            ) {
                td = loadPossibleSymbolTable(td);
            }
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        // mark where we are
        _value_tid = td;
        _state = IonBinaryIterator.S_AFTER_TID;
        _expect_symbol_table = false;
        return true;
    }
    int loadPossibleSymbolTable(int typedesc) {
        int original_position = _reader.position();
        
        try {
            int vlen = read_length(typedesc);   // we have to read past the overall value length first
            assert vlen == vlen;                // shut eclipse up about not reading vlen
            int alen = _reader.readVarUInt();   // now we have the length of the annotations themselves
            int aend = _reader.position() + alen;
            
            while (_reader.position() < aend) {
                int a = _reader.readVarUInt();
                if (a == SystemSymbolTable.ION_SYMBOL_TABLE_SID
                 || a == SystemSymbolTable.ION_1_0_SID
                ) {
                    int newtd = loadSymbolTable(a, original_position, aend);
                    if (newtd == -1) break;
                    return newtd;
                }
            }
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        _reader.position(original_position);
        return typedesc;
    }
    int loadSymbolTable(int annotationid, int original_start, int contents_start) throws IOException 
    {
        _reader.position(contents_start);
        
        int td = _reader.read();
        if (((td >>> 4) & 0xf) != IonConstants.tidStruct) return -1;
        
        boolean was_struct = this._in_struct;
        int     prev_end = this._local_end;
        
        this._value_tid = td;
        this._in_struct = true;
        
        int len = this.read_length(td);
        this._local_end = this._reader.position() + len;
        
        if (this._local_end >= prev_end) {
            _state = IonBinaryIterator.S_INVALID;
            throw new IonException("invalid binary format");
        }
        // TODO: this should get it's system symbol table somewhere else
        // like passed in from the user or deduced from the version stamp
        UnifiedSymbolTable systemSymbols = UnifiedSymbolTable.getSystemSymbolTableInstance();
        UnifiedSymbolTable local = new UnifiedSymbolTable(systemSymbols); 
        int field_sid = -1;
        while (hasNext()) {
            next();
            field_sid = getFieldId();
            switch (field_sid) {
            case SystemSymbolTable.MAX_ID_SID:
                local.setMaxId(getInt());
                break;
            case SystemSymbolTable.NAME_SID:
                local.setName(getString());
                break;
            case SystemSymbolTable.IMPORTS_SID:
                // BUGBUG: we should be looking these up somewhere !!
                break;
            case SystemSymbolTable.SYMBOLS_SID:
                stepInto();
                while (hasNext()) {
                    if (next() != IonType.STRING) {
                        continue; // we could error here, but open content says don't bother
                    }
                    int sid = getFieldId();
                    String symbol = getString();
                    local.defineSymbol(symbol, sid);
                }
                stepOut();
                break;
            case SystemSymbolTable.VERSION_SID:
                local.setVersion(getInt());
                break;
            default:
                break;
            }
        }
        _eof = false; // the hasNext() on the last field in the symbol table sets this
        
        this._symbols = local;
        this._in_struct = was_struct;
        this._local_end = prev_end;
        
        int value_start = _reader.position(); 
        if (value_start >= this._local_end) {
            _eof = true;
            return 0;
        }
        
        // read the next type descriptor, when we're reading
        // a symbol table we have to be in an sexp or the top
        // datagram, so we don't have to read the field sid
        td = _reader.read();
        if (td == ByteReader.EOF) {
            _eof = true;
            return 0;
        }
        
        return td;
    }
    public int nextTid() {
        if (_state != IonBinaryIterator.S_AFTER_TID) {
            throw new IllegalStateException();
        }
        // get actual type id
        int ion_type = (_value_tid & 0xf0) >>> 4;
        
        try {
            // if 14 (annotation)
            if (ion_type == IonConstants.tidTypedecl) {
                //      skip forward annotation length
                
                //      first we skip the value length and then 
                //      read the local annotation length
                read_length(_value_tid);

                //      set annotation start to the position of
                //      the first type desc byte
                this._annotation_start = _reader.position();
                int annotation_len = _reader.readVarUInt();
                _reader.skip(annotation_len);
                
                //      read tid again
                _value_tid = _reader.read();
                ion_type = (_value_tid & 0xf0) >>> 4;
            }
            else {
                // clear the annotation marker that's left over from our previous value
                _annotation_start = -1;
            }
            
            // read length (if necessary)
            _value_len = read_length(_value_tid);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        
        // set the state forward
        _state = IonBinaryIterator.S_BEFORE_CONTENTS;
        return ion_type;

        
    }
    @Override
    public IonType next()
    {
        if (_state != IonBinaryIterator.S_AFTER_TID) {
            throw new IllegalStateException();
        }
        // get actual type id
        int ion_type = (_value_tid & 0xf0) >>> 4;
        
        try {
            // if 14 (annotation)
            if (ion_type == IonConstants.tidTypedecl) {
                //      skip forward annotation length
                
                //      first we skip the value length and then 
                //      read the local annotation length
                read_length(_value_tid);

                //      set annotation start to the position of
                //      the first type desc byte
                this._annotation_start = _reader.position();
                int annotation_len = _reader.readVarUInt();
                _reader.skip(annotation_len);
                
                //      read tid again
                _value_tid = _reader.read();
                ion_type = (_value_tid & 0xf0) >>> 4;
            }
            else {
                // clear the annotation marker that's left over from our previous value
                _annotation_start = -1;
            }
            
            // read length (if necessary)
            _value_len = read_length(_value_tid);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        
        // set the state forward
        _state = IonBinaryIterator.S_BEFORE_CONTENTS;
        _value_type = null;
        switch (ion_type) {
        case IonConstants.tidNull:      // 0
            _value_type = IonType.NULL;
            break;
        case IonConstants.tidBoolean:   // 1
            _value_type = IonType.BOOL;
            break;
        case IonConstants.tidPosInt:    // 2
        case IonConstants.tidNegInt:    // 3
            _value_type = IonType.INT;
            break;
        case IonConstants.tidFloat:     // 4
            _value_type = IonType.FLOAT;
            break;
        case IonConstants.tidDecimal:   // 5
            _value_type = IonType.DECIMAL;
            break;
        case IonConstants.tidTimestamp: // 6
            _value_type = IonType.TIMESTAMP;
            break;
        case IonConstants.tidSymbol:    // 7
            _value_type = IonType.SYMBOL;
            break;
        case IonConstants.tidString:    // 8
            _value_type = IonType.STRING;
            break;
        case IonConstants.tidClob:      // 9
            _value_type = IonType.CLOB;
            break;
        case IonConstants.tidBlob:      // 10 A
            _value_type = IonType.BLOB;
            break;
        case IonConstants.tidList:      // 11 B
            _value_type = IonType.LIST;
            break;
        case IonConstants.tidSexp:      // 12 C
            _value_type = IonType.SEXP;
            break;
        case IonConstants.tidStruct:    // 13 D
            _value_type = IonType.STRUCT;
            break;
        case IonConstants.tidTypedecl:  // 14 E
        default:
            throw new IonException("unrecognized type encountered");
        }
        
        return _value_type;

    }
    int read_length(int tid) throws IOException {
        switch ((tid >>> 4) & 0xf) {
        case IonConstants.tidNull:      // 0
        case IonConstants.tidBoolean:   // 1
            return 0;
        case IonConstants.tidPosInt:    // 2
        case IonConstants.tidNegInt:    // 3
        case IonConstants.tidFloat:     // 4
        case IonConstants.tidDecimal:   // 5
        case IonConstants.tidTimestamp: // 6
        case IonConstants.tidSymbol:    // 7
        case IonConstants.tidString:    // 8
        case IonConstants.tidClob:      // 9
        case IonConstants.tidBlob:      // 10 A
        case IonConstants.tidList:      // 11 B
        case IonConstants.tidSexp:      // 12 C
        case IonConstants.tidStruct:    // 13 D
        case IonConstants.tidTypedecl:  // 14 E
            int len = tid & 0xf;
            if (len == 14) {
                len = _reader.readVarUInt();
            }
            else if (len == 15) {
                len = 0;
            }
            return len;
        default:
            throw new IonException("unrecognized type encountered");
        }
    }
    
    @Override
    public int getDepth() {
        return _top;
    }

    @Override
    public int getContainerSize() 
    {
        int size = 0;
        boolean counting_a_struct = false;
        
        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        int tid = (_value_tid >>> 4);
        switch (tid) {
            case IonConstants.tidSexp:
            case IonConstants.tidList:
                counting_a_struct = false;
                break;
            case IonConstants.tidStruct:
                counting_a_struct = true;
                break;
            default:
                throw new IllegalStateException();
        }
        
        int original_position = _reader.position();
        
        try {
            int aend = _value_len + original_position;
            
            while (_reader.position() < aend) {
                if (counting_a_struct) {
                    int fldid = _reader.readVarUInt();
                    assert fldid == fldid;
                }
                int td = _reader.read();
                int vlen = this.read_length(td);
                _reader.skip(vlen);
                size++;
            }
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        
        _reader.position(original_position);
        return size;
    }
    
    
    @Override
    public void stepInto()
    {
        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        int tid = (_value_tid >>> 4);
        if (tid != IonConstants.tidSexp
         && tid != IonConstants.tidList
         && tid != IonConstants.tidStruct
        ) {
            throw new IllegalStateException();
        }
        
        // first make room, if we need to
        if (_next_position_stack == null || _next_position_stack.length <= _top) {
            grow();
        }
        
        // here we can push the stack and start at the
        // beginning of the collections values
        
        // when we step back out we'll be just before our
        // siblings type desc byte
        int next_start = _reader.position() + _value_len;
        _next_position_stack[_top] = next_start;
        _in_struct_stack[_top] = _in_struct;
        _local_end_stack[_top] = _local_end;
        _symbol_stack[_top] = _symbols;
        _top++;
        
        // now we set up for this collections contents
        _in_struct = (tid == IonConstants.tidStruct);
        _expect_symbol_table = (tid == IonConstants.tidSexp);
        _local_end = next_start;
        _state = S_BEFORE_TID;
    }
    private void grow() {
        int newlen = (_next_position_stack == null) ? 10 : (_next_position_stack.length * 2);
        int [] temp1 = new int[newlen];
        boolean [] temp2 = new boolean[newlen];
        int [] temp3 = new int[newlen];
        UnifiedSymbolTable [] temp4 = new UnifiedSymbolTable[newlen];
        if (_top > 1) {
            System.arraycopy(_next_position_stack, 0, temp1, 0, _top);
            System.arraycopy(_in_struct_stack, 0, temp2, 0, _top);
            System.arraycopy(_local_end_stack, 0, temp3, 0, _top);
            System.arraycopy(_symbol_stack, 0, temp4, 0, _top);
        }
        _next_position_stack = temp1;
        _in_struct_stack = temp2;
        _local_end_stack = temp3;
        _symbol_stack = temp4;
    }
    
    @Override
    public void stepOut()
    {
        if (_top < 1) {
            // if we didn't step in, we can't step out
            throw new EmptyStackException();
        }
        
        int next_start;
        
        _top--;
        next_start = _next_position_stack[_top];
        _in_struct = _in_struct_stack[_top];
        _local_end = _local_end_stack[_top];
        _symbols = _symbol_stack[_top];
        
        _reader.position(next_start);
        _state = S_BEFORE_TID;
        _expect_symbol_table = false;
        _eof = false;
    }
    
    @Override
    public UnifiedSymbolTable getSymbolTable() {
        return _symbols;
    }

    @Override
    public void setSymbolTable(UnifiedSymbolTable  externalsymboltable) 
    {
        if (_symbols != null && SystemSymbolTable.ION_1_0.equals(_symbols.getName())) {
            _symbols = new UnifiedSymbolTable(_symbols);
        }
        if (_symbols == null) {
            _symbols = new UnifiedSymbolTable(UnifiedSymbolTable.getSystemSymbolTableInstance()); 
        }
        _symbols.addImportedTable(externalsymboltable);
    }
    
    @Override
    public IonType getType()
    {
        if (_eof || _state == IonBinaryIterator.S_BEFORE_TID) {
            throw new IllegalStateException();
        }
        return _value_type;
    }
    
    @Override
    public int getTypeId()
    {
        if (_eof || _state == IonBinaryIterator.S_BEFORE_TID) {
            throw new IllegalStateException();
        }
        return ((_value_tid >>> 4) & 0xf);
    }
    
    @Override
    public int[] getAnnotationIds()
    {
        int[] ids = null;
        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        if (_annotation_start == -1) return null;

        int pos = _reader.position();
        
        try {
            // first count
            _reader.position(_annotation_start);
            int len = _reader.readVarUInt();
            int annotation_end = _reader.position() + len;
            int count = 0;
            while (annotation_end > _reader.position()) {
                _reader.readVarUInt();
                count++;
            }
            
            // now, again, to save those values
            ids = new int[count];
            _reader.position(_annotation_start);
            _reader.readVarUInt();
            count = 0;
            while (annotation_end > _reader.position()) {
                ids[count++] = _reader.readVarUInt();
            }
        }
        catch (IOException e) {
            throw new IonException(e);
        }

        // restore out cursor position before we go back to our previous tasks
        _reader.position(pos);
        return ids;
    }

    @Override
    public String[] getAnnotations()
    {
        int[] ids = getAnnotationIds();
        if (ids == null) return null;
        if (_symbols == null) {
            throw new IllegalStateException();
        }

        String[] annotations = new String[ids.length];
        for (int ii=0; ii<ids.length; ii++) {
            annotations[ii] = _symbols.findSymbol(ids[ii]);
        }
        
        return annotations;
    }

    @Override
    public Iterator<Integer> getAnnotationIdIterator()
    {
        int[] ids = getAnnotationIds();
        if (ids == null) return null;
        return new IonTreeIterator.IdIterator(ids);
    }

    @Override
    public Iterator<String> getAnnotationIterator()
    {
        String[] ids = getAnnotations();
        if (ids == null) return null;
        return new IonTreeIterator.StringIterator(ids);
    }
    
    @Override
    public boolean isInStruct() {
        return _in_struct;
    }
    
    @Override
    public int getFieldId()
    {
        if (_value_field_id == -1) {
            throw new IllegalStateException();
        }
        return _value_field_id;
    }
    
    @Override
    public String getFieldName()
    {
        if (_value_field_id == -1 || _symbols == null) {
            throw new IllegalStateException();
        }
        return _symbols.findSymbol(_value_field_id);
    }
    
    @SuppressWarnings("deprecation")
    @Override
    public IonValue getIonValue(IonSystem sys)
    {
        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        
        int tid = (_value_tid >>> 4);
        if (isNull()) {
            switch ((tid >>> 4) & 0xf) {
            case IonConstants.tidNull:      return sys.newNull();
            case IonConstants.tidBoolean:   return sys.newNullBool();
            case IonConstants.tidPosInt:    
            case IonConstants.tidNegInt:    return sys.newNullInt();
            case IonConstants.tidFloat:     return sys.newNullFloat();
            case IonConstants.tidDecimal:   return sys.newNullDecimal();
            case IonConstants.tidTimestamp: return sys.newNullTimestamp();
            case IonConstants.tidSymbol:    return sys.newNullSymbol();
            case IonConstants.tidString:    return sys.newNullString();
            case IonConstants.tidClob:      return sys.newNullClob();
            case IonConstants.tidBlob:      return sys.newNullBlob();
            case IonConstants.tidList:      return sys.newNullList();
            case IonConstants.tidSexp:      return sys.newNullSexp();
            case IonConstants.tidStruct:    return sys.newNullString();
            default:
                throw new IonException("unrecognized type encountered");
            }
        }
        
        switch ((tid >>> 4) & 0xf) {
        case IonConstants.tidNull:      return sys.newNull();
        case IonConstants.tidBoolean:   return sys.newBool(getBool());
        case IonConstants.tidPosInt:    
        case IonConstants.tidNegInt:    return sys.newInt(getLong());
        case IonConstants.tidFloat:     
            IonFloat f = sys.newFloat();
            f.setValue(getDouble());
            return f;
        case IonConstants.tidDecimal:
            IonDecimal dec = sys.newDecimal();
            dec.setValue(getBigDecimal());
            return dec;
        case IonConstants.tidTimestamp: 
            IonTimestamp t = sys.newTimestamp();
            IonTokenReader.Type.timeinfo ti = getTimestamp();
            t.setMillis(ti.d.getTime());
            t.setLocalOffset(ti.localOffset);
            return t;
        case IonConstants.tidSymbol:    return sys.newSymbol(getString());
        case IonConstants.tidString:    return sys.newString(getString());
        case IonConstants.tidClob:
            IonClob clob = sys.newNullClob();
            clob.setBytes(getBytes());
            return clob;
        case IonConstants.tidBlob:
            IonBlob blob = sys.newNullBlob();
            blob.setBytes(getBytes());
            return blob;
        case IonConstants.tidList:
            IonList list = sys.newNullList();
            fillContainer(sys, list);
            return list;
        case IonConstants.tidSexp:
            IonSexp sexp = sys.newNullSexp();
            fillContainer(sys, sexp);
            return sexp;
        case IonConstants.tidStruct:
            IonStruct struct = sys.newNullStruct();
            fillContainer(sys, struct);
            return struct;
        default:
            throw new IonException("unrecognized type encountered");
        }
    }
    
    /**
     * @param list
     */
    private void fillContainer(IonSystem sys, IonSequence list)
    {
        stepInto();
        while(hasNext()) {
            next();
            list.add(getIonValue(sys));
        }
        stepOut();
    }
    private void fillContainer(IonSystem sys, IonStruct struct)
    {
        stepInto();
        while(hasNext()) {
            next();
            String fieldname = getFieldName();
            struct.add(fieldname, getIonValue(sys));
        }
        stepOut();
    }

    @Override
    public boolean isNull()
    {
        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        int tid = (_value_tid >>> 4);
        if (tid == IonConstants.tidNull) {
            return true;
        }
        return (_value_len == IonConstants.lnIsNullAtom);
    }

    @Override
    public boolean getBool()
    {
        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        int tid = (_value_tid >>> 4);
        if (tid != IonConstants.tidBoolean) {
            throw new IllegalStateException();
        }
        switch (_value_len) {
        case IonConstants.lnIsNullAtom:
            throw new NullPointerException();
        case IonConstants.lnBooleanFalse:
            _state = S_BEFORE_TID; // now we (should be) just in from of the next value
            return false;
        case IonConstants.lnBooleanTrue:
            _state = S_BEFORE_TID; // now we (should be) just in from of the next value
            return true;
        default:
            throw new IllegalStateException();
        }
    }

    @Override
    public int getInt()
    {
        return (int)getLong();
    }

    @Override
    public long getLong()
    {
        long value;
        
        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        int tid = (_value_tid >>> 4);
        if (tid != IonConstants.tidNegInt && tid != IonConstants.tidPosInt) {
            throw new IllegalStateException();
        }

        try {
            value = _reader.readULong(_value_len);
            if (tid == IonConstants.tidNegInt) {
                value = -value;
            }
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        _state = S_BEFORE_TID; // now we (should be) just in from of the next value
        return value;
    }

    @Override
    public double getDouble()
    {
        double value;
        
        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        int tid = (_value_tid >>> 4);
        if (tid != IonConstants.tidFloat) {
            throw new IllegalStateException();
        }

        try {
            value = _reader.readFloat(_value_len);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        _state = S_BEFORE_TID; // now we (should be) just in from of the next value
        return value;
    }

    @Override
    public BigDecimal getBigDecimal()
    {
        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        int tid = (_value_tid >>> 4);
        if (tid != IonConstants.tidDecimal) {
            throw new IllegalStateException();
        }
        if (tid == IonConstants.tidNull) {
            throw new NullPointerException();
        }

        BigDecimal value;
        try {
            value = _reader.readDecimal(_value_len);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        return value;
    }

    @Override
    public int getSymbolId()
    {
        long value;
        
        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        int tid = (_value_tid >>> 4);
        if (tid != IonConstants.tidSymbol) {
            throw new IllegalStateException();
        }

        try {
            value = _reader.readULong(_value_len);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        _state = S_BEFORE_TID; // now we (should be) just in from of the next value
        return (int)value;
    }

    @Override
    public Date getDate()
    {
        IonTokenReader.Type.timeinfo ti = getTimestamp();
        return ti.d;
    }
    
    @Override
    public IonTokenReader.Type.timeinfo getTimestamp()
    {
        IonTokenReader.Type.timeinfo ti;
        
        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        int tid = (_value_tid >>> 4);
        if (tid != IonConstants.tidTimestamp) {
            throw new IllegalStateException();
        }

        try {
            ti = _reader.readTimestamp(_value_len);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        _state = S_BEFORE_TID; // now we (should be) just in from of the next value
        return ti;
    }

    @Override
    public String getString()
    {
        String string_value;
        
        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        int tid = (_value_tid >>> 4);
        if (tid != IonConstants.tidSymbol && tid != IonConstants.tidString) {
            throw new IllegalStateException();
        }

        try {
            if (tid == IonConstants.tidSymbol) {
                long sid = _reader.readULong(_value_len);
                if (_symbols == null) {
                    
                }
                string_value = _symbols.findSymbol((int)sid);
            }
            else {
                string_value = _reader.readString(_value_len);
            }
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        _state = S_BEFORE_TID; // now we (should be) just in from of the next value
        return string_value;
    }

    @Override
    public byte[] getBytes()
    {
        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        int tid = (_value_tid >>> 4);
        if (tid != IonConstants.tidBlob && tid != IonConstants.tidClob) {
            throw new IllegalStateException();
        }

        byte[] value;
        try {
            value = new byte[_value_len];
            _reader.read(value, 0, _value_len);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        _state = S_BEFORE_TID; // now we (should be) just in from of the next value
        return value;
    }

    @Override
    public int getBytes(byte[] buffer, int offset, int len)
    {
        int readlen = -1;
        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        int tid = (_value_tid >>> 4);
        if (tid != IonConstants.tidBlob && tid != IonConstants.tidClob) {
            throw new IllegalStateException();
        }
        if (len < _value_len) {
            throw new IllegalArgumentException("buffer is too short for value");
        }

        try {
            readlen = _reader.read(buffer, offset, _value_len);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        _state = S_BEFORE_TID; // now we (should be) just in from of the next value
        return readlen;
    }
}
