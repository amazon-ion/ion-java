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
import com.amazon.ion.UnexpectedEofException;
import com.amazon.ion.impl.IonConstants;
import com.amazon.ion.impl.IonTokenReader;
import com.amazon.ion.impl.Base64Encoder.BinaryStream;
import com.amazon.ion.impl.IonTokenReader.Type.timeinfo;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * IonIterator implmentation that walks over a text stream and 
 * returns Ion values from it.
 */
public final class IonTextIterator
    extends IonIterator
{
   
//////////////////////////////////////////////////////////////////////////debug
    static final boolean _debug = false;

    

    IonTextTokenizer    _scanner;
    UnifiedSymbolTable  _symbol_table ;
    
    boolean         _eof;
    State           _state;

    boolean         _in_struct;
    boolean         _skip_children;

    boolean         _value_ready;
    boolean         _value_is_container;
    boolean         _is_null;
    IonType         _value_type;
    String          _field_name;
    int             _annotation_count;
    String[]        _annotations = new String[10];
    int             _value_start;
    int             _value_end;
    
    // local stack for the parser state machine
    int             _parser_state_top;
    State[]         _parser_state_stack = new State[10];

    //  local stack for stepInto() and stepOut()
    int             _container_state_top;
    IonType[]       _container_state_stack = new IonType[10];
    
    /**
     * a single depth save stack used by getContainerSize()
     * not multi-thread safe - there's only 1 per iterator
     * and it's expensive since it has to save the parser
     * state, the scanners (tokenizers) state, and the underlying
     * readers state.  But - that's what they asked for !
     */
    IonTextIterator _saved_copy;
    // int             _saved_position;
    void save_state() {
        if (_saved_copy == null) {
            _saved_copy = new IonTextIterator();
        }
        copy_state_to(_saved_copy);
        _saved_copy._scanner = this._scanner.get_saved_copy();
    }
    void restore_state() {
        _saved_copy.copy_state_to(this);
        this._scanner.restore_state();
    }
    void copy_state_to(IonTextIterator other) {
        
        
        
        other._annotation_count = this._annotation_count;
        if (this._annotation_count > 0) { 
            if (other._annotations.length < this._annotations.length) {
                other._annotations = new String[this._annotations.length];
            }
            System.arraycopy(this._annotations, 0, other._annotations, 0, this._annotation_count);
        }
        other._container_state_top = this._container_state_top;
        if (other._container_state_stack.length < this._container_state_top) {
            other._container_state_stack = new IonType[this._container_state_stack.length];
        }
        System.arraycopy(this._container_state_stack,  0
                        ,other._container_state_stack, 0
                        ,this._container_state_stack.length);

        other._lookahead_type = this._lookahead_type;

        other._extended_value_count = this._extended_value_count;
        if (this._extended_value_count > 0) {
            if ( other._extended_value_end == null
              || other._extended_value_end.length < this._extended_value_count
            ) {
                other._extended_value_end   = new int[this._extended_value_end.length]; 
                other._extended_value_start = new int[this._extended_value_end.length];
            }
            System.arraycopy(this._extended_value_end, 0
                           , other._extended_value_end, 0
                           , this._extended_value_end.length);
            System.arraycopy(this._extended_value_start, 0
                           , other._extended_value_start, 0
                           , this._extended_value_start.length);
        }
        
        other._parser_state_top = this._parser_state_top;
        if (other._parser_state_stack.length < this._parser_state_top) {
            other._parser_state_stack = new State[this._parser_state_top];
        }
        System.arraycopy(this._parser_state_stack, 0
                       , other._parser_state_stack, 0
                       , this._parser_state_stack.length);
        
        other._symbol_table = this._symbol_table;
        
        other._current_depth = this._current_depth;
        other._eof = this._eof;
        other._skip_children = this._skip_children;
        other._state = this._state;
        other._value_ready = this._value_ready;
        other._field_name = this._field_name;
        other._in_struct = this._in_struct;
        other._is_null = this._is_null;
        other._value_is_container = this._value_is_container;
        other._value_type = this._value_type;
        other._value_start = this._value_start;
        other._value_end = this._value_end;
    }
    

    void push_parser_state(State pendingState) {
        if (_parser_state_top >= _parser_state_stack.length) {
            int newlen = _parser_state_stack.length * 2;
            State[] temp = new State[newlen];
            System.arraycopy(_parser_state_stack, 0, temp, 0, _parser_state_top);
            _parser_state_stack = temp;
        }
        _parser_state_stack[_parser_state_top++] = pendingState;
    }
    State pop_parser_state() {
        _parser_state_top--;
        State s = _parser_state_stack[_parser_state_top];
        return s;
    }
    void makeNullValue(IonType t) {
        _is_null = true;
        _value_type = t;
    }
    
    void push_container_state(IonType newContainer) {
        if (_container_state_top >= _container_state_stack.length) {
            int newlen = _container_state_stack.length * 2;
            IonType[] temp = new IonType[newlen];
            System.arraycopy(_container_state_stack, 0, temp, 0, _container_state_top);
            _container_state_stack = temp;
        }
        _container_state_stack[_container_state_top++] = newContainer;
    }
    IonType pop_container_state() {
        _container_state_top--;
        IonType t = _container_state_stack[_container_state_top];
        return t;
    }
    void startCollection(IonType t) {
         push_container_state(t);
        _in_struct = (t.equals(IonType.STRUCT));
        _is_null = false;
        _value_ready = true;
        _value_is_container = true;
        _value_type = t; 
    }
    void closeCollection(IonType t) {
        IonType actual = pop_container_state();
        _in_struct = (t.equals(IonType.STRUCT));
        if (!t.equals(actual)) {
            throw new IonParsingException("mismatch start collection and end collection"+_scanner.input_position());
        }
        clearAnnotationList();
        clearFieldname();
        _value_ready = false;
        _value_is_container = false;
    }
    private IonTextIterator() {}
    public IonTextIterator(byte[] buf, int start, int len) {
        this(new IonTextTokenizer(buf, start, len));
    }
    public IonTextIterator(byte[] buf) {
        this(new IonTextTokenizer(buf));
    }
    public IonTextIterator(String ionText) {
        this(new IonTextTokenizer(ionText));
    }
    IonTextIterator(IonTextTokenizer scanner) {
        this._scanner = scanner;
        _state = IonTextIterator.State_read_datagram;
    }
    
    final void setFieldname(String fieldname) {
    	if (fieldname == null || fieldname.length() < 1) {
    		error();
    	}
        _field_name = fieldname;
    }
    final void clearFieldname() {
        _field_name = null;
    }
   
    void appendAnnotation(String name) {
        if (_annotation_count >= _annotations.length) {
            int newlen = _annotations.length * 2;
            String[] temp = new String[newlen];
            System.arraycopy(_annotations, 0, temp, 0, _annotation_count);
            _annotations = temp;
        }
        _annotations[_annotation_count++] = name;
    }
    void clearAnnotationList() {
        _annotation_count = 0;
    }
    
    @Override
    public int getContainerSize() {
        int size = 0;
        
        if (_value_type == null || _eof) {
            throw new IllegalStateException();
        }
        switch (_value_type) {
            case STRUCT:
            case LIST:
            case SEXP:
                // this is the OK case
                break;
            default:
                throw new IllegalStateException();
        }
        
        save_state();
        
        stepInto();
        while (hasNext()) {
            next();
            size++;
        }
        
        restore_state();
        
        return size;
    }
    
    
    @Override
    public void stepInto()
    {
        if (_value_type == null || _eof) {
            throw new IllegalStateException();
        }
        switch (_value_type) {
            case STRUCT:
            case LIST:
            case SEXP:
                _current_depth++;
                if (_debug) System.out.println("stepInto() new depth: "+this._current_depth);
                return;
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public void stepOut()
    {
        if (_current_depth < 1) {
            throw new IllegalStateException();
        }
        _current_depth--;
        if (_debug) System.out.println("stepOUT() new depth: "+this._current_depth);
        _eof = false;
    }

    @Override
    public int getDepth() {
        return _current_depth;
    }

    IonType _lookahead_type = null;
    int _current_depth = 0;
    
    @Override
    public boolean hasNext() 
    {    
        if (_eof) return false;
        if (_lookahead_type != null) return true;
        
        _lookahead_type = lookahead();
        _eof = (_lookahead_type == null);

        return !_eof;
    }
    @Override
    public IonType next() 
    {
        if (_lookahead_type == null && !hasNext()) {
            throw new NoSuchElementException();
        }
        IonType t = _lookahead_type;
        _lookahead_type = null;
        return t;
    }
    
    IonType lookahead() {
        _value_ready = false;
        _value_is_container = false;
        for (;;) {
            if (_eof) return null;
    
            int token = _scanner.lookahead(0);
            Matches found = _state.nextTokens[token];
            if (_debug) {
                System.out.print("on token "+IonTextTokenizer.getTokenName(token));
                System.out.print(" from state: " + _state.toString());
            }
            if (found == null) {
                if (_debug) {
                    System.out.println(" to error");
                }
                error();
            }
            else {
                _state = found.transition_method(this);
                if (_debug) {
                    System.out.println(" to state: " + _state.toString());
                }
            }
            if (_container_state_top < _current_depth) {
                // this should be picked up by hasNext() and turned into
                // _eof = true
                return null;
            }
            if (_value_ready) {
                if ((_value_is_container && (_container_state_top == _current_depth + 1))
                 || (!_value_is_container && (_container_state_top == _current_depth)) 
                ) {
                    if (_debug) {
                        System.out.print(" lookahead returning value "+this._value_type.toString());
                        System.out.println(" @ "+this._scanner.currentCharStart());
                    }
                    return _value_type;
                }
            }
        }
    }
    void error() {
        throw new IonParsingException("syntax error. parser in state " + _state.toString()+ _scanner.input_position());
    }
    @Override
    public int getTypeId()
    {
        switch (_value_type) {
            case NULL:      return IonConstants.tidNull;
            case BOOL:      return IonConstants.tidBoolean;
            case INT:
                // BUGBUG: we really need to look at the sign of the token for this
                            return IonConstants.tidPosInt;
            case FLOAT:     return IonConstants.tidFloat;
            case DECIMAL:   return IonConstants.tidDecimal;
            case TIMESTAMP: return IonConstants.tidTimestamp;
            case STRING:    return IonConstants.tidString;
            case SYMBOL:    return IonConstants.tidSymbol;
            case BLOB:      return IonConstants.tidBlob;
            case CLOB:      return IonConstants.tidClob;
            case STRUCT:    return IonConstants.tidStruct;
            case LIST:      return IonConstants.tidList;
            case SEXP:      return IonConstants.tidSexp;
        }
        return -1;
    }

    @Override
    public IonType getType()
    {
        return _value_type;
    }

    @Override
    public UnifiedSymbolTable getSymbolTable() 
    {
        return _symbol_table;
    }

    @Override
    public void setSymbolTable(UnifiedSymbolTable  externalsymboltable) 
    {
        if (_symbol_table == null) {
            _symbol_table = new UnifiedSymbolTable(UnifiedSymbolTable.getSystemSymbolTableInstance()); 
        }
        _symbol_table.addImportedTable(externalsymboltable);
    }

    
    @Override
    public int[] getAnnotationIds()
    {
        if (!_value_ready || _symbol_table == null) {
            throw new IllegalStateException();
        }
        int[] ids = null;
        if (_annotation_count > 0) {
            ids = new int[_annotation_count];
            for (int ii=0; ii<_annotation_count; ii++) {
                ids[ii] = _symbol_table.findSymbol(_annotations[ii]);
            }
        }
        return ids;
    }

    @Override
    public String[] getAnnotations()
    {
        if (!_value_ready) {
            throw new IllegalStateException();
        }
        String[] annotations = null;
        if (_annotation_count > 0) {
            annotations = new String[_annotation_count];
            System.arraycopy(_annotations, 0, annotations, 0, _annotation_count);
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
    public int getFieldId()
    {
        if (!_value_ready || _symbol_table == null) {
            throw new IllegalStateException();
        }
        int id = 0;
        if (_field_name != null) {
            id = _symbol_table.findSymbol(_field_name);
        }
        return id;
    }

    @Override
    public String getFieldName()
    {
        if (!_value_ready) {
            throw new IllegalStateException();
        }
        return _field_name;
    }

    @Override
    public IonValue getIonValue(IonSystem sys)
    {
        int tid = getTypeId();
        if (isNull()) {
            switch (tid) {
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
        
        switch (tid) {
        case IonConstants.tidNull:      return sys.newNull();
        case IonConstants.tidBoolean:   return sys.newBool(getBool());
        case IonConstants.tidPosInt:    
        case IonConstants.tidNegInt:    return sys.newInt(getLong());
        case IonConstants.tidFloat:     
            IonFloat f = sys.newNullFloat();
            f.setValue(getDouble());
            return f;
        case IonConstants.tidDecimal:
            IonDecimal dec = sys.newNullDecimal();
            dec.setValue(getBigDecimal());
            return dec;
        case IonConstants.tidTimestamp: 
            IonTimestamp t = sys.newNullTimestamp();
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
            fillContainerList(sys, list);
            return list;
        case IonConstants.tidSexp:
            IonSexp sexp = sys.newNullSexp();
            fillContainerList(sys, sexp);
            return sexp;
        case IonConstants.tidStruct:
            IonStruct struct = sys.newNullStruct();
            fillContainerStruct(sys, struct);
            return struct;
        default:
            throw new IonException("unrecognized type encountered");
        }
    }
    void fillContainerList(IonSystem sys, IonSequence list) {
        this.stepInto();
        while (this.hasNext()) {
            this.next();
            IonValue v = this.getIonValue(sys);
            list.add(v);
        }
        this.stepOut();
    }
    void fillContainerStruct(IonSystem sys, IonStruct struct) {
        this.stepInto();
        while (this.hasNext()) {
            this.next();
            String name = this.getFieldName(); 
            IonValue v = this.getIonValue(sys);
            struct.add(name, v);
        }
        this.stepOut();
    }

    @Override
    public boolean isInStruct()
    {
        return _in_struct;
    }

    @Override
    public boolean isNull()
    {
        return _is_null;
    }

    @Override
    public boolean getBool()
    {
        if (_value_type.equals(IonType.BOOL)) {
            int kw = _scanner.keyword(_value_start, _value_end);
            if (kw == IonTextTokenizer.KEYWORD_TRUE) return true;
            if (kw == IonTextTokenizer.KEYWORD_FALSE) return false;
        }
        throw new IllegalStateException("current value is not a boolean");
    
    }

    @Override
    public int getInt()
    {
        switch (_value_type) {
            case INT:
                int intvalue = Integer.parseInt(_scanner.getValueAsString(_value_start, _value_end));
                return intvalue;
            case FLOAT:
                double d = getDouble();
                return (int)d;
            case DECIMAL:
                BigDecimal bd = getBigDecimal();
                return bd.intValue();
            default:
                break;
        }
        throw new IllegalStateException("current value is not an ion int, float, or decimal");
    }

    @Override
    public long getLong()
    {
        switch (_value_type) {
            case INT:
                long longvalue = Long.parseLong(_scanner.getValueAsString(_value_start, _value_end));
                return longvalue;
            case FLOAT:
                double d = getDouble();
                return (long)d;
            case DECIMAL:
                BigDecimal bd = getBigDecimal();
                return bd.longValue();
            default:
                break;
        }
        throw new IllegalStateException("current value is not an ion int, float, or decimal");
    }

    @Override
    public double getDouble()
    {
        switch (_value_type) {
            case FLOAT:
                double d = Double.parseDouble(_scanner.getValueAsString(_value_start, _value_end));
                return d;
            case DECIMAL:
                BigDecimal bd = getBigDecimal();
                return bd.doubleValue();
            default:
                break;
        }
        throw new IllegalStateException("current value is not an ion float or decimal");
    }

    @Override
    public BigDecimal getBigDecimal()
    {
        switch (_value_type) {
            case DECIMAL:
            	String s = _scanner.getValueAsString(_value_start, _value_end);
            	s = s.replace('d', 'e');
            	s = s.replace('D', 'E');
                BigDecimal bd = new BigDecimal(s);
                return bd;
            default:
                break;
        }
        throw new IllegalStateException("current value is not an ion decimal");
    }

    @Override
    public timeinfo getTimestamp()
    {
        if (!_value_type.equals(IonType.TIMESTAMP)) {
            throw new IllegalStateException("current value is not a timestamp");    
        }
        String image = _scanner.getValueAsString(_value_start, _value_end);
        timeinfo ti = timeinfo.parse(image);
        return ti;
    }

    @Override
    public Date getDate()
    {
        timeinfo ti = getTimestamp();
        return ti.d;
    }

    @Override
    public String getString()
    {
        switch (_value_type) {
            case STRING:
            case SYMBOL:
                String value = _scanner.getValueAsString(_value_start, _value_end);
                return value;
            default:
                break;
        }
        throw new IllegalStateException("current value is not a symbol or string");
    }

    @Override
    public int getSymbolId()
    {
        if (this._symbol_table == null) {
            throw new IllegalStateException();
        }
        switch (_value_type) {
            case SYMBOL:
                String value = _scanner.getValueAsString(_value_start, _value_end);
                int sid = _symbol_table.findSymbol(value);
                return sid;
            default:
                break;
        }
        throw new IllegalStateException("current value is not a symbol");
    }

    public InputStream getByteStream() {
        InputStream valuestream = new ValueByteStream(_scanner._r, _extended_value_count, _extended_value_start, _extended_value_end);
        
        switch (_value_type) {
        case CLOB:
            ValueEscapedCharInputStream clobsteam
                = new ValueEscapedCharInputStream(valuestream);
            return clobsteam;
        case BLOB:
            Reader valuereader = new ValueByteReader(valuestream);
            BinaryStream binreader = new BinaryStream(valuereader, (char) 0);
            InputStream blobstream = new ValueByteStream2(binreader);
            return blobstream;
        default:
            throw new IllegalStateException("only on a CLOB or BLOB");
        }
    }
    
    int getLobByteLength(InputStream in) throws IOException {
        int len = 0;
        while (in.read() >= 0) {
            len++;
        }
        return len;
    }

    @Override
    public byte[] getBytes()
    {
        InputStream bytestream = this.getByteStream();
        byte[] bytes = null;
        
        try {
            int len = getLobByteLength(bytestream);
            bytes = new  byte[len];
            int c, pos = 0;
            bytestream.reset();
            
            while ((c = bytestream.read()) >= 0) {
                bytes[pos++] = (byte)(c & 0xff);
            }

            if (pos != len) {
                throw new IllegalStateException();
            }
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
        
        return bytes;
    }
    /**
     * this returns bytes (as char) from the extents over the input
     * byte buffer by walking the start and end arrays.  It does NOT
     * do utf 8 conversion because the two cases where it's used non-ascii
     * characters are invalid.
     */
    static class ValueByteStream extends InputStream  {
        IonTextBufferedStream in;
        int count;
        int [] starts;
        int [] ends;
        int array_pos;
        int start;
        int end;
        int pos;
        ValueByteStream(IonTextBufferedStream in, int count, int [] starts, int[] ends)
        {
            this.in = in;
            this.count = count;
            this.starts = starts;
            this.ends = ends;
            this.array_pos = 0;
            this.start = this.starts[this.array_pos];
            this.end = this.ends[this.array_pos];
            this.pos = 0;
        }
        @Override
        public void reset() {
            this.array_pos = 0;
            this.start = this.starts[this.array_pos];
            this.end = this.ends[this.array_pos];
            this.pos = 0;
        }
        @Override
        public void close()
            throws IOException
        {
            this.in = null;
            this.starts = null;
            this.ends = null;
            this.count = -1;
            this.pos = 1;
            this.end = 0;
            this.start = 0;
        }
        @Override
        public int read() throws IOException
        {
            if (this.pos >= this.end) {
                this.array_pos++;
                if (this.array_pos >= count) {
                    return -1;
                }
                this.start = this.starts[this.array_pos];
                this.end = this.ends[this.array_pos];
                this.pos = 0;
            }
            int c = this.in.getByte(pos++);
            if ((c & 0x80) != 0) {
                throw new IllegalStateException("illegal character encountered in lob image");
            }
            return c;
        }
    }
    static class ValueByteReader extends Reader {
        InputStream _in;
        
        ValueByteReader(InputStream in) {
            _in = in;
        }

        @Override
        public void close()
            throws IOException
        {
            _in = null;
        }
        @Override
        public void reset() throws IOException {
            _in.reset();
        }
        @Override
        public int read()
            throws IOException
        {
            int c = _in.read();
            return c;
        }

        @Override
        public int read(char[] arg0, int arg1, int arg2)
            throws IOException
        {
            int ii;
            for (ii=0; ii<arg2; ii++) {
                int c = read();
                if (c < 0) break;
                arg0[ii+arg1] = (char)c;
            }
            return ii;
        }
    }

    static class ValueByteStream2 extends InputStream {
        Reader _in;
        
        ValueByteStream2(Reader in) {
            _in = in;
        }

        @Override
        public void close()
            throws IOException
        {
            _in = null;
        }
        @Override
        public void reset() throws IOException {
            _in.reset();
        }
        @Override
        public int read()
            throws IOException
        {
            int c = _in.read();
            return c;
        }
    }

    static class ValueEscapedCharInputStream extends InputStream  {
        InputStream _in;
        ValueEscapedCharInputStream(InputStream in) {
            _in = in;
        }
        @Override
        public void close()
            throws IOException
        {
            _in = null;
        }
        @Override
        public void reset() throws IOException {
            _in.reset();
        }
        @Override
        public int read()
            throws IOException
        {
            int c2, c = _in.read();
            if (c < 0) return c;
            if (c == '\\') {
                switch (c) {
                case -1:
                    throw new IonParsingException(new UnexpectedEofException());
                case 't':  return '\t';
                case 'n':  return '\n';
                case 'v':  return '\u000B';
                case 'r':  return '\r';
                case 'f':  return '\f';
                case 'b':  return '\u0008';
                case 'a':  return '\u0007';
                case 'e':  return '\u001B';
                case '\\': return '\\';
                case '\"': return '\"';
                case '\'': return '\'';
                case '/':  return '/';
                case '?':  return '?';
                case 'x':
                    // a pair of hex digits
                    c = readDigit(16, true);
                    if (c < 0) break;
                    c2 = c << 4; // high nibble
                    c = readDigit(16, false);
                    if (c < 0) break;
                    return c2 + c; // high nibble + low nibble
                case 'u':
                    // exactly 4 hex digits
                    c = readDigit(16, true);
                    if (c < 0) break;
                    c2 = c << 12; // highest nibble
                    c = readDigit(16, true);
                    if (c < 0) break;
                    c2 += c << 8; // 2nd high nibble
                    c = readDigit(16, true);
                    if (c < 0) break;
                    c2 += c << 4; // almost lowest nibble
                    c = readDigit(16, true);
                    if (c < 0) break;
                    return c2 + c; // high nibble + lowest nibble
                case '0':
                    return 0;
                case '\n':
                    return read();
                default:
                    throw new IonParsingException("invalid escape sequence \"\\"
                                           + (char) c + "\" [" + c + "]");
                }
            }
            return c;
        }
        int readDigit(int radix, boolean isRequired) throws IOException 
        {
            int c = _in.read();
            if (c < 0) {
                return -1;
            }

            int c2 =  Character.digit(c, radix);
            if (c2 < 0) {
                if (isRequired) {
                    throw new IonParsingException("bad digit in escaped character '"+((char)c)+"'");
                }
                // if it's not a required digit, we just throw it back
                //unread(c);
                //return -1;
                // TODO: really, perhaps we should just support unread
                throw new IonParsingException("bad digit in escaped character '"+((char)c)+"'");
            }
            return c2;
        }
    }
    
    @Override
    public int getBytes(byte[] buffer, int offset, int len)
    {
        InputStream bytestream = this.getByteStream();
        int bytesread = 0;
        
        try {
            int c;
            
            while ((c = bytestream.read()) >= 0) {
                if (bytesread >= len) break;
                buffer[bytesread + offset] = (byte)(c & 0xff);
                bytesread++;
            }

            if (bytesread > len) {
                throw new IllegalStateException();
            }
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return bytesread;
    }
    
    void readAnnotations() 
    {
        while ((  _scanner.lookahead(0) == IonTextTokenizer.TOKEN_SYMBOL1
               || _scanner.lookahead(0) == IonTextTokenizer.TOKEN_SYMBOL2
               )
              && _scanner.lookahead(1) == IonTextTokenizer.TOKEN_DOUBLE_COLON 
            ) {
        	int kw = _scanner.keyword(_scanner._start, _scanner._end);
        	switch (kw) {
        	case IonTextTokenizer.KEYWORD_FALSE:
        	case IonTextTokenizer.KEYWORD_TRUE:
        	case IonTextTokenizer.KEYWORD_NULL:
        		error();
    		default:
    			break;
        	}
        	String name = _scanner.consumeTokenAsString();
            
            consumeToken(IonTextTokenizer.TOKEN_DOUBLE_COLON);
            appendAnnotation(name);
        }
    }

    int _extended_value_count = 0;
    int _extended_value_start[] = new int[10];
    int _extended_value_end[] = new int[10];
    void grow_extended_values() {
        int newlen = (_extended_value_start == null) ? 10 : _extended_value_start.length * 2;
        int[] temp1 = new int[newlen];
        int[] temp2 = new int[newlen];
        if (_extended_value_start != null) {
            System.arraycopy(_extended_value_start, 0, temp1, 0, _extended_value_count);
            System.arraycopy(_extended_value_end, 0, temp1, 0, _extended_value_count);
        }
        _extended_value_start = temp1;
        _extended_value_end   = temp2;
    }
    void makeValue(IonType t) {
        _extended_value_count = 0;
        _value_type = t;
        _value_start = _scanner.getValueStart();
        _value_end   = _scanner.getValueEnd();
        if (t.equals(IonType.SYMBOL) && _value_start >= _value_end) {
        	throw new IonParsingException("symbols have to have some content" + this._scanner.input_position());
        }
        _is_null = false;
        _value_ready = true;
        _scanner.consumeToken();
    }
    void openValue(IonType t) {
        _extended_value_count = 0;
        _value_type = t;
        _is_null = false;
        appendValue();
    }
    void closeValue(IonType t) {
        if (!t.equals(_value_type)) {
            throw new IonParsingException("unmatch token type closing value (internal error)" + this._scanner.input_position());
        }
        if (t.equals(IonType.SYMBOL) && _extended_value_count <= 0) {
        	throw new IonParsingException("symbols have to have some content" + this._scanner.input_position());
        }
        _value_ready = true;
    }
    void appendValue() {
        if (_extended_value_count >= _extended_value_start.length) {
            grow_extended_values();
        }
        _extended_value_start[_extended_value_count] = _scanner.getValueStart();
        _extended_value_end[_extended_value_count]   = _scanner.getValueEnd();
        _extended_value_count++;
        _scanner.consumeToken();
    }
    void consumeToken(int token) {
        int current_token = _scanner.lookahead(0); 
        if (current_token != token) {
            throw new IonParsingException("token mismatch consuming token (internal error)" + this._scanner.input_position());
        }
        _scanner.consumeToken();
    }

    /**
     * the Matches class holds an expected token and the code that 
     * should execute if the token is encountered in the input stream
     */
    static abstract class Matches {
        Matches( ) { }
        int   match_token = -1;
        abstract State transition_method(IonTextIterator parser);
    }

    static class State {
        String    name;
        Matches[] nextTokens = new Matches[IonTextTokenizer.TOKEN_MAX + 1];

        State(Matches[] matches, String statename) {
            name = statename;
            Matches last_match = null;
            for (int ii=0; ii<matches.length; ii++) {
                if (matches[ii].match_token == -1) {
                    assert (ii == matches.length - 1);
                    last_match = matches[ii];
                    break;
                }
                nextTokens[matches[ii].match_token] = matches[ii];
            }
            if (last_match != null) {
                for (int ii=0; ii<nextTokens.length; ii++) {
                    if (nextTokens[ii] != null) continue;
                    nextTokens[ii] = last_match;
                }
            }
        }
        State(Matches matches) {
            if (matches.match_token != -1) {
                nextTokens[matches.match_token] = matches;
            }
            else {
                for (int ii=0; ii<nextTokens.length; ii++) {
                    nextTokens[ii] = matches;
                }
            }
        }
        @Override
        public String toString() {
            return this.name;
        }
    }


//    state_done
//      "state_done"
//      lookahead( * )
//          { _eof = true;
//            return state_done;
//          }
    static class Lookahead_state_done extends Matches {
        @Override
        State transition_method(IonTextIterator parser) {
            parser._eof = true;
            return State_done;
        }
    }
    static State State_done = new State( new Lookahead_state_done() );

//    read_datagram {
//      "start_datagram"
//      lookahead( TOKEN_EOF )
//          { 
//            if (_top > 0) error("unprocessed pending states");
//            closeDatagram();
//            _eof = true;
//            return state_done;
//          }
//      lookahead( * )
//          {
//            clearAnnotationList()
//            push_state();
//            return read_annotated_value
//          }
//    }
    static class Lookahead_read_datagram_eof extends Matches {
        Lookahead_read_datagram_eof() {
            match_token = IonTextTokenizer.TOKEN_EOF;
        }
        @Override
        State transition_method(IonTextIterator parser) {
            parser._eof = true;
            return State_done;
        }
    }
    static class Lookahead_read_datagram extends Matches {
        @Override
        State transition_method(IonTextIterator parser) {
            parser.clearAnnotationList();
            parser.push_parser_state(State_read_datagram);
            return State_read_annotated_value;
        }
    }
    static Matches[] state_read_datagram_matches = {
        new Lookahead_read_datagram_eof (),
        new Lookahead_read_datagram ()
    };
    static State State_read_datagram = new State ( state_read_datagram_matches, "State_read_datagram" );

//    read_annotated_value {
//      "annotated_value"
//      lookahead( Identifier )
//          {
//              if (lookahead(1) == '::') {
//                  readAnnotations();
//              }
//              return read_plain_value;
//          }
//      lookahead( IdentifierLiteral )
//          {
//              if (lookahead(1) == '::') {
//                  readAnnotations();
//              }
//              return read_plain_value;
//          }
//      lookahead( * )
//          {
//              return read_plain_value;
//          }
//    }
    static class Lookahead_read_annotated_value_identifier extends Matches {
        Lookahead_read_annotated_value_identifier() {
            match_token = IonTextTokenizer.TOKEN_SYMBOL1;
        }
        @Override
        State transition_method(IonTextIterator parser) {
            parser.clearAnnotationList();
            if (parser._scanner.lookahead(1) == IonTextTokenizer.TOKEN_DOUBLE_COLON) {
                parser.readAnnotations();
            }
            return State_read_plain_value_sexp;
        }
    }
    static class Lookahead_read_annotated_value_identifier_literal extends Matches {
        Lookahead_read_annotated_value_identifier_literal () {
            match_token = IonTextTokenizer.TOKEN_SYMBOL2;
        }
        @Override
        State transition_method(IonTextIterator parser) {
            parser.clearAnnotationList();
            if (parser._scanner.lookahead(1) == IonTextTokenizer.TOKEN_DOUBLE_COLON) {
                parser.readAnnotations();
            }
            return State_read_plain_value_sexp;
        }
    }
    static class Lookahead_read_annotated_value extends Matches {
        @Override
        State transition_method(IonTextIterator parser) {
            return State_read_plain_value_sexp;
        }
    }
    static Matches[] state_read_annotated_value_matches = {
        new Lookahead_read_annotated_value_identifier(),
        new Lookahead_read_annotated_value_identifier_literal(),
        new Lookahead_read_annotated_value(),
    };
    static State State_read_annotated_value = new State ( state_read_annotated_value_matches, "State_read_annotated_value" );

//    read_plain_value {
//      "plain value"
//
//      lookahead( '(' )
//          { return read_sexp; }
//      lookahead( '{' )
//          { return read_struct; }
//      lookahead( '[' )
//          { return read_list; }
//      lookahead( Integer )
//          { makeValue(INT, consumeInteger());
//            return pop();
//          }
//      lookahead( Float )
//          { makeValue(FLOAT, consumeFloat());
//            return pop();
//          }
//      lookahead( Decimal )
//          { makeValue(DECIMAL, consumeDecimal());
//            return pop();
//          }
//      lookahead( Timestamp )
//          { makeValue(TIMESTAMP, consumeTimestamp());
//            return pop();
//          }
//      lookahead( Identifier )
//          { String identifier = consumeIdentifierAsString();
//            int keyword = lookupKeyword(identifier);
//            if (keyword == KEYWORD_TRUE) {
//              makeValue(BOOL, true);
//            }
//            else if (keyword == KEYWORD_FALSE) {
//              makeValue(BOOL, false);
//            }
//            else if (keyword == KEYWORD_NULL) {
//              return state_read_null;
//            }
//            else {
//              makeValue(SYMBOL, identifier);
//            }
//            return pop();
//          }
//      lookahead( IdentifierLiteral )
//          { makeValue(SYMBOL, consumeIdentifierLiteral());
//            return pop();
//          }
//      lookahead( StringLiteral )
//          { makeValue(STRING, consumeString1());
//            return pop();
//          }
//      lookahead( ExtendedStringLiteral )
//          { makeValue(STRING, consumeString2());
//            return pop();
//          }
//      lookahead( '{{' ) {
//          {
//            consumeToken('{{');
//            if (lookahead(0) == StringLiteral ) {
//                makeValue(CLOB, consumeString1());
//            }
//            else if (lookahead(0) == ExtendedStringLiteral ) {
//                makeValue(CLOB, consumeString2());
//            }
//            else {
//                makeValue(BLOB, consumeBase64Value());
//            }
//            consumeToken('}}'); // this errors if the closing }} isn't waiting
//            return pop();
//          }
//      // and we error i the token wasn't see in the lookahead list
//    }


    static class Lookahead_read_plain_value_open_paren extends Matches {
        Lookahead_read_plain_value_open_paren() {
            match_token = IonTextTokenizer.TOKEN_OPEN_PAREN;
        }
        @Override
        State transition_method(IonTextIterator parser) {
            parser.consumeToken(IonTextTokenizer.TOKEN_OPEN_PAREN);
            parser.startCollection(IonType.SEXP);
            return State_read_sexp_value;
        }
    }
    static class Lookahead_read_plain_value_open_brace extends Matches {
        Lookahead_read_plain_value_open_brace() {
            match_token = IonTextTokenizer.TOKEN_OPEN_BRACE;
        }
        @Override
        State transition_method(IonTextIterator parser) {
            parser.consumeToken(IonTextTokenizer.TOKEN_OPEN_BRACE);
            parser.startCollection(IonType.STRUCT);
            return State_read_struct_value;
        }
    }
    static class Lookahead_read_plain_value_open_square extends Matches {
        Lookahead_read_plain_value_open_square() {
            match_token = IonTextTokenizer.TOKEN_OPEN_SQUARE;
        }
        @Override
        State transition_method(IonTextIterator parser) {
            parser.consumeToken(IonTextTokenizer.TOKEN_OPEN_SQUARE);
            parser.startCollection(IonType.LIST);
            return State_read_list_value;
        }
    }
    static class Lookahead_read_plain_value_int extends Matches {
        Lookahead_read_plain_value_int() {
            match_token = IonTextTokenizer.TOKEN_INT;
        }
        @Override
        State transition_method(IonTextIterator parser) {
            parser.makeValue(IonType.INT);
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_plain_value_float extends Matches {
        Lookahead_read_plain_value_float() {
            match_token = IonTextTokenizer.TOKEN_FLOAT;
        }
        @Override
        State transition_method(IonTextIterator parser) {
            parser.makeValue(IonType.FLOAT);
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_plain_value_decimal extends Matches {
        Lookahead_read_plain_value_decimal() {
            match_token = IonTextTokenizer.TOKEN_DECIMAL;
        }
        @Override
        State transition_method(IonTextIterator parser) {
            parser.makeValue(IonType.DECIMAL);
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_plain_value_timestamp extends Matches {
        Lookahead_read_plain_value_timestamp() {
            match_token = IonTextTokenizer.TOKEN_TIMESTAMP;
        }
        @Override
        State transition_method(IonTextIterator parser) {
            parser.makeValue(IonType.TIMESTAMP);
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_plain_value_identifier extends Matches {
        Lookahead_read_plain_value_identifier() {
            match_token = IonTextTokenizer.TOKEN_SYMBOL1;
        }
        @Override
        State transition_method(IonTextIterator parser) {
            int keyword = parser._scanner.keyword(parser._scanner.getValueStart(), parser._scanner.getValueEnd());
            if (keyword == IonTextTokenizer.KEYWORD_TRUE) {
                parser.makeValue(IonType.BOOL);
            }
            else if (keyword == IonTextTokenizer.KEYWORD_FALSE) {
                parser.makeValue(IonType.BOOL);
            }
            else if (keyword == IonTextTokenizer.KEYWORD_NULL) {
                parser._scanner.consumeToken();
                return State_read_null;
            }
            else {
                parser.makeValue(IonType.SYMBOL);
            }
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_plain_value_identifier_literal extends Matches {
        Lookahead_read_plain_value_identifier_literal() {
            match_token = IonTextTokenizer.TOKEN_SYMBOL2;
        }
        @Override
        State transition_method(IonTextIterator parser) {
            parser.makeValue(IonType.SYMBOL);
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_plain_value_identifier_extended extends Matches {
    	Lookahead_read_plain_value_identifier_extended() {
            match_token = IonTextTokenizer.TOKEN_SYMBOL3;
        }
        @Override
        State transition_method(IonTextIterator parser) {
            parser.makeValue(IonType.SYMBOL);
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_plain_value_dot_as_identifier_extended extends Matches {
    	Lookahead_read_plain_value_dot_as_identifier_extended() {
            match_token = IonTextTokenizer.TOKEN_DOT;
        }
        @Override
        State transition_method(IonTextIterator parser) {
            parser.makeValue(IonType.SYMBOL);
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_plain_value_string_literal extends Matches {
        Lookahead_read_plain_value_string_literal() {
            match_token = IonTextTokenizer.TOKEN_STRING1;
        }
        @Override
        State transition_method(IonTextIterator parser) {
            parser.makeValue(IonType.STRING);
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_plain_value_string_literal2 extends Lookahead_read_plain_value_string_literal {
        Lookahead_read_plain_value_string_literal2() {
            match_token = IonTextTokenizer.TOKEN_STRING3;
        }
    }
    static class Lookahead_read_plain_value_string_literal_extended extends Matches {
        Lookahead_read_plain_value_string_literal_extended() {
            match_token = IonTextTokenizer.TOKEN_STRING2;
        }
        @Override
        State transition_method(IonTextIterator parser) {
            parser.makeValue(IonType.STRING);
            int token;
            for (;;) {
            	token = parser._scanner.lookahead(0);
            	if (token != IonTextTokenizer.TOKEN_STRING2
            	 && token != IonTextTokenizer.TOKEN_STRING4
            	) {
            		break;
            	}
                parser.appendValue();
            }
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_plain_value_string_literal_extended2 extends Lookahead_read_plain_value_string_literal_extended {
        Lookahead_read_plain_value_string_literal_extended2() {
            match_token = IonTextTokenizer.TOKEN_STRING4;
        }
    }
    static class Lookahead_read_plain_value_lob extends Matches {
        Lookahead_read_plain_value_lob() {
            match_token = IonTextTokenizer.TOKEN_OPEN_DOUBLE_BRACE;
        }
        @Override
        State transition_method(IonTextIterator parser) {
        	int start = -1, end = -1;
            parser.consumeToken(IonTextTokenizer.TOKEN_OPEN_DOUBLE_BRACE);
            int lookahead_char = parser._scanner.lob_lookahead();
              
            // if (parser._scanner.lookahead(0) == IonTextTokenizer.TOKEN_STRING1 ) { // or parser.peek() == '"'
            if (lookahead_char == '"') {
            	if (parser._scanner.lookahead(0) != IonTextTokenizer.TOKEN_STRING3) {
            		parser.error();
            	}
            	parser.makeValue(IonType.CLOB);
            }
            //else if (parser._scanner.lookahead(0) == IonTextTokenizer.TOKEN_STRING4 ) { // or parser.peek() == '\''
            else if (lookahead_char == '\'') {
            	if (parser._scanner.lookahead(0) != IonTextTokenizer.TOKEN_STRING4) {
            		parser.error();
            	}
            	parser.openValue(IonType.CLOB);
            	while (parser._scanner.lookahead(0) == IonTextTokenizer.TOKEN_STRING4) {
            		parser.appendValue();
            	}
            	parser.closeValue(IonType.CLOB);
            }
            else {
            	parser._scanner.scanBase64Value(parser);
            	start = parser._scanner._start;
            	end = parser._scanner._end;
            	parser._scanner.enqueueToken(IonTextTokenizer.TOKEN_BLOB, start, end);
            	parser.makeValue(IonType.BLOB);
            }
            // this checks to see that we have a single close and the
            // next character is another close (and it consumes the
            // next character if it is the 2nd single close brace)
            int end_token = parser._scanner.lookahead(0); 
            if (end_token != IonTextTokenizer.TOKEN_CLOSE_BRACE) {
            	parser.error();
            }
            if (!parser._scanner.isReallyDoubleBrace()) {
            	parser.error();
            }
            // note isReally doesn't change the token type that's pending
            // so we have to close a single brace to match what the parser
            // is expecting
            parser.consumeToken(IonTextTokenizer.TOKEN_CLOSE_BRACE); // this errors if the closing }} isn't waiting
            return parser.pop_parser_state();
        }
    }
    static Matches[] state_plain_value_matches = {
        new Lookahead_read_plain_value_open_paren(),
        new Lookahead_read_plain_value_open_brace(),
        new Lookahead_read_plain_value_open_square(),
        new Lookahead_read_plain_value_int(),
        new Lookahead_read_plain_value_float(),
        new Lookahead_read_plain_value_decimal(),
        new Lookahead_read_plain_value_timestamp(),
        new Lookahead_read_plain_value_identifier(),
        new Lookahead_read_plain_value_identifier_literal(),
        new Lookahead_read_plain_value_string_literal(),
        new Lookahead_read_plain_value_string_literal2(),
        new Lookahead_read_plain_value_string_literal_extended(),
        new Lookahead_read_plain_value_string_literal_extended2(),
        new Lookahead_read_plain_value_lob(),
    };
    static State State_read_plain_value  = new State ( state_plain_value_matches, "State_read_plain_value" );
    
    static Matches[] state_plain_value_sexp_matches = {
        new Lookahead_read_plain_value_open_paren(),
        new Lookahead_read_plain_value_open_brace(),
        new Lookahead_read_plain_value_open_square(),
        new Lookahead_read_plain_value_int(),
        new Lookahead_read_plain_value_float(),
        new Lookahead_read_plain_value_decimal(),
        new Lookahead_read_plain_value_timestamp(),
        new Lookahead_read_plain_value_identifier(),
        new Lookahead_read_plain_value_identifier_literal(),
        new Lookahead_read_plain_value_identifier_extended(),
        new Lookahead_read_plain_value_dot_as_identifier_extended(),
        new Lookahead_read_plain_value_string_literal(),
        new Lookahead_read_plain_value_string_literal2(),
        new Lookahead_read_plain_value_string_literal_extended(),
        new Lookahead_read_plain_value_string_literal_extended2(),
        new Lookahead_read_plain_value_lob(),
    };
    static State State_read_plain_value_sexp  = new State ( state_plain_value_sexp_matches, "State_read_plain_value_sexp" );


//    state_read_null {
//      "read null",
//      lookahead( '.' )
//          {
//            consumeToken('.');
//            if (lookahead(0) != Identifier) {
//              error();
//            }
//            String identifier = consumeIdentifierAsString();
//            int keyword = lookupKeyword(identifier);
//            switch (keyword) {
//              case kw_bool:   makeValue(BOOL, null);      break;
//              case kw_int:    makeValue(INT, null);       break;
//              case float: makeValue(FLOAT, null);     break;
//              case decimal:   makeValue(DECIMAL, null);   break;
//              case timestamp: makeValue(TIMESTAMP, null); break;
//              case symbol:    makeValue(SYMBOL, null);    break;
//              case string:    makeValue(STRING, null);    break;
//              case blob:  makeValue(BLOB, null);      break;
//              case clob:  makeValue(CLOB, null);      break;
//              case list:  makeValue(LIST, null);      break;
//              case sexp:  makeValue(SEXP, null);      break;
//              case struct:    makeValue(STRUCT, null);    break;
//              default:    error();
//            }
//            return pop();
//          }
//      lookahead( * )
//          { makeValue(NULL, null); 
//            return pop();
//          }
//    }

    static class Lookahead_read_null_dot extends Matches {
        Lookahead_read_null_dot() {
            match_token = IonTextTokenizer.TOKEN_DOT;
        }
        @Override
        State transition_method(IonTextIterator parser) {
            parser.consumeToken(IonTextTokenizer.TOKEN_DOT);
            if (parser._scanner.lookahead(0) != IonTextTokenizer.TOKEN_SYMBOL1) {
                parser.error();
            }
            int keyword = parser._scanner.keyword(parser._scanner.getValueStart(), parser._scanner.getValueEnd());
            switch (keyword) {
            	case IonTextTokenizer.KEYWORD_NULL:     parser.makeNullValue(IonType.NULL);     break;
                case IonTextTokenizer.KEYWORD_BOOL:     parser.makeNullValue(IonType.BOOL);     break;
                case IonTextTokenizer.KEYWORD_INT:      parser.makeNullValue(IonType.INT);      break;
                case IonTextTokenizer.KEYWORD_FLOAT:    parser.makeNullValue(IonType.FLOAT);    break;
                case IonTextTokenizer.KEYWORD_DECIMAL:  parser.makeNullValue(IonType.DECIMAL);  break;
                case IonTextTokenizer.KEYWORD_TIMESTAMP:parser.makeNullValue(IonType.TIMESTAMP);break;
                case IonTextTokenizer.KEYWORD_SYMBOL:   parser.makeNullValue(IonType.SYMBOL);   break;
                case IonTextTokenizer.KEYWORD_STRING:   parser.makeNullValue(IonType.STRING);   break;
                case IonTextTokenizer.KEYWORD_BLOB:     parser.makeNullValue(IonType.BLOB);     break;
                case IonTextTokenizer.KEYWORD_CLOB:     parser.makeNullValue(IonType.CLOB);     break;
                case IonTextTokenizer.KEYWORD_LIST:     parser.makeNullValue(IonType.LIST);     break;
                case IonTextTokenizer.KEYWORD_SEXP:     parser.makeNullValue(IonType.SEXP);     break;
                case IonTextTokenizer.KEYWORD_STRUCT:   parser.makeNullValue(IonType.STRUCT);
                	break;
                default:
            		parser.error();
            }
            parser.consumeToken(IonTextTokenizer.TOKEN_SYMBOL1);
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_null extends Matches {
        @Override
        State transition_method(IonTextIterator parser) {
            parser.makeNullValue(IonType.NULL);
            return parser.pop_parser_state();
        }
    }
    static Matches[] state_read_null_matches = {
        new Lookahead_read_null_dot(),
        new Lookahead_read_null(),
    };
    static State State_read_null = new State ( state_read_null_matches, "State_read_null" );

//    read_sexp_value {
//      "read sexp value"
//      lookahead( Identifier ) 
//          {
//            if (lookahead(1) == '::' ) {
//              readAnnotations();
//            }
//            return read_sexp_value;
//          }
//      lookahead( IdentifierLiteral )
//          {
//            if (lookahead(1) == '::' ) {
//              readAnnotations();
//            }
//            return read_sexp_value;
//          }
//      lookahead( extendsedIdentifier, * )
//    /     {
//            makeValue(SYMBOL, consumeextendsedIdentifierAsString());
//            return read_sexp_value;
//          }
//      lookahead( ')' )
//          { consumeToken(')');
//            closeCollection(SEXP);
//            return pop();
//          }
//      lookahead( *, * )
//          { push(read_sexp_value);
//            return read_plain_value;
//          }
//    }
    static class Lookahead_read_sexp_value_identifier extends Matches {
        Lookahead_read_sexp_value_identifier() {
            match_token = IonTextTokenizer.TOKEN_SYMBOL1;
        }
        @Override
        State transition_method(IonTextIterator parser) {
            parser.clearAnnotationList();
            parser.clearFieldname();
            if ( parser._scanner.lookahead(1) == IonTextTokenizer.TOKEN_DOUBLE_COLON ) 
            {
                parser.readAnnotations();
            }
            
            parser.push_parser_state( State_read_sexp_value );
            parser.push_parser_state(State_close_value);
            return State_read_plain_value_sexp;
            
            //parser.makeValue(IonType.SYMBOL);
            //return State_read_sexp_value;
        }
    }
    static class Lookahead_read_sexp_value_identifier_literal extends Matches {
        Lookahead_read_sexp_value_identifier_literal() {
            match_token = IonTextTokenizer.TOKEN_SYMBOL2;
        }
        @Override
        State transition_method(IonTextIterator parser) {
            parser.clearAnnotationList();
            parser.clearFieldname();
            if ( parser._scanner.lookahead(1) == IonTextTokenizer.TOKEN_DOUBLE_COLON ) {
                parser.readAnnotations();
            }
            
            parser.push_parser_state( State_read_sexp_value );
            parser.push_parser_state(State_close_value);
            return State_read_plain_value_sexp;
            
            //parser.makeValue(IonType.SYMBOL);
            //return State_read_sexp_value;
        }

    }
    static class Lookahead_read_sexp_value_extended_identifier extends Matches {
        Lookahead_read_sexp_value_extended_identifier() {
            match_token = IonTextTokenizer.TOKEN_SYMBOL3;
        }
        @Override
        State transition_method(IonTextIterator parser) {
            parser.clearAnnotationList();
            parser.clearFieldname();
            parser.makeValue(IonType.SYMBOL);
            return State_read_sexp_value;
        }
    }
    static class Lookahead_read_sexp_value_close_paren extends Matches {
        Lookahead_read_sexp_value_close_paren() {
            match_token = IonTextTokenizer.TOKEN_CLOSE_PAREN;
        }
        @Override
        State transition_method(IonTextIterator parser){
            parser.consumeToken(IonTextTokenizer.TOKEN_CLOSE_PAREN);
            parser.closeCollection(IonType.SEXP);
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_sexp_value extends Matches {
        @Override
        State transition_method(IonTextIterator parser) {
            parser.clearAnnotationList();
            parser.clearFieldname();
            parser.push_parser_state(State_read_sexp_value);
            parser.push_parser_state(State_close_value);
            return State_read_plain_value_sexp;
        }
    }
    static Matches[] state_read_sexp_value_matches = {
        new Lookahead_read_sexp_value_identifier(),
        new Lookahead_read_sexp_value_identifier_literal(),
        new Lookahead_read_sexp_value_extended_identifier(),
        new Lookahead_read_sexp_value_close_paren(),
        new Lookahead_read_sexp_value(),
    };
    static State State_read_sexp_value = new State ( state_read_sexp_value_matches, "State_read_sexp_value" );
    
    
    static class Lookahead_close_value extends Matches {
        @Override
        State transition_method(IonTextIterator parser) {
            parser.clearAnnotationList();
            parser.clearFieldname();
            return parser.pop_parser_state();
        }
    }
    static Matches[] state_close_value_matches = {
        new Lookahead_close_value(),
    };
    static State State_close_value = new State ( state_close_value_matches, "State_close_value" );
      

//    read_list_value {
//      lookahead( ']' )
//          { consumeToken(']');
//            closeCollection(LIST);
//            return pop();
//          }
//      lookahead( Identifier )
//          {
//            if (lookahead(1) == '::' ) {
//              readAnnotations();
//            }
//            push( read_list_comma );
//            return read_plain_value;
//          }
//      lookahead( IdentifierLiteral, )
//          {
//            if (lookahead(1) == '::' ) {
//              readAnnotations();
//            }
//            push( read_list_comma );
//            return read_plain_value;
//          }
//      lookahead( * )
//          {
//            push( read_list_comma );
//            return read_plain_value;
//          }
//    }
    static class Lookahead_read_list_value_close_square extends Matches {
        Lookahead_read_list_value_close_square() {
            match_token = IonTextTokenizer.TOKEN_CLOSE_SQUARE;
        }
        @Override
        State transition_method(IonTextIterator parser){
            parser.consumeToken(IonTextTokenizer.TOKEN_CLOSE_SQUARE);
            parser.closeCollection(IonType.LIST);
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_list_value_identifier extends Matches {
        Lookahead_read_list_value_identifier() {
            match_token = IonTextTokenizer.TOKEN_SYMBOL1;
        }
        @Override
        State transition_method(IonTextIterator parser){
            parser.clearAnnotationList();
            parser.clearFieldname();
            if ( parser._scanner.lookahead(1) == IonTextTokenizer.TOKEN_DOUBLE_COLON ) {
                parser.readAnnotations();
            }
            parser.push_parser_state( State_read_list_comma );
            parser.push_parser_state(State_close_value);
            return State_read_plain_value;
        }
    }
    static class Lookahead_read_list_value_identifier_literal extends Matches {
        Lookahead_read_list_value_identifier_literal() {
            match_token = IonTextTokenizer.TOKEN_SYMBOL2;
        }
        @Override
        State transition_method(IonTextIterator parser){
            parser.clearAnnotationList();
            parser.clearFieldname();
            if ( parser._scanner.lookahead(1) == IonTextTokenizer.TOKEN_DOUBLE_COLON ) {
                parser.readAnnotations();
            }
            parser.push_parser_state( State_read_list_comma );
            parser.push_parser_state(State_close_value);
            return State_read_plain_value;
        }
    }
    static class Lookahead_read_list_value extends Matches {
        @Override
        State transition_method(IonTextIterator parser) {
            parser.clearAnnotationList();
            parser.clearFieldname();
            parser.push_parser_state(State_read_list_comma);
            parser.push_parser_state(State_close_value);
            return State_read_plain_value;
        }
    }
    static Matches[] state_read_list_value_matches = {
        new Lookahead_read_list_value_close_square (),
        new Lookahead_read_list_value_identifier (),
        new Lookahead_read_list_value_identifier_literal (),
        new Lookahead_read_list_value (),
    };
    static State State_read_list_value = new State ( state_read_list_value_matches, "State_read_list_value" );

//    read_list_comma {
//      lookahead( ',' )
//          { consumeToken(',');
//            return read_list_value;
//          }
//      lookahead( ']' )
//          { consumeToken(']');
//            closeCollection(LIST);
//            return pop();
//          }
//    }
    static class Lookahead_read_list_comma_comma extends Matches {
        Lookahead_read_list_comma_comma() {
            match_token = IonTextTokenizer.TOKEN_COMMA;
        }
        @Override
        State transition_method(IonTextIterator parser){
            parser.consumeToken( IonTextTokenizer.TOKEN_COMMA );
            return State_read_list_value;
        }
    }
    static class Lookahead_read_list_comma_close_square extends Matches {
        Lookahead_read_list_comma_close_square() {
            match_token = IonTextTokenizer.TOKEN_CLOSE_SQUARE;
        }
        @Override
        State transition_method(IonTextIterator parser){
            parser.consumeToken( IonTextTokenizer.TOKEN_CLOSE_SQUARE );
            parser.closeCollection( IonType.LIST );
            return parser.pop_parser_state();
        }
    }
    static Matches[] state_read_list_comma_matches = {
        new Lookahead_read_list_comma_comma(),
        new Lookahead_read_list_comma_close_square(),
    };
    static State State_read_list_comma = new State ( state_read_list_comma_matches, "State_read_list_comma" );


//    read_struct_value {
//      lookahead( '}' )
//          { consumeToken('}');
//            closeCollection(STRUCT);
//            return pop();
//          }
//      lookahead( Identifier )
//          {
//            String fieldname = consumeIdentifierAsString();
//            consumeToken(':');
//            return read_struct_member;
//          }
//      lookahead( IdentifierLiteral )
//          {
//            String fieldname = consumeIdentifierAsString();
//            consumeToken(':');
//            return read_struct_member;
//          }
//      lookahead( StringLiteral )
//          {
//            String fieldname = consumeIdentifierAsString();
//            consumeToken(':');
//            return read_struct_member;
//          }
//    }
    static class Lookahead_read_struct_value_identifier extends Matches {
        Lookahead_read_struct_value_identifier() {
            match_token = IonTextTokenizer.TOKEN_SYMBOL1;
        }
        @Override
        State transition_method(IonTextIterator parser) {
        	int kw = parser._scanner.keyword(parser._scanner._start, parser._scanner._end);
        	switch (kw) {
        	case IonTextTokenizer.KEYWORD_TRUE:
        	case IonTextTokenizer.KEYWORD_FALSE:
        	case IonTextTokenizer.KEYWORD_NULL:
        		parser.error();
        	default:
        		break;
        	}
            String fieldname = parser._scanner.consumeTokenAsString();
            parser.setFieldname(fieldname);
            parser.consumeToken( IonTextTokenizer.TOKEN_COLON );
            return State_read_struct_member;
        }
    }
    static class Lookahead_read_struct_value_identifier_literal extends Matches {
        Lookahead_read_struct_value_identifier_literal() {
            match_token = IonTextTokenizer.TOKEN_SYMBOL2;
        }
        @Override
        State transition_method(IonTextIterator parser) {
            String fieldname = parser._scanner.consumeTokenAsString();
            parser.setFieldname(fieldname);
            parser.consumeToken( IonTextTokenizer.TOKEN_COLON );
            return State_read_struct_member;
        }
    }
    static class Lookahead_read_struct_value_string_literal extends Lookahead_read_struct_value_identifier_literal {
        Lookahead_read_struct_value_string_literal() {
            match_token = IonTextTokenizer.TOKEN_STRING1;
        }
    }
    static class Lookahead_read_struct_value_string_literal2 extends Lookahead_read_struct_value_identifier_literal {
        Lookahead_read_struct_value_string_literal2() {
            match_token = IonTextTokenizer.TOKEN_STRING3;
        }
    }
    static class Lookahead_read_struct_value_string_literal_extended extends Matches {
    	Lookahead_read_struct_value_string_literal_extended() {
            match_token = IonTextTokenizer.TOKEN_STRING2;
        }
        @Override
        State transition_method(IonTextIterator parser){
            String fieldname = parser._scanner.consumeTokenAsString();
            // this isn't an especially pretty way to do this, but if someone is
            // really creating a field name by concatinating long strings - geez,
            // what do they expect?
            int token;
            for (;;) {
            	token = parser._scanner.lookahead(0);
            	if (token != IonTextTokenizer.TOKEN_STRING2
            	 && token != IonTextTokenizer.TOKEN_STRING4
            	) {
            		break;
            	}
                fieldname += parser._scanner.consumeTokenAsString();
            }            
            parser.setFieldname(fieldname);
            parser.consumeToken( IonTextTokenizer.TOKEN_COLON );
            return State_read_struct_member;
        }
    }
    static class Lookahead_read_struct_value_string_literal_extended2 extends Lookahead_read_struct_value_string_literal_extended {
    	Lookahead_read_struct_value_string_literal_extended2() {
            match_token = IonTextTokenizer.TOKEN_STRING4;
        }
    }
    static class Lookahead_read_struct_value_close_brace extends Matches {
        Lookahead_read_struct_value_close_brace() {
            match_token = IonTextTokenizer.TOKEN_CLOSE_BRACE;
        }
        @Override
        State transition_method(IonTextIterator parser){
            parser.consumeToken( IonTextTokenizer.TOKEN_CLOSE_BRACE );
            parser.closeCollection( IonType.STRUCT );
            return parser.pop_parser_state();
        }
    }
    static Matches[] state_read_struct_value_matches = {
        new Lookahead_read_struct_value_identifier (),
        new Lookahead_read_struct_value_identifier_literal (),
        new Lookahead_read_struct_value_string_literal (),
        new Lookahead_read_struct_value_string_literal2 (),
        new Lookahead_read_struct_value_string_literal_extended (),
        new Lookahead_read_struct_value_string_literal_extended2 (),
        new Lookahead_read_struct_value_close_brace (),
    };
    static State State_read_struct_value = new State ( state_read_struct_value_matches, "State_read_struct_value" );


//    read_struct_comma {
//      lookahead( '}' )
//          { consumeToken('}');
//            closeCollection(STRUCT);
//            return pop();
//          }
//      lookahead( ',' )
//          { consumeToken(',');
//            return read_struct_value;
//          }
//    }
    static class Lookahead_read_struct_comma_comma extends Matches {
        Lookahead_read_struct_comma_comma() {
            match_token = IonTextTokenizer.TOKEN_COMMA;
        }
        @Override
        State transition_method(IonTextIterator parser){
            parser.consumeToken( IonTextTokenizer.TOKEN_COMMA );
            return State_read_struct_value;
        }
    }
    static class Lookahead_read_struct_comma_close_brace extends Matches {
        Lookahead_read_struct_comma_close_brace() {
            match_token = IonTextTokenizer.TOKEN_CLOSE_BRACE;
        }
        @Override
        State transition_method(IonTextIterator parser){
            parser.consumeToken( IonTextTokenizer.TOKEN_CLOSE_BRACE );
            parser.closeCollection( IonType.STRUCT );
            return parser.pop_parser_state();
        }
    }
    static Matches[] state_read_struct_comma_matches = {
        new Lookahead_read_struct_comma_comma (),
        new Lookahead_read_struct_comma_close_brace (),
    };
    static State State_read_struct_comma = new State ( state_read_struct_comma_matches, "State_read_struct_comma" );

//    read_struct_member {
//      lookahead( Identifier )
//          {
//            if (lookahead(1) == '::' ) {
//              readAnnotations();
//            }
//            push( read_struct_value );  //  State_read_struct_comma
//            return read_plain_value;
//          }
//      lookahead( IdentifierLiteral )
//          {
//            if (lookahead(1) == '::' ) {
//              readAnnotations();
//            }
//            push( read_struct_value ); // State_read_struct_comma
//            return read_plain_value;
//          }
//      lookahead( * )
//          {
//            push( read_struct_value );  // State_read_struct_comma
//            return read_plain_value;
//          }
//    }
    static class Lookahead_read_struct_member_identifier extends Matches {
        Lookahead_read_struct_member_identifier() {
            match_token = IonTextTokenizer.TOKEN_SYMBOL1;
        }
        @Override
        State transition_method(IonTextIterator parser){
            parser.clearAnnotationList();
            if ( parser._scanner.lookahead(1) == IonTextTokenizer.TOKEN_DOUBLE_COLON ) {
                parser.readAnnotations();
            }
            parser.push_parser_state( State_read_struct_comma );
            parser.push_parser_state(State_close_value);
            return State_read_plain_value;
        }
    }
    static class Lookahead_read_struct_member_identifier_literal extends Matches {
        Lookahead_read_struct_member_identifier_literal() {
            match_token = IonTextTokenizer.TOKEN_SYMBOL2;
        }
        @Override
        State transition_method(IonTextIterator parser){
            parser.clearAnnotationList();
            if ( parser._scanner.lookahead(1) == IonTextTokenizer.TOKEN_DOUBLE_COLON ) {
                parser.readAnnotations();
            }
            parser.push_parser_state( State_read_struct_comma );
            parser.push_parser_state(State_close_value);
            return State_read_plain_value;
        }
    }
    static class Lookahead_read_struct_member extends Matches {
        @Override
        State transition_method(IonTextIterator parser) {
            parser.clearAnnotationList();
            parser.push_parser_state( State_read_struct_comma );
            parser.push_parser_state(State_close_value);
            return State_read_plain_value;
        }
    }
    static Matches[] state_read_struct_member_matches = {
        new Lookahead_read_struct_member_identifier (),
        new Lookahead_read_struct_member_identifier_literal (),
        new Lookahead_read_struct_member (),
    };
    static State State_read_struct_member = new State ( state_read_struct_member_matches, "State_read_struct_member" );
    
    static class IonParsingException extends IonException {
		private static final long serialVersionUID = 1L;
		IonParsingException(String msg) {
    		super("Ion parser: " + msg);
    	}
    	IonParsingException(Exception e) {
    		this(e, "");
    	}
    	IonParsingException(Exception e, String msg) {
    		super("Ion parser: " + msg + e.getMessage());
    		this.setStackTrace(e.getStackTrace());
    	}
    	
    }

}
