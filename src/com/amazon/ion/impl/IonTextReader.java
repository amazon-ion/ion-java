// Copyright (c) 2008-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl.IonImplUtils.EMPTY_ITERATOR;
import static com.amazon.ion.util.IonTextUtils.printQuotedSymbol;

import com.amazon.ion.IonBlob;
import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonClob;
import com.amazon.ion.IonException;
import com.amazon.ion.IonList;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.Timestamp;
import com.amazon.ion.UnexpectedEofException;
import com.amazon.ion.impl.Base64Encoder.BinaryStream;
import com.amazon.ion.impl.IonTokenReader.Type.timeinfo;
import com.amazon.ion.util.IonTextUtils;
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
public final class IonTextReader
    implements IonReader
{

//////////////////////////////////////////////////////////////////////////debug
    static final boolean _debug = false;


    IonTextTokenizer    _scanner;
    IonCatalog          _catalog;
    SymbolTable  _current_symtab;

    boolean         _eof;
    State           _state;

    final boolean   _is_returning_system_values;

    boolean         _in_struct;
    boolean         _skip_children;

    boolean         _value_ready;
    boolean         _value_is_container;
    boolean         _is_null;
    int             _value_token;  // used to distiguish int from hex int
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
    IonTextReader _saved_copy;
    // int             _saved_position;
    void save_state() {
        if (_saved_copy == null) {
            _saved_copy = new IonTextReader();
        }
        copy_state_to(_saved_copy);
        _saved_copy._scanner = this._scanner.get_saved_copy();
    }
    void restore_state() {
        _saved_copy.copy_state_to(this);
        this._scanner.restore_state();
    }
    void copy_state_to(IonTextReader other) {

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

        other._current_symtab = this._current_symtab;

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

    private IonTextReader() {
        _is_returning_system_values = false;  // FIXME really?!?!?
    }

    public IonTextReader(byte[] buf) {
        this(new IonTextTokenizer(buf), null, false);
    }
    public IonTextReader(byte[] buf, int start, int len) {
        this(new IonTextTokenizer(buf, start, len), null, false);
    }

    public IonTextReader(byte[] buf, int start, int len,
                         IonCatalog catalog,
                         boolean returnSystemValues)
    {
        this(new IonTextTokenizer(buf, start, len),
             catalog,
             returnSystemValues);
    }

    public IonTextReader(String ionText) {
        this(new IonTextTokenizer(ionText), null, false);
    }

    public IonTextReader(String ionText,
                         IonCatalog catalog,
                         boolean returnSystemValues)
    {
        this(new IonTextTokenizer(ionText), catalog, returnSystemValues);
    }

    public IonTextReader(byte[] buf, IonCatalog catalog) {
        this(new IonTextTokenizer(buf), catalog, false);
    }
    public IonTextReader(byte[] buf, int start, int len, IonCatalog catalog) {
        this(new IonTextTokenizer(buf, start, len), catalog, false);
    }
    public IonTextReader(String ionText, IonCatalog catalog) {
        this(new IonTextTokenizer(ionText), catalog, false);
    }

    private IonTextReader(IonTextTokenizer scanner, IonCatalog catalog,
                          boolean returnSystemValues)
    {
        this._is_returning_system_values = returnSystemValues;
        this._scanner = scanner;
        this._catalog = catalog;
        this._current_symtab = UnifiedSymbolTable.getSystemSymbolTableInstance();
        _state = IonTextReader.State_read_datagram;
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

    public void stepIn()
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

    public void stepOut()
    {
        if (_current_depth < 1) {
            throw new IllegalStateException();
        }
        _current_depth--;
        if (_debug) System.out.println("stepOUT() new depth: "+this._current_depth);
        _eof = false;
    }

    public int getDepth() {
        return _current_depth;
    }

    IonType _lookahead_type = null;
    int _current_depth = 0;

    public boolean hasNext()
    {
        boolean has_next = hasNextHelper();

        while (has_next) {
            if (!checkForSystemValuesToSkipOrProcess()) break;
            _lookahead_type = null;
            has_next = hasNextHelper();
        }

        // if we have a "next" then we better not be at eof
        // and visa versa
        assert has_next == !_eof;

        return has_next;
    }
    private boolean hasNextHelper()
    {
        if (_eof) return false;

        if (_lookahead_type == null) {
            _lookahead_type = lookahead();
            _eof = (_lookahead_type == null);
        }

        return !_eof;
    }

    /**
     * @return true iff the current value is a system value to be skipped.
     */
    private boolean checkForSystemValuesToSkipOrProcess() {
        boolean skip_value = false;

        // we only look for system values at the top level,
        if (_current_depth == 0) {

            // the only system value we skip at the top
            // is a local symbol table, but we process the
            // version symbol, and shared symbol tables too
            switch (_lookahead_type) {
            case SYMBOL:
                if (UnifiedSymbolTable.ION_1_0.equals(this.stringValue())) {
                    _current_symtab = UnifiedSymbolTable.getSystemSymbolTableInstance();
                    skip_value = true; // FIXME get system tab from current
                }
                break;
            case STRUCT:

                /*
                it looks like we'll need to "save" this state to avoid
                having to make a big copy (hard to say which is cheaper really)

                to save state we need a token stream with:
                    token_marker: start, end
                    type, value TM,  annotations TM's fieldname TM
                */

                if (_annotation_count > 0) {
                    // TODO - this should be done with flags set while we're
                    // recognizing the annotations below (in the fullness of time)
                    if (hasAnnotation(UnifiedSymbolTable.ION_SYMBOL_TABLE)) {

                        if (_is_returning_system_values) this.save_state();

                        SymbolTable local = loadLocalSymbolTable();
                        if (local != null) { //FIXME shouldn't happen
                            _current_symtab = local;
                            skip_value = true;
                        }

                        if (_is_returning_system_values) this.restore_state();

                    }
                }
                break;
            default:
                break;
            }
        }

        return _is_returning_system_values ? false : skip_value;
    }

    public IonType next()
    {
        if (_lookahead_type == null) {
            // we check just in case the caller didn't call hasNext()
            if (!hasNext()) {
                // so if there really no next, it's a problem here
                throw new NoSuchElementException();
            }
            // if there's something to return then it better have a type
            assert _lookahead_type != null;
        }

        // if we return the lookahead type from next, it's gone
        // this will force hasNext() to look again
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
    IonType lookahead_non_debug() {
        _value_ready = false;
        _value_is_container = false;
        for (;;) {
            if (_eof) return null;

            int token = _scanner.lookahead(0);
            Matches found = _state.nextTokens[token];

            _state = found.transition_method(this);
            if (_container_state_top < _current_depth) {
                return null; // test is for skipping nested children, turned into eof by hasNext()
            }
            if (_value_ready && isValueCurrent()) {
                return _value_type;
            }
        }
    }
    private final boolean isValueCurrent() {
        boolean is_current;
        if (_value_is_container) {
            is_current = (_container_state_top == _current_depth + 1);
        }
        else {
            is_current = (_container_state_top == _current_depth);
        }
        return is_current;
    }

    void error() {
        throw new IonParsingException("syntax error. parser in state " + _state.toString()+ _scanner.input_position());
    }

    void error(String reason) {
        String message =
            "Syntax error" + _scanner.input_position() + ": " + reason;
        throw new IonParsingException(message);
    }

    protected SymbolTable loadLocalSymbolTable() {
        // TODO do we really want only the system symtab in context?
        SymbolTable temp = _current_symtab;
        _current_symtab = _current_symtab.getSystemSymbolTable();
        this.stepIn();
        UnifiedSymbolTable table =
            new UnifiedSymbolTable(_current_symtab, this, _catalog);
        this.stepOut();
        _current_symtab = temp;
        return table;
    }


    public IonType getType()
    {
        return _value_type;
    }

    public SymbolTable getSymbolTable()
    {
        if (_current_symtab == null) {
            _current_symtab = new UnifiedSymbolTable(UnifiedSymbolTable.getSystemSymbolTableInstance());
        }
        assert _current_symtab.isLocalTable() || _current_symtab.isSystemTable();
        return _current_symtab;
    }

    public int[] getTypeAnnotationIds()
    {
        if (!_value_ready || _current_symtab == null) {
            throw new IllegalStateException();
        }
        int[] ids = null;
        if (_annotation_count > 0) {
            ids = new int[_annotation_count];
            for (int ii=0; ii<_annotation_count; ii++) {
                ids[ii] = _current_symtab.findSymbol(_annotations[ii]);
            }
        }
        return ids;
    }

    public String[] getTypeAnnotations()
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

    public boolean hasAnnotation(String annotation)
    {
        if (!_value_ready) {
            throw new IllegalStateException();
        }
        for (int ii=0; ii<_annotation_count; ii++) {
            if (_annotations[ii].equals(annotation)) return true;
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    public Iterator<Integer> iterateTypeAnnotationIds()
    {
        int[] ids = getTypeAnnotationIds();
        if (ids == null) return (Iterator<Integer>) EMPTY_ITERATOR;
        return new IonTreeReader.IdIterator(ids);
    }

    @SuppressWarnings("unchecked")
    public Iterator<String> iterateTypeAnnotations()
    {
        String[] ids = getTypeAnnotations();
        if (ids == null) return (Iterator<String>) EMPTY_ITERATOR;
        return new IonTreeReader.StringIterator(ids);
    }

    public int getFieldId()
    {
        if (!_value_ready || _current_symtab == null) {
            throw new IllegalStateException();
        }
        int id = 0; // FIXME this violates contract
        if (_field_name != null) {
            id = _current_symtab.findSymbol(_field_name);
        }
        return id;
    }

    public String getFieldName()
    {
        if (!_value_ready) {
            throw new IllegalStateException();
        }
        return _field_name;
    }

    public IonValue getIonValue(IonSystem sys)
    {
        if (isNullValue()) {
            switch (_value_type) {
            case NULL:      return sys.newNull();
            case BOOL:      return sys.newNullBool();
            case INT:       return sys.newNullInt();
            case FLOAT:     return sys.newNullFloat();
            case DECIMAL:   return sys.newNullDecimal();
            case TIMESTAMP: return sys.newNullTimestamp();
            case SYMBOL:    return sys.newNullSymbol();
            case STRING:    return sys.newNullString();
            case CLOB:      return sys.newNullClob();
            case BLOB:      return sys.newNullBlob();
            case LIST:      return sys.newNullList();
            case SEXP:      return sys.newNullSexp();
            case STRUCT:    return sys.newNullString();
            default:
                throw new IonException("unrecognized type encountered");
            }
        }

        switch (_value_type) {
        case NULL:      return sys.newNull();
        case BOOL:      return sys.newBool(booleanValue());
        case INT:       return sys.newInt(longValue());
        case FLOAT:     return sys.newFloat(doubleValue());
        case DECIMAL:   return sys.newDecimal(bigDecimalValue());
        case TIMESTAMP:
            IonTimestamp t = sys.newNullTimestamp();
            Timestamp ti = timestampValue();
            t.setValue(ti);
            return t;
        case SYMBOL:    return sys.newSymbol(stringValue());
        case STRING:    return sys.newString(stringValue());
        case CLOB:
            IonClob clob = sys.newNullClob();
            // FIXME inefficient: both newBytes and setBytes copy the data
            clob.setBytes(newBytes());
            return clob;
        case BLOB:
            IonBlob blob = sys.newNullBlob();
            // FIXME inefficient: both newBytes and setBytes copy the data
            blob.setBytes(newBytes());
            return blob;
        case LIST:
            IonList list = sys.newNullList();
            fillContainerList(sys, list);
            return list;
        case SEXP:
            IonSexp sexp = sys.newNullSexp();
            fillContainerList(sys, sexp);
            return sexp;
        case STRUCT:
            IonStruct struct = sys.newNullStruct();
            fillContainerStruct(sys, struct);
            return struct;
        default:
            throw new IonException("unrecognized type encountered");
        }
    }
    void fillContainerList(IonSystem sys, IonSequence list) {
        this.stepIn();
        while (this.hasNext()) {
            this.next();
            IonValue v = this.getIonValue(sys);
            list.add(v);
        }
        this.stepOut();
    }
    void fillContainerStruct(IonSystem sys, IonStruct struct) {
        this.stepIn();
        while (this.hasNext()) {
            this.next();
            String name = this.getFieldName();
            IonValue v = this.getIonValue(sys);
            struct.add(name, v);
        }
        this.stepOut();
    }

    public boolean isInStruct()
    {
        return _in_struct;
    }

    public boolean isNullValue()
    {
        return _is_null;
    }

    public boolean booleanValue()
    {
        if (_value_type.equals(IonType.BOOL)) {
            int kw = _scanner.keyword(_value_start, _value_end);
            if (kw == IonTextTokenizer.KEYWORD_TRUE) return true;
            if (kw == IonTextTokenizer.KEYWORD_FALSE) return false;
        }
        throw new IllegalStateException("current value is not a boolean");

    }

    public int intValue()
    {
        switch (_value_type) {
            case INT:
                String s;
                int intvalue;
                if (this._value_token == IonTextTokenizer.TOKEN_INT) {
                    s = _scanner.getValueAsString(_value_start, _value_end);
                    intvalue = Integer.parseInt(s);
                }
                else if (this._value_token == IonTextTokenizer.TOKEN_HEX) {
                    int start = _value_start + 2; // skip over the "0x" header
                    int c1 = _scanner.getByte(_value_start);
                    boolean is_negative = false;
                    if (c1 == '-') {
                        start++;
                        is_negative = true;
                    } else if (c1 == '+') {
                        start++;
                    }
                    s = _scanner.getValueAsString(start, _value_end);
                    intvalue = Integer.parseInt(s, 16);
                    if (is_negative) intvalue = -intvalue;
                }
                else {
                    throw new IllegalStateException("unexpected token created an int");
                }
                return intvalue;
            case FLOAT:
                double d = doubleValue();
                return (int)d;
            case DECIMAL:
                BigDecimal bd = bigDecimalValue();
                return bd.intValue();
            default:
                break;
        }
        throw new IllegalStateException("current value is not an ion int, float, or decimal");
    }

    public long longValue()
    {
        switch (_value_type) {
            case INT:
                String s;
                long longvalue;
                if (this._value_token == IonTextTokenizer.TOKEN_INT) {
                    s = _scanner.getValueAsString(_value_start, _value_end);
                    longvalue = Long.parseLong(s);
                }
                else if (this._value_token == IonTextTokenizer.TOKEN_HEX) {
                    int start = _value_start + 2; // skip over the "0x" header
                    int c1 = _scanner.getByte(_value_start);
                    boolean is_negative = false;
                    if (c1 == '-') {
                        start++;
                        is_negative = true;
                    } else if (c1 == '+') {
                        start++;
                    }
                    s = _scanner.getValueAsString(start, _value_end);
                    longvalue = Long.parseLong(s, 16);
                    if (is_negative) longvalue = -longvalue;
                }
                else {
                    throw new IllegalStateException("unexpected token created an int");
                }
                return longvalue;
            case FLOAT:
                double d = doubleValue();
                return (long)d;
            case DECIMAL:
                BigDecimal bd = bigDecimalValue();
                return bd.longValue();
            default:
                break;
        }
        throw new IllegalStateException("current value is not an ion int, float, or decimal");
    }

    public double doubleValue()
    {
        switch (_value_type) {
            case FLOAT:
            	String image = _scanner.getValueAsString(_value_start, _value_end);
            	// check for special values
            	switch (image.charAt(0)) {
            	case 'n': //  nan ??
            		if ("nan".equals(image)) return Double.NaN;
            		break;
            	case '+': // +inf ?
            		if ("+inf".equals(image)) return Double.POSITIVE_INFINITY;
            		break;
            	case 'i': //  inf ?
            		if ("inf".equals(image)) return Double.POSITIVE_INFINITY;
            		break;
            	case '-': // -inf ??
            		if ("-inf".equals(image))  return Double.NEGATIVE_INFINITY;
            		break;
            	}
                double d = Double.parseDouble(image);
                return d;
            case DECIMAL:
                BigDecimal bd = bigDecimalValue();
                return bd.doubleValue();
            default:
                break;
        }
        throw new IllegalStateException("current value is not an ion float or decimal");
    }

    public BigDecimal bigDecimalValue()
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

    public Timestamp timestampValue()
    {
        if (!_value_type.equals(IonType.TIMESTAMP)) {
            throw new IllegalStateException("current value is not a timestamp");
        }
        String image = _scanner.getValueAsString(_value_start, _value_end);
        Timestamp ti = timeinfo.parse(image);

        // FIXME broken for null.timestamp?!?

        return ti;
    }

    public Date dateValue()
    {
        return timestampValue().dateValue();
    }

    public String stringValue()
    {
        switch (_value_type) {
            case STRING:
            case SYMBOL:
                break;
            default:
                throw new IllegalStateException("current value is not a symbol or string");
        }

        if (this._is_null) {
            return null;
        }

        String value;
        if (_extended_value_count > 0) {
            int pos = _scanner.startValueAsString();
            for (int ii=0; ii<_extended_value_count; ii++) {
                int start = this._extended_value_start[ii];
                int end   = this._extended_value_end[ii];
                _scanner.continueValueAsString(start, end);
            }
            value = _scanner.closeValueAsString(pos);
        }
        else {
            value = _scanner.getValueAsString(_value_start, _value_end);
        }
        
        // for symbols we need to populate the symbol table and get a sid assigned
        if (_value_type.equals(IonType.SYMBOL)) {
            assert ! this._is_null;

            // we have to "wash" the value through a symbol table to address
            // cases like $007 TODO is this even well-defined?
            SymbolTable syms = this.getSymbolTable();
            int sid = syms.findSymbol(value);
            if (sid <= 0) {
                if (syms.isSystemTable()) {
                    _current_symtab = new UnifiedSymbolTable(syms);
                    syms = _current_symtab;
                }
                assert syms.isLocalTable();
                sid = syms.addSymbol(value);
            }
            value = syms.findSymbol(sid);
        }
        return value;
    }

    public int getSymbolId()
    {
        if (this._current_symtab == null) {
            throw new IllegalStateException();
        }
        switch (_value_type) {
            case SYMBOL:
                String value = _scanner.getValueAsString(_value_start, _value_end);
                int sid = _current_symtab.findSymbol(value);
                return sid;
            default:
                break;
        }
        throw new IllegalStateException("current value is not a symbol");
    }

    public InputStream getByteStream() {
        InputStream valuestream;
        if (_extended_value_count > 0 || _value_end < 0 || _value_start < 0) {
            valuestream = new ValueByteStream(_scanner._r, _extended_value_count, _extended_value_start, _extended_value_end);
        }
        else {
            valuestream = new ValueByteStream(_scanner._r, _value_start, _value_end);
        }

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

    public int byteSize()
    {
        InputStream bytestream = this.getByteStream();
        try {
            return getLobByteLength(bytestream);
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public byte[] newBytes()
    {
        InputStream bytestream = this.getByteStream();
        byte[] bytes = null;
        int c, len, pos = 0;

        try {
            len = getLobByteLength(bytestream);
            bytes = new  byte[len];

            bytestream = this.getByteStream();
            // TODO: implement reset() on the underlying Lob streams: bytestream.reset();

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
            this.pos = this.start;
        }
        ValueByteStream(IonTextBufferedStream in, int start, int end)
        {
            this.in = in;
            this.count = 0;
            this.starts = null;
            this.ends = null;
            this.array_pos = 0;
            this.start = start;
            this.end = end;
            this.pos = start;
        }
        @Override
        public void reset() {
            if (count > 0) {
                this.array_pos = 0;
                this.start = this.starts[this.array_pos];
                this.end = this.ends[this.array_pos];
            }
            else {
                // nothing to do in the no-array of positions case
            }
            this.pos = this.start;
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
                this.pos = this.start;
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
        static final int NO_LOOKAHEAD = Integer.MIN_VALUE;
        InputStream _in;
        int         _lookahead;
        ValueEscapedCharInputStream(InputStream in) {
            _in = in;
            _lookahead = NO_LOOKAHEAD;
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
            int c2, c;
            if (_lookahead == NO_LOOKAHEAD) {
                c = _in.read();
            }
            else {
                c = _lookahead;
                _lookahead = NO_LOOKAHEAD;
            }

            if (c == '\r') {
                c2 = _in.read();
                if (c2 != '\n') {
                    _lookahead = c2;
                }
                c = '\n';
            }
            else if (c == '\n') {
                c2 = _in.read();
                if (c2 != '\r') {
                    _lookahead = c2;
                }
                c = '\n';
            }
            else if (c == '\\') {
                c2 = _in.read();
                switch (c2) {
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
                case '\r':
                    c = _in.read();
                    if (c == '\n') {
                        c = read();
                    }
                    break;
                case '\n':
                    c = _in.read();
                    if (c == '\r') {
                        c = read();
                    }
                    break;
                default:
                    throw new IonParsingException("invalid escape sequence \"\\"
                                           + (char) c2 + "\" [" + c2 + "]");
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
    	int token = _scanner.lookahead(0);
        while ((  token == IonTextTokenizer.TOKEN_SYMBOL_BASIC
               || token == IonTextTokenizer.TOKEN_SYMBOL_QUOTED
               )
              && _scanner.lookahead(1) == IonTextTokenizer.TOKEN_DOUBLE_COLON
        ) {
            int kw = _scanner.keyword(_scanner.getStart(), _scanner.getEnd());
            switch (kw) {
            case IonTextTokenizer.KEYWORD_FALSE:
            case IonTextTokenizer.KEYWORD_TRUE:
            case IonTextTokenizer.KEYWORD_NULL:
            case IonTextTokenizer.KEYWORD_INF:
            case IonTextTokenizer.KEYWORD_NAN:
            	if (token != IonTextTokenizer.TOKEN_SYMBOL_QUOTED) {
            		// keywords are ok if they're quoted
                    String reason =
                        "Cannot use unquoted keyword " +
                        _scanner.getValueAsString() + " as annotation";
                    error(reason);
            	}
                break;
            default:
                break;
            }
            String name = _scanner.consumeTokenAsString();

            consumeToken(IonTextTokenizer.TOKEN_DOUBLE_COLON);
            appendAnnotation(name);
            token = _scanner.lookahead(0);
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
    void makeNullValue(IonType t) {
        _is_null = true;
        _value_ready = true;
        _value_type = t;
    }
    void makeValue(IonType t, int token) {
        _extended_value_count = 0;
        _value_token = token;
        _value_type = t;
        _value_start = _scanner.getStart();
        _value_end   = _scanner.getEnd();
        if (t.equals(IonType.SYMBOL) && _value_start >= _value_end) {
            throw new IonParsingException("symbols have to have some content" + this._scanner.input_position());
        }
        _is_null = false;
        _value_ready = true;
        _scanner.consumeToken();
    }
    void openValue(IonType t, int token) {
        _extended_value_count = 0;
        _value_token = token;
        _value_type = t;
        _is_null = false;
        _value_start = -1;
        _value_end = -2;
        appendValue(token);
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
    void appendValue(int token) {
        int start = _scanner.getStart();
        int end = _scanner.getEnd();

        // see: IonTextTokenizer.java
        //public static final int TOKEN_STRING1 = string in double quotes (") some > 255
        //public static final int TOKEN_STRING2 = part of a string in triple quotes (''') some > 255
        //public static final int TOKEN_STRING3 = string in double quotes (") all <= 255
        //public static final int TOKEN_STRING4 = part of a string in triple quotes (''') all chars <= 255
        switch (token) {
        case IonTextTokenizer.TOKEN_STRING_UTF8:
            assert (_value_token == IonTextTokenizer.TOKEN_STRING_UTF8 || _value_token == IonTextTokenizer.TOKEN_STRING_CLOB);
            _value_token = IonTextTokenizer.TOKEN_STRING_UTF8;
            break;
        case IonTextTokenizer.TOKEN_STRING_UTF8_LONG:
            assert (_value_token == IonTextTokenizer.TOKEN_STRING_UTF8_LONG || _value_token == IonTextTokenizer.TOKEN_STRING_CLOB_LONG);
            _value_token = IonTextTokenizer.TOKEN_STRING_UTF8_LONG;
            break;
        default:
            break;
        }
        appendValue(start, end);
        _scanner.consumeToken();
    }
    void appendValue(int start, int end) {
        if (_extended_value_count >= _extended_value_start.length) {
            grow_extended_values();
        }
        _extended_value_start[_extended_value_count] = start;
        _extended_value_end[_extended_value_count]   = end;
        _extended_value_count++;
    }

    // FIXME this is probably not appropriate for syntax errors
    // use version below that takes a more explicit description.
    void consumeToken(int token) {
        int current_token = _scanner.lookahead(0);
        if (current_token != token) {
            throw new IonParsingException("token mismatch consuming token (internal error)" + this._scanner.input_position());
        }
        _scanner.consumeToken();
    }

    void consumeToken(int token, String description) {
        int current_token = _scanner.lookahead(0);
        if (current_token != token) {
            String message =
                "Expected " + description + this._scanner.input_position();
            throw new IonParsingException(message);
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
        abstract State transition_method(IonTextReader parser);
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
        State transition_method(IonTextReader parser) {
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
        State transition_method(IonTextReader parser) {
            parser._eof = true;
            return State_done;
        }
    }
    static class Lookahead_read_datagram extends Matches {
        @Override
        State transition_method(IonTextReader parser) {
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
            match_token = IonTextTokenizer.TOKEN_SYMBOL_BASIC;
        }
        @Override
        State transition_method(IonTextReader parser) {
            parser.clearAnnotationList();
            if (parser._scanner.lookahead(1) == IonTextTokenizer.TOKEN_DOUBLE_COLON) {
                parser.readAnnotations();
            }
            return State_read_plain_value;
        }
    }
    static class Lookahead_read_annotated_value_identifier_literal extends Matches {
        Lookahead_read_annotated_value_identifier_literal () {
            match_token = IonTextTokenizer.TOKEN_SYMBOL_QUOTED;
        }
        @Override
        State transition_method(IonTextReader parser) {
            parser.clearAnnotationList();
            if (parser._scanner.lookahead(1) == IonTextTokenizer.TOKEN_DOUBLE_COLON) {
                parser.readAnnotations();
            }
            return State_read_plain_value;
        }
    }
    static class Lookahead_read_annotated_value extends Matches {
        @Override
        State transition_method(IonTextReader parser) {
            return State_read_plain_value;
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
        State transition_method(IonTextReader parser) {
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
        State transition_method(IonTextReader parser) {
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
        State transition_method(IonTextReader parser) {
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
        State transition_method(IonTextReader parser) {
            parser.makeValue(IonType.INT, IonTextTokenizer.TOKEN_INT);
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_plain_value_hex extends Matches {
        Lookahead_read_plain_value_hex() {
            match_token = IonTextTokenizer.TOKEN_HEX;
        }
        @Override
        State transition_method(IonTextReader parser) {
            parser.makeValue(IonType.INT, IonTextTokenizer.TOKEN_HEX);
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_plain_value_float extends Matches {
        Lookahead_read_plain_value_float() {
            match_token = IonTextTokenizer.TOKEN_FLOAT;
        }
        @Override
        State transition_method(IonTextReader parser) {
            parser.makeValue(IonType.FLOAT, IonTextTokenizer.TOKEN_FLOAT);
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_plain_value_decimal extends Matches {
        Lookahead_read_plain_value_decimal() {
            match_token = IonTextTokenizer.TOKEN_DECIMAL;
        }
        @Override
        State transition_method(IonTextReader parser) {
            parser.makeValue(IonType.DECIMAL, IonTextTokenizer.TOKEN_DECIMAL);
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_plain_value_timestamp extends Matches {
        Lookahead_read_plain_value_timestamp() {
            match_token = IonTextTokenizer.TOKEN_TIMESTAMP;
        }
        @Override
        State transition_method(IonTextReader parser) {
            parser.makeValue(IonType.TIMESTAMP, IonTextTokenizer.TOKEN_TIMESTAMP);
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_plain_value_identifier extends Matches {
        Lookahead_read_plain_value_identifier() {
            match_token = IonTextTokenizer.TOKEN_SYMBOL_BASIC;
        }
        @Override
        State transition_method(IonTextReader parser) {
            int start = parser._scanner.getStart();
            int end = parser._scanner.getEnd();
            int keyword = parser._scanner.keyword(start, end);
            switch (keyword) {
            case IonTextTokenizer.KEYWORD_TRUE:
            case IonTextTokenizer.KEYWORD_FALSE:
                parser.makeValue(IonType.BOOL, IonTextTokenizer.TOKEN_SYMBOL_BASIC);
                break;
            case IonTextTokenizer.KEYWORD_INF:
            case IonTextTokenizer.KEYWORD_NAN:
            	parser.makeValue(IonType.FLOAT, IonTextTokenizer.TOKEN_SYMBOL_BASIC); 
            	break;
            case IonTextTokenizer.KEYWORD_NULL:
                parser._scanner.consumeToken();
                return State_read_null;
            default:
                parser.makeValue(IonType.SYMBOL, IonTextTokenizer.TOKEN_SYMBOL_BASIC);
            	break;
            }
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_plain_value_identifier_literal extends Matches {
        Lookahead_read_plain_value_identifier_literal() {
            match_token = IonTextTokenizer.TOKEN_SYMBOL_QUOTED;
        }
        @Override
        State transition_method(IonTextReader parser) {
        	String s = parser._scanner.getValueAsString();
            if (s.length() < 1) {
            	throw new IonException("symbols must not be empty");
            }
            parser.makeValue(IonType.SYMBOL, IonTextTokenizer.TOKEN_SYMBOL_QUOTED);
            return parser.pop_parser_state();
        }
    }

    static class Lookahead_read_plain_value_sexp_identifier_extended extends Matches {
    	Lookahead_read_plain_value_sexp_identifier_extended() {
            match_token = IonTextTokenizer.TOKEN_SYMBOL_OPERATOR;
        }
        @Override
        State transition_method(IonTextReader parser) {
        	// +inf and -inf are extended identifiers (SYMBOL3's)
        	String s = parser._scanner.getValueAsString();
        	IonType value_type = null;
        	switch (s.length()) {
        	case 0:
        		throw new IonException("symbols must not be empty");
        	case 4:
        		if ("+inf".equals(s)) {
        			value_type = IonType.FLOAT;
        		}
        		else if ("-inf".equals(s)) {
        			value_type = IonType.FLOAT;
        		}
        		break;
        	case 3:
        		if ("nan".equals(s)) {
        			value_type = IonType.FLOAT;
        		}
        		else if ("inf".equals(s)) {
        			value_type = IonType.FLOAT;
        		}
        		break;
    		default:
    			break;
        	}
        	if (value_type == null) parser.error(); 
        	parser.makeValue(value_type, IonTextTokenizer.TOKEN_SYMBOL_OPERATOR);
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_plain_value_dot_as_identifier_extended extends Matches {
        Lookahead_read_plain_value_dot_as_identifier_extended() {
            match_token = IonTextTokenizer.TOKEN_DOT;
        }
        @Override
        State transition_method(IonTextReader parser) {
            int start;
            int lookahead_char = parser._scanner.peek_char();
            if (IonTextUtils.isOperatorPart(lookahead_char)) {
                start = parser._scanner.getStart();
                parser.consumeToken(IonTextTokenizer.TOKEN_DOT);
                int token = parser._scanner.lookahead(0);
                if (token != IonTextTokenizer.TOKEN_SYMBOL_OPERATOR) {
                    parser.error();
                }
                parser._scanner.setNextStart(start); // back up and grab the '.' that started all this
                parser.makeValue(IonType.SYMBOL, IonTextTokenizer.TOKEN_DOT);
            }
            else {
                parser.makeValue(IonType.SYMBOL, IonTextTokenizer.TOKEN_SYMBOL_OPERATOR);
            }
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_plain_value_comma_as_identifier_extended extends Matches {
        Lookahead_read_plain_value_comma_as_identifier_extended() {
            match_token = IonTextTokenizer.TOKEN_COMMA;
        }
        @Override
        State transition_method(IonTextReader parser) {
        	
            parser.makeValue(IonType.SYMBOL, IonTextTokenizer.TOKEN_COMMA);
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_plain_value_string_literal extends Matches {
        Lookahead_read_plain_value_string_literal() {
            match_token = IonTextTokenizer.TOKEN_STRING_UTF8;
        }
        @Override
        State transition_method(IonTextReader parser) {
            parser.makeValue(IonType.STRING, IonTextTokenizer.TOKEN_STRING_UTF8);
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_plain_value_string_literal2 extends Lookahead_read_plain_value_string_literal {
        Lookahead_read_plain_value_string_literal2() {
            match_token = IonTextTokenizer.TOKEN_STRING_CLOB;
        }
    }
    static class Lookahead_read_plain_value_string_literal_extended extends Matches {
        Lookahead_read_plain_value_string_literal_extended() {
            match_token = IonTextTokenizer.TOKEN_STRING_UTF8_LONG;
        }
        @Override
        State transition_method(IonTextReader parser) {
            parser.openValue(IonType.STRING, match_token);
            int token;
            for (;;) {
                token = parser._scanner.lookahead(0);
                if (token != IonTextTokenizer.TOKEN_STRING_UTF8_LONG
                 && token != IonTextTokenizer.TOKEN_STRING_CLOB_LONG
                ) {
                    break;
                }
                parser.appendValue(token);
            }
            parser.closeValue(IonType.STRING);
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_plain_value_string_literal_extended2 extends Lookahead_read_plain_value_string_literal_extended {
        Lookahead_read_plain_value_string_literal_extended2() {
            match_token = IonTextTokenizer.TOKEN_STRING_CLOB_LONG;
        }
    }
    static class Lookahead_read_plain_value_lob extends Matches {
        Lookahead_read_plain_value_lob() {
            match_token = IonTextTokenizer.TOKEN_OPEN_DOUBLE_BRACE;
        }
        @Override
        State transition_method(IonTextReader parser) {
            parser.consumeToken(IonTextTokenizer.TOKEN_OPEN_DOUBLE_BRACE);
            int lookahead_char = parser._scanner.lob_lookahead();

            if (lookahead_char == '"') {
                if (parser._scanner.lookahead(0) != IonTextTokenizer.TOKEN_STRING_CLOB) {
                    parser.error();
                }
                parser.makeValue(IonType.CLOB, IonTextTokenizer.TOKEN_STRING_CLOB);
            }
            else if (lookahead_char == '\'') {
                int token = parser._scanner.lookahead(0);
                if (token != IonTextTokenizer.TOKEN_STRING_CLOB_LONG) {
                    parser.error();
                }
                parser.openValue(IonType.CLOB, token);
                for (;;) {
                    token = parser._scanner.lookahead(0);
                    if (token != IonTextTokenizer.TOKEN_STRING_CLOB_LONG) break;
                    parser.appendValue(token);
                }
                parser.closeValue(IonType.CLOB);
            }
            else if (lookahead_char == '}') {
                // put a "fake" start and end in to simulate a 0 length token
                parser._scanner.setNextStart(0);
                parser._scanner.setNextEnd(0);
                parser._scanner.enqueueToken(IonTextTokenizer.TOKEN_BLOB);
                parser.makeValue(IonType.BLOB, IonTextTokenizer.TOKEN_BLOB);
            }
            else {
                parser._scanner.scanBase64Value(parser);
                parser._scanner.enqueueToken(IonTextTokenizer.TOKEN_BLOB);
                parser.makeValue(IonType.BLOB, IonTextTokenizer.TOKEN_BLOB);
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
        new Lookahead_read_plain_value_hex(),
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
        new Lookahead_read_plain_value_hex(),
        new Lookahead_read_plain_value_float(),
        new Lookahead_read_plain_value_decimal(),
        new Lookahead_read_plain_value_timestamp(),
        new Lookahead_read_plain_value_identifier(),
        new Lookahead_read_plain_value_identifier_literal(),
        new Lookahead_read_plain_value_sexp_identifier_extended(),
        new Lookahead_read_plain_value_dot_as_identifier_extended(),
        // we don't allow comma's here: new Lookahead_read_plain_value_comma_as_identifier_extended(),
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
        State transition_method(IonTextReader parser) {
            parser.consumeToken(IonTextTokenizer.TOKEN_DOT);
            if (parser._scanner.lookahead(0) != IonTextTokenizer.TOKEN_SYMBOL_BASIC) {
                parser.error();
            }
            int start = parser._scanner.getStart();
            int end   = parser._scanner.getEnd();
            int keyword = parser._scanner.keyword(start, end);
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
            parser.consumeToken(IonTextTokenizer.TOKEN_SYMBOL_BASIC);
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_null extends Matches {
        @Override
        State transition_method(IonTextReader parser) {
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
            match_token = IonTextTokenizer.TOKEN_SYMBOL_BASIC;
        }
        @Override
        State transition_method(IonTextReader parser) {
            parser.clearAnnotationList();
            parser.clearFieldname();
            assert parser._scanner.lookahead(0) == IonTextTokenizer.TOKEN_SYMBOL_BASIC;
            if (parser._scanner.lookahead(1) == IonTextTokenizer.TOKEN_DOUBLE_COLON )
            {
                parser.readAnnotations();
            }

            parser.push_parser_state( State_read_sexp_value );
            parser.push_parser_state(State_close_value);
            return State_read_plain_value_sexp;
        }
    }
    static class Lookahead_read_sexp_value_identifier_literal extends Matches {
        Lookahead_read_sexp_value_identifier_literal() {
            match_token = IonTextTokenizer.TOKEN_SYMBOL_QUOTED;
        }
        @Override
        State transition_method(IonTextReader parser) {
            parser.clearAnnotationList();
            parser.clearFieldname();
            assert parser._scanner.lookahead(0) == IonTextTokenizer.TOKEN_SYMBOL_QUOTED;
            if ( parser._scanner.lookahead(1) == IonTextTokenizer.TOKEN_DOUBLE_COLON ) {
                parser.readAnnotations();
            }

            parser.push_parser_state( State_read_sexp_value );
            parser.push_parser_state(State_close_value);
            return State_read_plain_value_sexp;
        }

    }
    static class Lookahead_read_sexp_value_extended_identifier extends Matches {
        Lookahead_read_sexp_value_extended_identifier() {
            match_token = IonTextTokenizer.TOKEN_SYMBOL_OPERATOR;
        }
        @Override
        State transition_method(IonTextReader parser) {
            parser.clearAnnotationList();
            parser.clearFieldname();
            parser.makeValue(IonType.SYMBOL, IonTextTokenizer.TOKEN_SYMBOL_OPERATOR);
            return State_read_sexp_value;
        }
    }
    static class Lookahead_read_sexp_value_close_paren extends Matches {
        Lookahead_read_sexp_value_close_paren() {
            match_token = IonTextTokenizer.TOKEN_CLOSE_PAREN;
        }
        @Override
        State transition_method(IonTextReader parser){
            parser.consumeToken(IonTextTokenizer.TOKEN_CLOSE_PAREN);
            parser.closeCollection(IonType.SEXP);
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_sexp_value extends Matches {
        @Override
        State transition_method(IonTextReader parser) {
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
        State transition_method(IonTextReader parser) {
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
        State transition_method(IonTextReader parser){
            parser.consumeToken(IonTextTokenizer.TOKEN_CLOSE_SQUARE);
            parser.closeCollection(IonType.LIST);
            return parser.pop_parser_state();
        }
    }
    static class Lookahead_read_list_value_identifier extends Matches {
        Lookahead_read_list_value_identifier() {
            match_token = IonTextTokenizer.TOKEN_SYMBOL_BASIC;
        }
        @Override
        State transition_method(IonTextReader parser){
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
            match_token = IonTextTokenizer.TOKEN_SYMBOL_QUOTED;
        }
        @Override
        State transition_method(IonTextReader parser){
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
        State transition_method(IonTextReader parser) {
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
        State transition_method(IonTextReader parser){
            parser.consumeToken( IonTextTokenizer.TOKEN_COMMA );
            return State_read_list_value;
        }
    }
    static class Lookahead_read_list_comma_close_square extends Matches {
        Lookahead_read_list_comma_close_square() {
            match_token = IonTextTokenizer.TOKEN_CLOSE_SQUARE;
        }
        @Override
        State transition_method(IonTextReader parser){
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
            match_token = IonTextTokenizer.TOKEN_SYMBOL_BASIC;
        }
        @Override
        State transition_method(IonTextReader parser) {
            int start = parser._scanner.getStart();
            int end   = parser._scanner.getEnd();
            int kw = parser._scanner.keyword(start, end);
            switch (kw) {
            case IonTextTokenizer.KEYWORD_TRUE:
            case IonTextTokenizer.KEYWORD_FALSE:
            case IonTextTokenizer.KEYWORD_NULL:
            case IonTextTokenizer.KEYWORD_INF:
            case IonTextTokenizer.KEYWORD_NAN:
                parser.error();
            default:
                break;
            }
            String fieldname = parser._scanner.consumeTokenAsString();
            parser.setFieldname(fieldname);
            String description =
                "colon (:) after field name " + printQuotedSymbol(fieldname);
            parser.consumeToken( IonTextTokenizer.TOKEN_COLON, description );
            return State_read_struct_member;
        }
    }
    static class Lookahead_read_struct_value_identifier_literal extends Matches {
        Lookahead_read_struct_value_identifier_literal() {
            match_token = IonTextTokenizer.TOKEN_SYMBOL_QUOTED;
        }
        @Override
        State transition_method(IonTextReader parser) {
            String fieldname = parser._scanner.consumeTokenAsString();
            parser.setFieldname(fieldname);
            String description =
                "colon (:) after field name " + printQuotedSymbol(fieldname);
            parser.consumeToken( IonTextTokenizer.TOKEN_COLON, description );
            return State_read_struct_member;
        }
    }
    static class Lookahead_read_struct_value_string_literal extends Lookahead_read_struct_value_identifier_literal {
        Lookahead_read_struct_value_string_literal() {
            match_token = IonTextTokenizer.TOKEN_STRING_UTF8;
        }
    }
    static class Lookahead_read_struct_value_string_literal2 extends Lookahead_read_struct_value_identifier_literal {
        Lookahead_read_struct_value_string_literal2() {
            match_token = IonTextTokenizer.TOKEN_STRING_CLOB;
        }
    }
    static class Lookahead_read_struct_value_string_literal_extended extends Matches {
        Lookahead_read_struct_value_string_literal_extended() {
            match_token = IonTextTokenizer.TOKEN_STRING_UTF8_LONG;
        }
        @Override
        State transition_method(IonTextReader parser){
            String fieldname = parser._scanner.consumeTokenAsString();
            // this isn't an especially pretty way to do this, but if someone is
            // really creating a field name by concatinating long strings - geez,
            // what do they expect?
            int token;
            for (;;) {
                token = parser._scanner.lookahead(0);
                if (token != IonTextTokenizer.TOKEN_STRING_UTF8_LONG
                 && token != IonTextTokenizer.TOKEN_STRING_CLOB_LONG
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
            match_token = IonTextTokenizer.TOKEN_STRING_CLOB_LONG;
        }
    }
    static class Lookahead_read_struct_value_close_brace extends Matches {
        Lookahead_read_struct_value_close_brace() {
            match_token = IonTextTokenizer.TOKEN_CLOSE_BRACE;
        }
        @Override
        State transition_method(IonTextReader parser){
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
        State transition_method(IonTextReader parser){
            parser.consumeToken( IonTextTokenizer.TOKEN_COMMA );
            return State_read_struct_value;
        }
    }
    static class Lookahead_read_struct_comma_close_brace extends Matches {
        Lookahead_read_struct_comma_close_brace() {
            match_token = IonTextTokenizer.TOKEN_CLOSE_BRACE;
        }
        @Override
        State transition_method(IonTextReader parser){
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
            match_token = IonTextTokenizer.TOKEN_SYMBOL_BASIC;
        }
        @Override
        State transition_method(IonTextReader parser){
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
            match_token = IonTextTokenizer.TOKEN_SYMBOL_QUOTED;
        }
        @Override
        State transition_method(IonTextReader parser){
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
        State transition_method(IonTextReader parser) {
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
