/*
 * Copyright 2009-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion.impl;

import static software.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static software.amazon.ion.impl.IonTokenConstsX.TOKEN_CLOSE_BRACE;
import static software.amazon.ion.impl.IonTokenConstsX.TOKEN_CLOSE_PAREN;
import static software.amazon.ion.impl.IonTokenConstsX.TOKEN_CLOSE_SQUARE;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Iterator;
import software.amazon.ion.IonException;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonType;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.UnknownSymbolException;
import software.amazon.ion.impl.PrivateScalarConversions.AS_TYPE;
import software.amazon.ion.impl.PrivateScalarConversions.ValueVariant;
import software.amazon.ion.impl.UnifiedSavePointManagerX.SavePoint;

/**
 * Reader implementation that reads the token stream and validates
 * the Ion grammar.  This does not care about system values.  It
 * does not materialize values or convert them.  It does mark values
 * in the UnifiedInputStream if they might be field names or annotations
 * since it does populate these properties directly. Otherwise it
 * accepts the TextRawToken's assessment of the type of the next
 * token, which is based on as few characters as possible, typically
 * 1 but generally less than 5.
 *
 * This is called by the {@link IonReaderTextSystemX}, which in turn is most
 * often called by the {@link IonReaderTextUserX}.  One of these two (system
 * reader or user reader) should be invoked by the user for reading text Ion
 * data.  This class is not intended for general use.
 *
 * This reader scan skip values and in doing so it does not
 * materialize the contents and it does not validate the contents.
 * TODO amznlabs/ion-java#7 We may want to make validation on skip optional.
 *
 * This manages the value buffer (_v ValueVariant) and the lob
 * content (_lob_*) which is cached in some cases.  It's main
 * job however is recognizing the correct order of the input
 * tokens.  This is done in parse_to_next_value (called by hasNext).
 *
 * The current state is represented by an int (whose value should
 * be one of the values of the STATE_* constants).  The legal
 * transitions are stored in TransitionActions and TransitionActions2.
 * The first (TransitionActions) is a two dimensional array whose
 * dimensions are state and input token. The value stored is an
 * int that represents the action to be taken (ACTION_*).  The
 * second copy of this data (TransitionActions2) is a one dimensional
 * array built from the first and manually dereferenced in the
 * parse_to_next_value method.  This turns out to be a significant
 * performance gain (<sigh>).  Logically these are the same.
 *
 */
abstract class IonReaderTextRawX
    implements IonReader
{
    public abstract BigInteger bigIntegerValue();

//              static final boolean _object_parser           = false;
              static final boolean _debug                   = false;
    private   static final int     DEFAULT_STACK_DEPTH      = 10;
    protected static final int     UNKNOWN_SIZE             = -1;
    private   static final int     DEFAULT_ANNOTATION_COUNT =  5;

    static final int STATE_BEFORE_ANNOTATION_DATAGRAM     =  0;
    static final int STATE_BEFORE_ANNOTATION_CONTAINED    =  1;
    static final int STATE_BEFORE_ANNOTATION_SEXP         =  2;
    static final int STATE_BEFORE_FIELD_NAME              =  3;
    static final int STATE_BEFORE_VALUE_CONTENT           =  4;
    static final int STATE_BEFORE_VALUE_CONTENT_SEXP      =  5;
    static final int STATE_IN_LONG_STRING                 =  6;
    static final int STATE_IN_CLOB_DOUBLE_QUOTED_CONTENT  =  7;
    static final int STATE_IN_CLOB_TRIPLE_QUOTED_CONTENT  =  8;
    static final int STATE_IN_BLOB_CONTENT                =  9;
    static final int STATE_AFTER_VALUE_CONTENTS           = 10;
    static final int STATE_EOF                            = 11;
    static final int STATE_MAX                            = 11;
    private final String get_state_name(int state) {
        switch(state) {
        case STATE_BEFORE_ANNOTATION_DATAGRAM:    return "STATE_BEFORE_ANNOTATION_DATAGRAM";
        case STATE_BEFORE_ANNOTATION_CONTAINED:   return "STATE_BEFORE_ANNOTATION_CONTAINED";
        case STATE_BEFORE_ANNOTATION_SEXP:        return "STATE_BEFORE_ANNOTATION_SEXP";
        case STATE_BEFORE_FIELD_NAME:             return "STATE_BEFORE_FIELD_NAME";
        case STATE_BEFORE_VALUE_CONTENT:          return "STATE_BEFORE_VALUE_CONTENT";
        case STATE_BEFORE_VALUE_CONTENT_SEXP:     return "STATE_BEFORE_VALUE_CONTENT_SEXP";
        case STATE_IN_LONG_STRING:                return "STATE_IN_LONG_STRING";
        case STATE_IN_CLOB_DOUBLE_QUOTED_CONTENT: return "STATE_IN_CLOB_DOUBLE_QUOTED_CONTENT";
        case STATE_IN_CLOB_TRIPLE_QUOTED_CONTENT: return "STATE_IN_CLOB_TRIPLE_QUOTED_CONTENT";
        case STATE_IN_BLOB_CONTENT:               return "STATE_IN_BLOB_CONTENT";
        case STATE_AFTER_VALUE_CONTENTS:          return "STATE_AFTER_VALUE_CONTENTS";
        case STATE_EOF:                           return "STATE_EOF";
        default:                                  return "<invalid state: "+Integer.toString(state)+">";
        }
    }

    static final int ACTION_NOT_DEFINED          =  0;
    static final int ACTION_LOAD_FIELD_NAME      =  1;
    static final int ACTION_LOAD_ANNOTATION      =  2;
    static final int ACTION_START_STRUCT         =  3;
    static final int ACTION_START_LIST           =  4;
    static final int ACTION_START_SEXP           =  5;
    static final int ACTION_START_LOB            =  6;
    static final int ACTION_LOAD_SCALAR          =  8;
    static final int ACTION_PLUS_INF             =  9;
    static final int ACTION_MINUS_INF            = 10;
    static final int ACTION_EAT_COMMA            = 11; // if this is unnecessary (because load_scalar handle it) we don't need "after_value"
    static final int ACTION_FINISH_CONTAINER     = 12;
    static final int ACTION_FINISH_LOB           = 13;
    static final int ACTION_FINISH_DATAGRAM      = 14;
    static final int ACTION_EOF                  = 15;
    static final int ACTION_count                = 16;
    @SuppressWarnings("unused")
    private final String get_action_name(int action) {
        switch(action) {
        case ACTION_NOT_DEFINED:        return "ACTION_DO_NOTHING";
        case ACTION_LOAD_FIELD_NAME:    return "ACTION_LOAD_FIELD_NAME";
        case ACTION_LOAD_ANNOTATION:    return "ACTION_LOAD_ANNOTATION";
        case ACTION_START_STRUCT:       return "ACTION_START_STRUCT";
        case ACTION_START_LIST:         return "ACTION_START_LIST";
        case ACTION_START_SEXP:         return "ACTION_START_SEXP";
        case ACTION_START_LOB:          return "ACTION_START_LOB";
        case ACTION_LOAD_SCALAR:        return "ACTION_LOAD_SCALAR";
        case ACTION_PLUS_INF:           return "ACTION_PLUS_INF";
        case ACTION_MINUS_INF:          return "ACTION_MINUS_INF";
        case ACTION_EAT_COMMA:          return "ACTION_EAT_COMMA";
        case ACTION_FINISH_CONTAINER:   return "ACTION_FINISH_CONTAINER";
        case ACTION_FINISH_LOB:         return "ACTION_FINISH_LOB";
        case ACTION_FINISH_DATAGRAM:    return "ACTION_FINISH_DATAGRAM";
        case ACTION_EOF:                return "ACTION_EOF";
        default:                        return "<unrecognized action: "+Integer.toString(action)+">";
        }
    }

    static final int[][] TransitionActions = makeTransitionActionArray();
    static final int[][] makeTransitionActionArray()
    {
        int[][] actions = new int[STATE_MAX + 1][IonTokenConstsX.TOKEN_MAX + 1];

        actions[STATE_BEFORE_ANNOTATION_DATAGRAM][IonTokenConstsX.TOKEN_EOF]                = ACTION_FINISH_DATAGRAM;
        actions[STATE_BEFORE_ANNOTATION_DATAGRAM][IonTokenConstsX.TOKEN_UNKNOWN_NUMERIC]    = ACTION_LOAD_SCALAR;
        actions[STATE_BEFORE_ANNOTATION_DATAGRAM][IonTokenConstsX.TOKEN_INT]                = ACTION_LOAD_SCALAR;
        actions[STATE_BEFORE_ANNOTATION_DATAGRAM][IonTokenConstsX.TOKEN_BINARY]             = ACTION_LOAD_SCALAR;
        actions[STATE_BEFORE_ANNOTATION_DATAGRAM][IonTokenConstsX.TOKEN_HEX]                = ACTION_LOAD_SCALAR;
        actions[STATE_BEFORE_ANNOTATION_DATAGRAM][IonTokenConstsX.TOKEN_DECIMAL]            = ACTION_LOAD_SCALAR;
        actions[STATE_BEFORE_ANNOTATION_DATAGRAM][IonTokenConstsX.TOKEN_FLOAT]              = ACTION_LOAD_SCALAR;
        actions[STATE_BEFORE_ANNOTATION_DATAGRAM][IonTokenConstsX.TOKEN_FLOAT_INF]          = ACTION_PLUS_INF;
        actions[STATE_BEFORE_ANNOTATION_DATAGRAM][IonTokenConstsX.TOKEN_FLOAT_MINUS_INF]    = ACTION_MINUS_INF;
        actions[STATE_BEFORE_ANNOTATION_DATAGRAM][IonTokenConstsX.TOKEN_TIMESTAMP]          = ACTION_LOAD_SCALAR;
        actions[STATE_BEFORE_ANNOTATION_DATAGRAM][IonTokenConstsX.TOKEN_STRING_DOUBLE_QUOTE]= ACTION_LOAD_SCALAR;
        actions[STATE_BEFORE_ANNOTATION_DATAGRAM][IonTokenConstsX.TOKEN_STRING_TRIPLE_QUOTE]= ACTION_LOAD_SCALAR;
        actions[STATE_BEFORE_ANNOTATION_DATAGRAM][IonTokenConstsX.TOKEN_SYMBOL_IDENTIFIER]  = ACTION_LOAD_ANNOTATION;
        actions[STATE_BEFORE_ANNOTATION_DATAGRAM][IonTokenConstsX.TOKEN_SYMBOL_QUOTED]      = ACTION_LOAD_ANNOTATION;
        actions[STATE_BEFORE_ANNOTATION_DATAGRAM][IonTokenConstsX.TOKEN_OPEN_PAREN]         = ACTION_START_SEXP;
        actions[STATE_BEFORE_ANNOTATION_DATAGRAM][IonTokenConstsX.TOKEN_OPEN_BRACE]         = ACTION_START_STRUCT;
        actions[STATE_BEFORE_ANNOTATION_DATAGRAM][IonTokenConstsX.TOKEN_OPEN_SQUARE]        = ACTION_START_LIST;
        actions[STATE_BEFORE_ANNOTATION_DATAGRAM][IonTokenConstsX.TOKEN_OPEN_DOUBLE_BRACE]  = ACTION_START_LOB;

        // both before_annotation and after_annotation are essentially the same as
        // BOF (after_annotation can't accept EOF as valid however)
        for (int ii=0; ii<IonTokenConstsX.TOKEN_MAX+1; ii++) {
            actions[STATE_BEFORE_ANNOTATION_CONTAINED][ii] = actions[STATE_BEFORE_ANNOTATION_DATAGRAM][ii];
            actions[STATE_BEFORE_ANNOTATION_SEXP][ii]      = actions[STATE_BEFORE_ANNOTATION_DATAGRAM][ii];
            actions[STATE_BEFORE_VALUE_CONTENT][ii]        = actions[STATE_BEFORE_ANNOTATION_DATAGRAM][ii];
            actions[STATE_BEFORE_VALUE_CONTENT_SEXP][ii]   = actions[STATE_BEFORE_ANNOTATION_DATAGRAM][ii];
        }
        // now patch up the differences between these 4 states handling of tokens vs before_annotation_datagram
        actions[STATE_BEFORE_ANNOTATION_CONTAINED][IonTokenConstsX.TOKEN_EOF]            = 0;
        actions[STATE_BEFORE_ANNOTATION_CONTAINED][IonTokenConstsX.TOKEN_CLOSE_PAREN]    = ACTION_FINISH_CONTAINER;
        actions[STATE_BEFORE_ANNOTATION_CONTAINED][IonTokenConstsX.TOKEN_CLOSE_BRACE]    = ACTION_FINISH_CONTAINER;
        actions[STATE_BEFORE_ANNOTATION_CONTAINED][IonTokenConstsX.TOKEN_CLOSE_SQUARE]   = ACTION_FINISH_CONTAINER;

        actions[STATE_BEFORE_ANNOTATION_SEXP][IonTokenConstsX.TOKEN_EOF]                 = 0;
        actions[STATE_BEFORE_ANNOTATION_SEXP][IonTokenConstsX.TOKEN_SYMBOL_OPERATOR]     = ACTION_LOAD_SCALAR;
        actions[STATE_BEFORE_ANNOTATION_SEXP][IonTokenConstsX.TOKEN_DOT]                 = ACTION_LOAD_SCALAR;
        actions[STATE_BEFORE_ANNOTATION_SEXP][IonTokenConstsX.TOKEN_CLOSE_PAREN]         = ACTION_FINISH_CONTAINER;
        actions[STATE_BEFORE_ANNOTATION_SEXP][IonTokenConstsX.TOKEN_CLOSE_BRACE]         = ACTION_FINISH_CONTAINER;
        actions[STATE_BEFORE_ANNOTATION_SEXP][IonTokenConstsX.TOKEN_CLOSE_SQUARE]        = ACTION_FINISH_CONTAINER;

        actions[STATE_BEFORE_VALUE_CONTENT][IonTokenConstsX.TOKEN_EOF]                   = 0;
        actions[STATE_BEFORE_VALUE_CONTENT][IonTokenConstsX.TOKEN_SYMBOL_IDENTIFIER]     = ACTION_LOAD_SCALAR;
        actions[STATE_BEFORE_VALUE_CONTENT][IonTokenConstsX.TOKEN_SYMBOL_QUOTED]         = ACTION_LOAD_SCALAR;

        actions[STATE_BEFORE_VALUE_CONTENT_SEXP][IonTokenConstsX.TOKEN_EOF]              = 0;
        actions[STATE_BEFORE_VALUE_CONTENT_SEXP][IonTokenConstsX.TOKEN_SYMBOL_IDENTIFIER]= ACTION_LOAD_SCALAR;
        actions[STATE_BEFORE_VALUE_CONTENT_SEXP][IonTokenConstsX.TOKEN_SYMBOL_QUOTED]    = ACTION_LOAD_SCALAR;
        actions[STATE_BEFORE_VALUE_CONTENT_SEXP][IonTokenConstsX.TOKEN_SYMBOL_OPERATOR]  = ACTION_LOAD_SCALAR;

        actions[STATE_BEFORE_FIELD_NAME][IonTokenConstsX.TOKEN_EOF]                      = 0;
        actions[STATE_BEFORE_FIELD_NAME][IonTokenConstsX.TOKEN_SYMBOL_IDENTIFIER]        = ACTION_LOAD_FIELD_NAME;
        actions[STATE_BEFORE_FIELD_NAME][IonTokenConstsX.TOKEN_SYMBOL_QUOTED]            = ACTION_LOAD_FIELD_NAME;
        actions[STATE_BEFORE_FIELD_NAME][IonTokenConstsX.TOKEN_STRING_DOUBLE_QUOTE]      = ACTION_LOAD_FIELD_NAME;
        actions[STATE_BEFORE_FIELD_NAME][IonTokenConstsX.TOKEN_STRING_TRIPLE_QUOTE]      = ACTION_LOAD_FIELD_NAME;
        actions[STATE_BEFORE_FIELD_NAME][IonTokenConstsX.TOKEN_CLOSE_PAREN]              = ACTION_FINISH_CONTAINER;
        actions[STATE_BEFORE_FIELD_NAME][IonTokenConstsX.TOKEN_CLOSE_BRACE]              = ACTION_FINISH_CONTAINER;
        actions[STATE_BEFORE_FIELD_NAME][IonTokenConstsX.TOKEN_CLOSE_SQUARE]             = ACTION_FINISH_CONTAINER;

         // after a value we'll either see a separator (like ',')
         // or a containers closing token. If we're not in a container
         // (i.e. we're at the top level) then this isn't the state we
         // should be in.  We'll be in STATE_BEFORE_ANNOTATION_DATAGRAM
         actions[STATE_AFTER_VALUE_CONTENTS][IonTokenConstsX.TOKEN_COMMA]                = ACTION_EAT_COMMA;
         actions[STATE_AFTER_VALUE_CONTENTS][IonTokenConstsX.TOKEN_CLOSE_PAREN]          = ACTION_FINISH_CONTAINER;
         actions[STATE_AFTER_VALUE_CONTENTS][IonTokenConstsX.TOKEN_CLOSE_BRACE]          = ACTION_FINISH_CONTAINER;
         actions[STATE_AFTER_VALUE_CONTENTS][IonTokenConstsX.TOKEN_CLOSE_SQUARE]         = ACTION_FINISH_CONTAINER;

         // the three "in_<lob>" value states have to be handled
         // specially, they can only scan forward to the end of
         // the content on next, or read content for the user otherwise
         actions[STATE_IN_CLOB_DOUBLE_QUOTED_CONTENT][IonTokenConstsX.TOKEN_CLOSE_BRACE] = ACTION_FINISH_LOB;
         actions[STATE_IN_CLOB_TRIPLE_QUOTED_CONTENT][IonTokenConstsX.TOKEN_CLOSE_BRACE] = ACTION_FINISH_LOB;
         actions[STATE_IN_BLOB_CONTENT][IonTokenConstsX.TOKEN_CLOSE_BRACE]               = ACTION_FINISH_LOB;

         // the eof action exists because finishing an unread value can place the scanner just before
         // the input stream eof and set the current state to eof - in which case we just need to return eof
         for (int ii=0; ii<IonTokenConstsX.TOKEN_MAX+1; ii++) {
             actions[STATE_EOF][ii] =  ACTION_EOF;
         }

         return actions;
    }

    static final int[] TransitionActions2 = makeTransition2ActionArray();
    static int[] makeTransition2ActionArray() {
        int   s, s_count = STATE_MAX + 1;
        int   t, t_count = IonTokenConstsX.TOKEN_MAX + 1;
        int[] a = new int[s_count * t_count];
        for (s = 0; s < s_count; s++) {
            for (t=0; t < t_count; t++) {
                int ii = s * IonTokenConstsX.TOKEN_count + t;
                a[ii] = TransitionActions[s][t];
            }
        }
        return a;
    }

    //
    //  actual class members (preceding values are just parsing
    //  control constants).
    //

    IonReaderTextRawTokensX  _scanner;

    boolean             _eof;
    int                 _state;

    IonType[]           _container_state_stack = new IonType[DEFAULT_STACK_DEPTH];
    int                 _container_state_top;
    boolean             _container_is_struct;           // helper bool's set on push and pop and used
    boolean             _container_prohibits_commas;    // frequently during state transitions actions

    boolean             _has_next_called;
    IonType             _value_type;
    int                 _value_keyword;
    IonType             _null_type;
    String              _field_name;
    int                 _field_name_sid = UNKNOWN_SYMBOL_ID;
    int                 _annotation_count;
    SymbolToken[]    _annotations;

    boolean             _current_value_save_point_loaded;
    SavePoint           _current_value_save_point;
    boolean             _current_value_buffer_loaded;
    StringBuilder       _current_value_buffer;

    ValueVariant        _v = new ValueVariant();

    long                _value_start_offset;
    long                _value_start_line;
    long                _value_start_column;
    IonType             _nesting_parent;

    enum LOB_STATE { EMPTY, READ, FINISHED }
    boolean             _lob_value_set;
    int                 _lob_token;
    long                _lob_value_position;
    LOB_STATE           _lob_loaded;
    byte[]              _lob_bytes;
    int                 _lob_actual_len;


    protected IonReaderTextRawX() {
        super();
        _nesting_parent = null;
    }


    /**
     * @return This implementation always returns null.
     */
    public <T> T asFacet(Class<T> facetType)
    {
        return null;
    }

    //========================================================================

    protected final void init_once() {
        _current_value_buffer = new StringBuilder();
        _annotations = new SymbolToken[DEFAULT_ANNOTATION_COUNT];
    }

    protected final void init(UnifiedInputStreamX iis, IonType parent)
    {
        init(iis, parent, 1, 1);
    }

    protected final void init(UnifiedInputStreamX iis
                             ,IonType parent
                             ,long start_line
                             ,long start_column
    ) {

        assert(parent != null);
        _scanner = new IonReaderTextRawTokensX(iis, start_line, start_column);
        _value_start_line = start_line;
        _value_start_column = start_column;
        _current_value_save_point = iis.savePointAllocate();
        _lob_loaded = LOB_STATE.EMPTY;
        int starting_state = get_state_at_container_start(parent);
        set_state(starting_state);
        _eof = false;
        push_container_state(parent);
    }

    protected final void re_init(UnifiedInputStreamX iis
                                ,IonType parent
                                ,long start_line
                                ,long start_column
    ) {
        _state = 0;
        _container_state_top = 0;
        _container_is_struct = false;
        _container_prohibits_commas = false;
        _has_next_called = false;
        _value_type = null;
        _value_keyword = 0;
        _null_type = null;
        _field_name = null;
        _field_name_sid = UNKNOWN_SYMBOL_ID;
        _annotation_count = 0;
        _current_value_save_point_loaded = false;
        _current_value_buffer_loaded = false;
        _value_start_offset = 0;
        _lob_value_set = false;
        _lob_token = 0;
        _lob_value_position = 0;
        _lob_bytes = null;
        _lob_actual_len = 0;

        init(iis, parent, start_line, start_column);

        _nesting_parent = parent;
        if (IonType.STRUCT.equals(_nesting_parent)) {
            _container_is_struct = true;
        }
    }

    public void close()
        throws IOException
    {
        _scanner.close();
    }

    private final void set_state(int new_state) {
        _state = new_state;
    }
    private final int get_state_int() {
        return _state;
    }
    private final String get_state_name() {
        String name = get_state_name(get_state_int());
        return name;
    }

    protected final void clear_current_value_buffer() {
        if (_current_value_buffer_loaded) {
            _current_value_buffer.setLength(0);
            _current_value_buffer_loaded = false;
        }
        if (_current_value_save_point_loaded) {
            _current_value_save_point.clear();
            _current_value_save_point_loaded = false;
        }
    }

    private final void current_value_is_null(IonType null_type)
    {
        clear_current_value_buffer();
        _value_type = _null_type;
        _v.setValueToNull(null_type);
        _v.setAuthoritativeType(PrivateScalarConversions.AS_TYPE.null_value);
    }

    private final void current_value_is_bool(boolean value)
    {
        clear_current_value_buffer();
        _value_type = IonType.BOOL;
        _v.setValue(value);
        _v.setAuthoritativeType(PrivateScalarConversions.AS_TYPE.boolean_value);
    }

    private final void set_fieldname(SymbolToken sym) {
        String text = sym.getText();
        int sid = sym.getSid();
        if (text != null && text.length() < 1) {
            parse_error("empty strings are not valid field names");
        }
        _field_name = text;
        _field_name_sid = sid;
    }

    private final void clear_fieldname() {
        _field_name = null;
        _field_name_sid = UNKNOWN_SYMBOL_ID;
    }

    private final void append_annotation(SymbolToken sym) {
        // empty text is checked by caller
        int oldlen = _annotations.length;
        if (_annotation_count >= oldlen) {
            int newlen = oldlen * 2;
            SymbolToken[] temp = new SymbolToken[newlen];
            System.arraycopy(_annotations, 0, temp, 0, oldlen);
            _annotations = temp;
        }
        _annotations[_annotation_count++] = sym;
    }

    private final void clear_annotation_list() {
        _annotation_count = 0;
    }

    /**
     * this looks forward to see if there is an upcoming value
     * if there is it returns true.  It may have to clean up
     * any value that's partially complete (for example a
     * collection whose annotation has been read and loaded
     * but the user has chosen not to step into the collection).
     * @return true if more data remains, false on eof
     */
    boolean hasNext()
    {
        boolean has_next = has_next_raw_value();
        return has_next;
    }
    protected final boolean has_next_raw_value() {
        if (!_has_next_called && !_eof) {
            try {
                finish_value(null);
                clear_value();
                parse_to_next_value();
            }
            catch (IOException e) {
                throw new IonException(e);
            }
            _has_next_called = true;
        }
        return (_eof != true);
    }

    /**
     * returns the type of the next value in the stream.
     * it calls hasNext to assure that the value has been properly
     * started, since hasNext prepares a value as a side effect of
     * determining whether or not a value is pending.
     * A NoSuchElementException is thrown if there are not values remaining.
     * Once called if there is a value available it's contents can
     * be accessed through the other public API's (such as getLong()).
     * @return type of the next value, or null if there is none.
     */
    public IonType next()
    {
        if (!hasNext()) {
            return null;
        }
        if (_value_type == null && _scanner.isUnfinishedToken()) {
            try {
                token_contents_load(_scanner.getToken());
            }
            catch (IOException e) {
                throw new IonException(e);
            }
        }
        _has_next_called = false;
        return _value_type;
    }
    private final void finish_and_save_value() throws IOException
    {
        if (!_current_value_save_point_loaded) {
            _scanner.save_point_start(_current_value_save_point);
            finish_value(_current_value_save_point);
            _current_value_save_point_loaded = true;
        }
    }
    private final void finish_value(SavePoint sp) throws IOException
    {
        if (_scanner.isUnfinishedToken()) {
            if (sp != null && _value_type != null) {
                switch (_value_type) {
                case STRUCT:
                case SEXP:
                case LIST:
                    sp = null;
                    break;
                default:
                    break;
                }
            }
            _scanner.finish_token(sp);

            int new_state = get_state_after_value();
            set_state(new_state);
        }
        _has_next_called = false;
    }
    private final void clear_value()
    {
        _value_type = null;
        _null_type = null;
        if (_lob_value_set) {
            _lob_value_set = false;
            _lob_value_position = 0;
        }
        if (!LOB_STATE.EMPTY.equals(_lob_loaded)) {
            _lob_actual_len = -1;
            _lob_bytes = null;
            _lob_loaded = LOB_STATE.EMPTY;
        }
        clear_current_value_buffer();
        clear_annotation_list();
        clear_fieldname();
        _v.clear();
        _value_start_offset = -1;
    }

    private final void set_container_flags(IonType t) {
        switch (t) {
        case LIST:
            _container_is_struct = false;
            _container_prohibits_commas = false;
            break;
        case SEXP:
            _container_is_struct = false;
            _container_prohibits_commas = true;
            break;
        case STRUCT:
            _container_is_struct = true;
            _container_prohibits_commas = false;
            break;
        case DATAGRAM:
            _container_is_struct = false;
            _container_prohibits_commas = true;
            break;
        default:
            throw new IllegalArgumentException("type must be a container, not a "+t.toString());
        }
    }

    private int get_state_after_value()
    {
        int state_after_scalar;
        switch(getContainerType()) {
        case LIST:
        case STRUCT:
            state_after_scalar = STATE_AFTER_VALUE_CONTENTS;
            break;
        case SEXP:
            state_after_scalar = STATE_BEFORE_ANNOTATION_SEXP;
            break;
        case DATAGRAM:
            state_after_scalar = STATE_BEFORE_ANNOTATION_DATAGRAM;
            break;
        default:
            String message = "invalid container type encountered during parsing "
                           + getContainerType()
                           + _scanner.input_position();
            throw new IonException(message);
        }
        if (_nesting_parent != null && getDepth() == 0) {
            state_after_scalar = STATE_EOF;
        }
        return state_after_scalar;
    }
    private final int get_state_after_annotation() {
        int state_after_annotation;
        switch(get_state_int()) {
        case STATE_AFTER_VALUE_CONTENTS:
            IonType container = top_state();
            switch(container) {
            case STRUCT:
            case LIST:
            case DATAGRAM:
                state_after_annotation = STATE_BEFORE_VALUE_CONTENT;
                break;
            case SEXP:
                state_after_annotation = STATE_BEFORE_VALUE_CONTENT_SEXP;
                break;
            default:
                String message = "invalid container type encountered during parsing "
                    + container
                    + _scanner.input_position();
                throw new IonException(message);
            }
            break;
        case STATE_BEFORE_ANNOTATION_DATAGRAM:
        case STATE_BEFORE_ANNOTATION_CONTAINED:
            state_after_annotation = STATE_BEFORE_VALUE_CONTENT;
            break;
        case STATE_BEFORE_ANNOTATION_SEXP:
            state_after_annotation = STATE_BEFORE_VALUE_CONTENT_SEXP;
            break;
        default:
            String message = "invalid state encountered during parsing before the value "
                + get_state_name()
                + _scanner.input_position();
            throw new IonException(message);
        }
        return state_after_annotation;
    }

    private final int get_state_after_container() {
        IonType container = top_state();
        int new_state = get_state_after_container(container);
        return new_state;
    }

    private final int get_state_after_container(int token) {
        IonType container = top_state();

        switch(container) {
            case STRUCT:
                check_container_close(container, TOKEN_CLOSE_BRACE, token);
                break;
            case LIST:
                check_container_close(container, TOKEN_CLOSE_SQUARE, token);
                break;
            case SEXP:
                check_container_close(container, TOKEN_CLOSE_PAREN, token);
                break;
            case DATAGRAM:
                // We shouldn't get here.  Fall through.
            default:
                String message = "invalid container type encountered during parsing "
                    + container
                    + _scanner.input_position();
                throw new IonException(message);
        }

        int new_state = get_state_after_container(container);
        return new_state;
    }

    private final int get_state_after_container(IonType container) {
        int new_state;
        if (container == null) {
            new_state = STATE_BEFORE_ANNOTATION_DATAGRAM;
        }
        else {
            switch(container) {
                case STRUCT:
                case LIST:
                    new_state = STATE_AFTER_VALUE_CONTENTS;
                    break;
                case SEXP:
                    new_state = STATE_BEFORE_ANNOTATION_SEXP;
                    break;
                case DATAGRAM:
                    new_state = STATE_BEFORE_ANNOTATION_DATAGRAM;
                    break;
                default:
                    String message = "invalid container type encountered during parsing "
                        + container
                        + _scanner.input_position();
                    throw new IonException(message);
            }
            if (_nesting_parent != null && getDepth() == 0) {
                new_state = STATE_EOF;
            }
        }
        return new_state;
    }

    private final void check_container_close(IonType container, int expectedToken, int actualToken)
    {
        if (actualToken != expectedToken) {
            String message = container.toString().toLowerCase() + " closed by "
                + IonTokenConstsX.describeToken(actualToken)
                + _scanner.input_position();
            throw new IonException(message);
        }
    }

    private final int get_state_at_container_start(IonType container) {
        int new_state;
        if (container == null) {
            new_state = STATE_BEFORE_ANNOTATION_DATAGRAM;
        }
        else {
            switch (container) {
            case STRUCT:
                new_state = STATE_BEFORE_FIELD_NAME;
                break;
            case LIST:
                new_state = STATE_BEFORE_ANNOTATION_CONTAINED;
                break;
            case SEXP:
                new_state = STATE_BEFORE_ANNOTATION_SEXP;
                break;
            case DATAGRAM:
                new_state = STATE_BEFORE_ANNOTATION_DATAGRAM;
                break;
            default:
                String message = "invalid container type encountered during parsing "
                    + container
                    + _scanner.input_position();
                throw new IonException(message);
            }
        }
        return new_state;
    }


    private final SymbolToken parseSymbolToken(String context,
                                                  StringBuilder sb,
                                                  int t)
        throws IOException
    {
        String text;
        int sid;

        if (t == IonTokenConstsX.TOKEN_SYMBOL_IDENTIFIER) {
            int kw = IonTokenConstsX.keyword(sb, 0, sb.length());
            switch (kw) {
                case IonTokenConstsX.KEYWORD_FALSE:
                case IonTokenConstsX.KEYWORD_TRUE:
                case IonTokenConstsX.KEYWORD_NULL:
                case IonTokenConstsX.KEYWORD_NAN:
                    // keywords are not ok unless they're quoted
                    String reason =
                    "Cannot use unquoted keyword " +
                        sb.toString() + " as " + context;
                    parse_error(reason);
                case IonTokenConstsX.KEYWORD_sid:
                    text = null;
                    sid = IonTokenConstsX.decodeSid(sb);
                    break;
                default:
                    text = sb.toString();
                    sid = UNKNOWN_SYMBOL_ID;
                    break;
            }
        }
        else {
            text = sb.toString();
            sid = UNKNOWN_SYMBOL_ID;
        }

        return new SymbolTokenImpl(text, sid);
    }


    protected final void parse_to_next_value() throws IOException
    {
        int t;
        int action, temp_state;
        boolean trailing_whitespace = false;  // TODO: there's a better way to do this
        StringBuilder sb;

        // FIXME: check depth and type before doing anything further
        //        if we're on a collection and at the correct depth
        //        we need to skip over the contents of the collection
        //        before doing any more parsing

        // we'll need a token to get started here
        // we'll also remember where we were when we started if the
        // user later wants to get a span over this value.  In the
        // case where we just before a comma, after the comma we'll
        // reset this offset since for the span the comma isn't part
        // of the span when it's hoisted
        _value_start_offset = _scanner.getStartingOffset();
        _value_start_line   = _scanner.getLineNumber();
        _value_start_column = _scanner.getLineOffset();

        t = _scanner.nextToken();

        for (;;) {
            int idx = get_state_int() * IonTokenConstsX.TOKEN_count + t;
            action = TransitionActions2[idx];
            // this used to be (but the 2d array is 9072ms vs 8786ms
            // timing, 3% of total file parse time!):
            // action = TransitionActions[get_state_int()][t];
            switch (action) {
            case ACTION_NOT_DEFINED:
                {
                    // TODO why would we get here?
                    boolean span_eof = false;

                    if (_nesting_parent != null) {
                        switch (_nesting_parent) {
                            case LIST:
                                if (t == IonTokenConstsX.TOKEN_CLOSE_SQUARE) {
                                    span_eof = true;
                                }
                                break;
                            case SEXP:
                                if (t == IonTokenConstsX.TOKEN_CLOSE_PAREN){
                                    span_eof = true;
                                }
                                break;
                            case STRUCT:
                                if (t == IonTokenConstsX.TOKEN_CLOSE_BRACE) {
                                    span_eof = true;
                                }
                                break;
                            default:
                                break;
                        }
                    }
                    if (span_eof != true) {
                        String message = "invalid syntax [state:"
                                       + get_state_name()
                                       + " on token:"
                                       +IonTokenConstsX.getTokenName(t)
                                       +"]";
                        parse_error(message);
                    }
                    set_state(STATE_EOF);
                    _eof = true;
                    return;
                }
            case ACTION_EOF:
                set_state(STATE_EOF);
                _eof = true;
                return;
            case ACTION_LOAD_FIELD_NAME:
            {
                if (!is_in_struct_internal()) {
                    throw new IllegalStateException("field names have to be in structs");
                }
                //finish_value(_current_value_save_point);
                finish_and_save_value();

                sb = token_contents_load(t);

                SymbolToken sym = parseSymbolToken("a field name", sb, t);
                set_fieldname(sym);
                clear_current_value_buffer();

                t = _scanner.nextToken();
                if (t != IonTokenConstsX.TOKEN_COLON) {
                    String message = "field name must be followed by a colon, not a "
                                   + IonTokenConstsX.getTokenName(t);
                    parse_error(message);
                }
                _scanner.tokenIsFinished();
                set_state(STATE_BEFORE_ANNOTATION_CONTAINED);
                t = _scanner.nextToken();
                break;
            }
            case ACTION_LOAD_ANNOTATION:
            {
                sb = token_contents_load(t);
                if (sb.length() < 1) {
                    // this is the case for an empty symbol
                    parse_error("empty symbols are not valid");
                }
                trailing_whitespace = _scanner.skip_whitespace();
                if (!_scanner.skipDoubleColon()) {
                    // unnecessary: set_current_value(sp);
                    // this will "loop around" to ACTION_LOAD_SCALAR
                    // since this is necessarily a symbol of one
                    // sort of another
                    temp_state = get_state_after_annotation();
                    set_state(temp_state);
                    break;
                }

                // We have an annotation!
                SymbolToken sym = parseSymbolToken("an annotation", sb, t);
                append_annotation(sym);
                clear_current_value_buffer();

                // Consumed the annotation, move on.
                // note: that peekDoubleColon() consumed the two colons
                // so nextToken won't see them
                t = _scanner.nextToken();
                switch(t) {
                case IonTokenConstsX.TOKEN_SYMBOL_IDENTIFIER:
                case IonTokenConstsX.TOKEN_SYMBOL_QUOTED:
                    // This may be another annotation, so stay in this state
                    // and come around the horn again to check it out.
                    break;
                default:
                    // we leave the error handling to the transition
                    temp_state = get_state_after_annotation();
                    set_state(temp_state);
                    break;
                }
                break;
            }
            case ACTION_START_STRUCT:
                _value_type = IonType.STRUCT;
                temp_state = STATE_BEFORE_FIELD_NAME;
                set_state(temp_state);
                return;
            case ACTION_START_LIST:
                _value_type = IonType.LIST;
                temp_state = STATE_BEFORE_ANNOTATION_CONTAINED;
                set_state(temp_state);
                return;
            case ACTION_START_SEXP:
                _value_type = IonType.SEXP;
                temp_state = STATE_BEFORE_ANNOTATION_SEXP;
                set_state(temp_state);
                return;
            case ACTION_START_LOB:
                switch (_scanner.peekLobStartPunctuation()) {
                case IonTokenConstsX.TOKEN_STRING_DOUBLE_QUOTE:
                    set_state(STATE_IN_CLOB_DOUBLE_QUOTED_CONTENT);
                    _lob_token = IonTokenConstsX.TOKEN_STRING_DOUBLE_QUOTE;
                    _value_type = IonType.CLOB;
                    break;
                case IonTokenConstsX.TOKEN_STRING_TRIPLE_QUOTE:
                    set_state(STATE_IN_CLOB_TRIPLE_QUOTED_CONTENT);
                    _lob_token = IonTokenConstsX.TOKEN_STRING_TRIPLE_QUOTE;
                    _value_type = IonType.CLOB;
                    break;
                default:
                    set_state(STATE_IN_BLOB_CONTENT);
                    _lob_token = IonTokenConstsX.TOKEN_OPEN_DOUBLE_BRACE;
                    _value_type = IonType.BLOB;
                    break;
                }
                return;
            case ACTION_LOAD_SCALAR:
                if (t == IonTokenConstsX.TOKEN_SYMBOL_IDENTIFIER) {
                    sb = token_contents_load(t);
                    int _value_keyword = IonTokenConstsX.keyword(sb, 0, sb.length());
                    switch (_value_keyword) {
                    case IonTokenConstsX.KEYWORD_NULL:
                    {
                        int kwt = trailing_whitespace ? IonTokenConstsX.KEYWORD_none : _scanner.peekNullTypeSymbol();
                        switch (kwt) {
                        case IonTokenConstsX.KEYWORD_NULL:      _null_type = IonType.NULL;       break;
                        case IonTokenConstsX.KEYWORD_BOOL:      _null_type = IonType.BOOL;       break;
                        case IonTokenConstsX.KEYWORD_INT:       _null_type = IonType.INT;        break;
                        case IonTokenConstsX.KEYWORD_FLOAT:     _null_type = IonType.FLOAT;      break;
                        case IonTokenConstsX.KEYWORD_DECIMAL:   _null_type = IonType.DECIMAL;    break;
                        case IonTokenConstsX.KEYWORD_TIMESTAMP: _null_type = IonType.TIMESTAMP;  break;
                        case IonTokenConstsX.KEYWORD_SYMBOL:    _null_type = IonType.SYMBOL;     break;
                        case IonTokenConstsX.KEYWORD_STRING:    _null_type = IonType.STRING;     break;
                        case IonTokenConstsX.KEYWORD_BLOB:      _null_type = IonType.BLOB;       break;
                        case IonTokenConstsX.KEYWORD_CLOB:      _null_type = IonType.CLOB;       break;
                        case IonTokenConstsX.KEYWORD_LIST:      _null_type = IonType.LIST;       break;
                        case IonTokenConstsX.KEYWORD_SEXP:      _null_type = IonType.SEXP;       break;
                        case IonTokenConstsX.KEYWORD_STRUCT:    _null_type = IonType.STRUCT;     break;
                        case IonTokenConstsX.KEYWORD_none:      _null_type = IonType.NULL;       break; // this happens when there isn't a '.' otherwise peek throws the error or returns none
                        default: parse_error("invalid keyword id ("+kwt+") encountered while parsing a null");
                        }
                        // at this point we've consumed a dot '.' and it's preceding whitespace
                        // clear_value();
                        current_value_is_null(_null_type);
                        // set to null_type in above call: _value_type = IonType.NULL;
                        break;
                    }
                    case IonTokenConstsX.KEYWORD_TRUE:
                        _value_type = IonType.BOOL;
                        current_value_is_bool(true);
                        break;
                    case IonTokenConstsX.KEYWORD_FALSE:
                        _value_type = IonType.BOOL;
                        current_value_is_bool(false);
                        break;
                    case IonTokenConstsX.KEYWORD_NAN:
                        _value_type = IonType.FLOAT;
                        clear_current_value_buffer();
                        _v.setValue(Double.NaN);
                        _v.setAuthoritativeType(AS_TYPE.double_value);
                        break;
                    case IonTokenConstsX.KEYWORD_sid:
                    {
                        int sid = IonTokenConstsX.decodeSid(sb);
                        _v.setValue(sid);
                        _v.setAuthoritativeType(AS_TYPE.int_value);
                    }
                    default:
                        // We don't care about any other 'keywords'
                        _value_type = IonType.SYMBOL;
                        break;
                    }
                }
                else if (t == IonTokenConstsX.TOKEN_DOT) {
                    _value_type = IonType.SYMBOL;
                    clear_current_value_buffer();
                    _v.setValue(".");
                    _v.setAuthoritativeType(AS_TYPE.string_value);
                }
                else {
                    // if it's not a symbol we just look at the token type
                    _value_type = IonTokenConstsX.ion_type_of_scalar(t);
                }
                int state_after_scalar = get_state_after_value();
                set_state(state_after_scalar);
                return;
            case ACTION_PLUS_INF:
                _value_type = IonType.FLOAT;
                clear_current_value_buffer();
                _v.setValue(Double.POSITIVE_INFINITY);
                _v.setAuthoritativeType(AS_TYPE.double_value);
                state_after_scalar = get_state_after_value();
                set_state(state_after_scalar);
                return;
            case ACTION_MINUS_INF:
                _value_type = IonType.FLOAT;
                clear_current_value_buffer();
                _v.setValue(Double.NEGATIVE_INFINITY);
                _v.setAuthoritativeType(AS_TYPE.double_value);
                state_after_scalar = get_state_after_value();
                set_state(state_after_scalar);
                return;
            case ACTION_EAT_COMMA:
                if (_container_prohibits_commas) {
                    parse_error("commas aren't used to separate values in "+getContainerType().toString());
                }
                int new_state = STATE_BEFORE_ANNOTATION_CONTAINED;
                if (_container_is_struct) {
                    new_state = STATE_BEFORE_FIELD_NAME;
                }
                set_state(new_state);
                _scanner.tokenIsFinished();
                // when we eat a comma we need to reset the current
                // value start used to define a span, since the comma
                // isn't part of the span when it's hoisted
                _value_start_offset = _scanner.getStartingOffset();
                t = _scanner.nextToken();
                break;
            case ACTION_FINISH_CONTAINER:
                new_state = get_state_after_container(t);
                set_state(new_state);
                _eof = true;
                return;
            case ACTION_FINISH_LOB:
                state_after_scalar = get_state_after_value();
                set_state(state_after_scalar);
                return;
            case ACTION_FINISH_DATAGRAM:
                if (getDepth() != 0) {
                    parse_error("state failure end of datagram encounterd with a non-container stack");
                }
                set_state(STATE_EOF);
                _eof = true;
                return;
            default: parse_error("unexpected token encountered: "+IonTokenConstsX.getTokenName(t));
            }
        }
    }

    protected final StringBuilder token_contents_load(int token_type) throws IOException
    {
        StringBuilder sb = _current_value_buffer;
        boolean       clob_chars_only;
        int           c;

        if (_current_value_buffer_loaded) {
            return sb;
        }
        else if (_current_value_save_point_loaded) {
            assert(!_scanner.isUnfinishedToken() && !_current_value_save_point.isClear());
            // _scanner.load_save_point_contents( _current_value_save_point, sb);

            _scanner.save_point_activate(_current_value_save_point);
            switch (token_type) {
            default:
                _scanner.load_raw_characters(sb);
                break;
            case IonTokenConstsX.TOKEN_SYMBOL_IDENTIFIER:
                _scanner.load_symbol_identifier(sb);
                _value_type = IonType.SYMBOL;
                break;
            case IonTokenConstsX.TOKEN_SYMBOL_OPERATOR:
                _scanner.load_symbol_operator(sb);
                _value_type = IonType.SYMBOL;
                break;
            case IonTokenConstsX.TOKEN_SYMBOL_QUOTED:
                clob_chars_only = (IonType.CLOB == _value_type);
                _scanner.load_single_quoted_string(sb, clob_chars_only);
                _value_type = IonType.SYMBOL;
                break;
            case IonTokenConstsX.TOKEN_STRING_DOUBLE_QUOTE:
                clob_chars_only = (IonType.CLOB == _value_type);
                _scanner.load_double_quoted_string(sb, clob_chars_only);
                _value_type = IonType.STRING;
                break;
            case IonTokenConstsX.TOKEN_STRING_TRIPLE_QUOTE:
                clob_chars_only = (IonType.CLOB == _value_type);
                _scanner.load_triple_quoted_string(sb, clob_chars_only);
                _value_type = IonType.STRING;
                break;
            }
            _scanner.save_point_deactivate(_current_value_save_point);
            _current_value_buffer_loaded = true;
        }
        else {
            _scanner.save_point_start(_current_value_save_point);
            switch (token_type) {
            case IonTokenConstsX.TOKEN_UNKNOWN_NUMERIC:
            case IonTokenConstsX.TOKEN_INT:
            case IonTokenConstsX.TOKEN_BINARY:
            case IonTokenConstsX.TOKEN_HEX:
            case IonTokenConstsX.TOKEN_FLOAT:
            case IonTokenConstsX.TOKEN_DECIMAL:
            case IonTokenConstsX.TOKEN_TIMESTAMP:
                _value_type = _scanner.load_number(sb);
                break;
            case IonTokenConstsX.TOKEN_SYMBOL_IDENTIFIER:
                _scanner.load_symbol_identifier(sb);
                _value_type = IonType.SYMBOL;
                break;
            case IonTokenConstsX.TOKEN_SYMBOL_OPERATOR:
                _scanner.load_symbol_operator(sb);
                _value_type = IonType.SYMBOL;
                break;
            case IonTokenConstsX.TOKEN_SYMBOL_QUOTED:
                clob_chars_only = (IonType.CLOB == _value_type);
                c = _scanner.load_single_quoted_string(sb, clob_chars_only);
                if (c == UnifiedInputStreamX.EOF) {
                    //String message = "EOF encountered before closing single quote";
                    //parse_error(message);
                    _scanner.unexpected_eof();
                }
                _value_type = IonType.SYMBOL;
                break;
            case IonTokenConstsX.TOKEN_STRING_DOUBLE_QUOTE:
                clob_chars_only = (IonType.CLOB == _value_type);
                c = _scanner.load_double_quoted_string(sb, clob_chars_only);
                if (c == UnifiedInputStreamX.EOF) {
                    // String message = "EOF encountered before closing single quote";
                    // parse_error(message);
                    _scanner.unexpected_eof();
                }
                _value_type = IonType.STRING;
                break;
            case IonTokenConstsX.TOKEN_STRING_TRIPLE_QUOTE:
                clob_chars_only = (IonType.CLOB == _value_type);
                c = _scanner.load_triple_quoted_string(sb, clob_chars_only);
                if (c == UnifiedInputStreamX.EOF) {
                    //String message = "EOF encountered before closing single quote";
                    //parse_error(message);
                    _scanner.unexpected_eof();
                }
                _value_type = IonType.STRING;
                break;
            default:
                String message = "unexpected token "
                               + IonTokenConstsX.getTokenName(token_type)
                               + " encountered";
                throw new IonException(message);
            }
            _current_value_save_point.markEnd();
            _current_value_save_point_loaded = true;
            _current_value_buffer_loaded = true;
            tokenValueIsFinished();
        }
        return sb;
    }

    /**
     * called by super classes to tell us that the
     * current token has been consumed.
     */
    protected void tokenValueIsFinished()
    {
        _scanner.tokenIsFinished();
        if (IonType.BLOB.equals(_value_type) || IonType.CLOB.equals(_value_type))
        {
            int state_after_scalar = get_state_after_value();
            set_state(state_after_scalar);
        }
    }

    private final void push_container_state(IonType newContainer)
    {
        int oldlen = _container_state_stack.length;
        if (_container_state_top >= oldlen) {
            int newlen = oldlen * 2;
            IonType[] temp = new IonType[newlen];
            System.arraycopy(_container_state_stack, 0, temp, 0, oldlen);
            _container_state_stack = temp;
        }
        set_container_flags(newContainer);
        _container_state_stack[_container_state_top++] = newContainer;
    }

    private final void pop_container_state() {
        _container_state_top--;
        set_container_flags(top_state());
        _eof = false;
        _has_next_called = false;

        int new_state = get_state_after_container();
        set_state(new_state);
    }

    private final IonType top_state() {
        int top = _container_state_top - 1;
        IonType top_container = _container_state_stack[top];
        return top_container;
    }

    public IonType getType()
    {
        return _value_type;
    }
    // externally we're if we're in a hoisted struct
    // we're not really in a struct, we at the top level
    public boolean isInStruct()
    {
        boolean in_struct = false;
        IonType container = getContainerType();
        if (IonType.STRUCT.equals(container)) {
            if (getDepth() > 0) {
                in_struct = true;
            }
            else {
                assert(IonType.STRUCT.equals(_nesting_parent) == true);
            }
        }
        return in_struct;
    }
    // internally (really only in parse_to_next()) we care
    // about being in a struct even if it's a hoisted container
    // since the hoisted values will still have a field name we
    // have to ignore
    private boolean is_in_struct_internal()
    {
        boolean in_struct = false;
        IonType container = getContainerType();
        if (IonType.STRUCT.equals(container)) {
            in_struct = true;
        }
        return in_struct;
    }
    public IonType getContainerType()
    {
        if (_container_state_top == 0) return IonType.DATAGRAM;
        return _container_state_stack[_container_state_top - 1];
    }
    public int getDepth()
    {
        int depth = _container_state_top;
        if (depth > 0) {
int debugging_depth = depth;
            IonType top_type = _container_state_stack[0];
            if (_nesting_parent == null) {
                if (IonType.DATAGRAM.equals(top_type)) {
                    depth--;
                }
            }
            else {
                if (_nesting_parent.equals(top_type)) {
                    depth--;
                }
            }
if (depth == debugging_depth) {
    System.err.println("so here's a case where we didn't subtract 1");
}
        }
        return depth;
    }

    public String getFieldName()
    {
        // For hoisting
        if (getDepth() == 0 && is_in_struct_internal()) return null;

        String name = _field_name;
        if (name == null && _field_name_sid > 0)
        {
            throw new UnknownSymbolException(_field_name_sid);
        }
        return name;
    }

    final String getRawFieldName()
    {
        // For hoisting
        if (getDepth() == 0 && is_in_struct_internal()) return null;
        return _field_name;
    }

    int getFieldId()
    {
        // For hoisting
        if (getDepth() == 0 && is_in_struct_internal()) return UNKNOWN_SYMBOL_ID;
        return _field_name_sid;
    }

    public SymbolToken getFieldNameSymbol()
    {
        // For hoisting
        if (getDepth() == 0 && is_in_struct_internal()) return null;

        String name = _field_name;
        int sid = getFieldId();
        if (name == null && sid == UNKNOWN_SYMBOL_ID) return null;
        return new SymbolTokenImpl(name, sid);
    }

    public Iterator<String> iterateTypeAnnotations()
    {
        return PrivateUtils.stringIterator(getTypeAnnotations());
    }

    public String[] getTypeAnnotations()
    {
        return PrivateUtils.toStrings(_annotations, _annotation_count);
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
            break;
        default:
            throw new IllegalStateException();
        }

        int new_state = get_state_at_container_start(_value_type);
        set_state(new_state);

        push_container_state(_value_type);

        _scanner.tokenIsFinished();
        try {
            finish_value(null);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        if (_v.isNull()) {
            _eof = true;
            _has_next_called = true;  // there are no contents in a null container
        }

        _value_type = null;

        if (_debug) System.out.println("stepInto() new depth: "+getDepth());
    }
    public void stepOut()
    {
        if (getDepth() < 1) {
            throw new IllegalStateException(IonMessages.CANNOT_STEP_OUT);
        }
        try {
            finish_value(null);
            switch (getContainerType()) {
            case STRUCT:
                if (!_eof) _scanner.skip_over_struct();
                break;
            case LIST:
                if (!_eof) _scanner.skip_over_list();
                break;
            case SEXP:
                if (!_eof) _scanner.skip_over_sexp();
                break;
            case DATAGRAM:
                break;
            default:
                throw new IllegalStateException();
            }
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        pop_container_state();
        _scanner.tokenIsFinished();
        try {
            finish_value(null);
        }
        catch (IOException e) {
            throw new IonException(e);
        }

        clear_value();

        if (_debug) System.out.println("stepOUT() new depth: "+getDepth());
    }

    //
    // symbol related code that is inactive in this parser
    //
    public SymbolTable getSymbolTable()
    {
        return null;
    }

    //
    // helper classes
    //

    public static class IonReaderTextParsingException extends IonException {
        private static final long serialVersionUID = 1L;

        IonReaderTextParsingException(String msg) {
            super(msg);
        }
        IonReaderTextParsingException(Exception e) {
            super(e);
        }
        IonReaderTextParsingException(String msg, Exception e) {
            super(msg, e);
        }
    }

    protected final void parse_error(String reason) {
        String message =
                "Syntax error"
              + _scanner.input_position()
              + ": "
              + reason;
        throw new IonReaderTextParsingException(message);
    }
    protected final void parse_error(Exception e) {
        String message =
                "Syntax error at "
              + _scanner.input_position()
              + ": "
              + e.getLocalizedMessage();
        throw new IonReaderTextParsingException(message, e);
    }
}
