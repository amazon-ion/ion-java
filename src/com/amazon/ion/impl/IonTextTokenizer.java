// Copyright (c) 2008-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.util.IonTextUtils.printCodePointAsString;

import com.amazon.ion.UnexpectedEofException;
import com.amazon.ion.util.IonTextUtils;
import java.io.IOException;

/**
 * Tokenizer for the Ion text parser in IonTextIterator. This
 * reads bytes and returns the interesting tokens it recognizes
 * or an error.  While, currently, this does UTF-8 decoding
 * as it goes that is unnecessary.  The main entry point is
 * lookahead(n) which gets the token type n tokens ahead (0
 * is the next token).  The tokens type, its starting offset
 * in the input stream and its ending offset in the input stream
 * are cached, so lookahead() can be called repeatedly with
 * little overhead.  This supports a 7 token lookahead and requires
 * a "recompile" to change this limit.  (this could be "fixed"
 * but seems unnecessary at this time - the limit is in
 * IonTextTokenizer._token_lookahead_size which is 1 larger than
 * the size of the lookahead allowed)  Tokens are consumed by
 * a call to consumeToken, or the helper consumeTokenAsString.
 * The informational interfaces - getValueStart(), getValueEnd()
 * getValueAsString() can be used to get the contents of the
 * value once the caller has decided how to use it.
 */
public final class IonTextTokenizer
{
//////////////////////////////////////////////////////////////////////////  debug
static final boolean _debug = false;
//static final boolean _token_counter_on = true;
//int _tokens_read = 0;
//int _tokens_consumed = 0;
    private static final int EMPTY_PEEKAHEAD = -2;
    private static final int EMPTY_ESCAPE_SEQUENCE = -2;

    public static final int TOKEN_ERROR     =  -1;
    public static final int TOKEN_EOF       =   0;

    public static final int TOKEN_INT       =   1;
    public static final int TOKEN_HEX       =   2;
    public static final int TOKEN_DECIMAL   =   3;
    public static final int TOKEN_FLOAT     =   4;
    public static final int TOKEN_TIMESTAMP =   5;
    public static final int TOKEN_BLOB      =   6;

    public static final int TOKEN_SYMBOL1 =  TOKEN_BLOB + 1; //  7 java identifier
    public static final int TOKEN_SYMBOL2 =  TOKEN_BLOB + 2; //  8 single quoted string
    public static final int TOKEN_SYMBOL3 =  TOKEN_BLOB + 3; //  9 operator sequence for sexp
    public static final int TOKEN_STRING1 =  TOKEN_BLOB + 4; // 10 string in double quotes (") some > 255
    public static final int TOKEN_STRING2 =  TOKEN_BLOB + 5; // 11 part of a string in triple quotes (''') some > 255
    public static final int TOKEN_STRING3 =  TOKEN_BLOB + 6; // 12 string in double quotes (") all <= 255
    public static final int TOKEN_STRING4 =  TOKEN_BLOB + 7; // 13 part of a string in triple quotes (''') all chars <= 255

    public static final int TOKEN_DOT          = TOKEN_STRING4 + 1;   // 14
    public static final int TOKEN_COMMA        = TOKEN_STRING4 + 2;   // 15
    public static final int TOKEN_COLON        = TOKEN_STRING4 + 3;   // 16
    public static final int TOKEN_DOUBLE_COLON = TOKEN_STRING4 + 4;   // 17

    public static final int TOKEN_OPEN_PAREN   = TOKEN_DOUBLE_COLON + 1;   // 18
    public static final int TOKEN_CLOSE_PAREN  = TOKEN_DOUBLE_COLON + 2;   // 19
    public static final int TOKEN_OPEN_BRACE   = TOKEN_DOUBLE_COLON + 3;   // 20
    public static final int TOKEN_CLOSE_BRACE  = TOKEN_DOUBLE_COLON + 4;   // 21
    public static final int TOKEN_OPEN_SQUARE  = TOKEN_DOUBLE_COLON + 5;   // 22
    public static final int TOKEN_CLOSE_SQUARE = TOKEN_DOUBLE_COLON + 6;   // 23

    public static final int TOKEN_OPEN_DOUBLE_BRACE  = TOKEN_CLOSE_SQUARE + 1;   // 24
    public static final int TOKEN_CLOSE_DOUBLE_BRACE = TOKEN_CLOSE_SQUARE + 2;   // 25

    public static final int TOKEN_MAX = TOKEN_CLOSE_DOUBLE_BRACE; // 25 = 2 + 6 + 4 + 7 + 6

    public static final int KEYWORD_TRUE      =  1;
    public static final int KEYWORD_FALSE     =  2;
    public static final int KEYWORD_NULL      =  3;
    public static final int KEYWORD_BOOL      =  4;
    public static final int KEYWORD_INT       =  5;
    public static final int KEYWORD_FLOAT     =  6;
    public static final int KEYWORD_DECIMAL   =  7;
    public static final int KEYWORD_TIMESTAMP =  8;
    public static final int KEYWORD_SYMBOL    =  9;
    public static final int KEYWORD_STRING    = 10;
    public static final int KEYWORD_BLOB      = 11;
    public static final int KEYWORD_CLOB      = 12;
    public static final int KEYWORD_LIST      = 13;
    public static final int KEYWORD_SEXP      = 14;
    public static final int KEYWORD_STRUCT    = 15;

    public static String getTokenName(int t) {
        switch (t) {
        case TOKEN_ERROR:     return "TOKEN_ERROR";
        case TOKEN_EOF:       return "TOKEN_EOF";

        case TOKEN_INT:       return "TOKEN_INT";
        case TOKEN_HEX:       return "TOKEN_HEX";
        case TOKEN_DECIMAL:   return "TOKEN_DECIMAL";
        case TOKEN_FLOAT:     return "TOKEN_FLOAT";
        case TOKEN_TIMESTAMP: return "TOKEN_TIMESTAMP";
        case TOKEN_BLOB:      return "TOKEN_BLOB";

        case TOKEN_SYMBOL1: return "TOKEN_SYMBOL1";
        case TOKEN_SYMBOL2: return "TOKEN_SYMBOL2";
        case TOKEN_SYMBOL3: return "TOKEN_SYMBOL3";
        case TOKEN_STRING1: return "TOKEN_STRING1";
        case TOKEN_STRING2: return "TOKEN_STRING2";
        case TOKEN_STRING3: return "TOKEN_STRING3";
        case TOKEN_STRING4: return "TOKEN_STRING4";

        case TOKEN_DOT:          return "TOKEN_DOT";
        case TOKEN_COMMA:          return "TOKEN_COMMA";
        case TOKEN_COLON:          return "TOKEN_COLON";
        case TOKEN_DOUBLE_COLON: return "TOKEN_DOUBLE_COLON";

        case TOKEN_OPEN_PAREN:   return "TOKEN_OPEN_PAREN";
        case TOKEN_CLOSE_PAREN:  return "TOKEN_CLOSE_PAREN";
        case TOKEN_OPEN_BRACE:   return "TOKEN_OPEN_BRACE";
        case TOKEN_CLOSE_BRACE:  return "TOKEN_CLOSE_BRACE";
        case TOKEN_OPEN_SQUARE:  return "TOKEN_OPEN_SQUARE";
        case TOKEN_CLOSE_SQUARE: return "TOKEN_CLOSE_SQUARE";

        case TOKEN_OPEN_DOUBLE_BRACE:  return "TOKEN_OPEN_DOUBLE_BRACE";
        case TOKEN_CLOSE_DOUBLE_BRACE: return "TOKEN_CLOSE_DOUBLE_BRACE";
        default: return "<invalid token "+t+">";
        }
    }

    static boolean[] isBase64Character = makeBase64Array();
    static int       base64FillerCharacter = '=';
    static boolean[] makeBase64Array()
    {
        boolean[] base64 = new boolean[128];

        for (int ii='0'; ii<='9'; ii++) {
            base64[ii] = true;
        }
        for (int ii='a'; ii<='z'; ii++) {
            base64[ii] = true;
        }
        for (int ii='A'; ii<='Z'; ii++) {
            base64[ii] = true;
        }
        base64['+'] = true;
        base64['/'] = true;
        return base64;
    }

    static int[] hexValue = makeHexValueArray();
    static boolean[] isHexDigit = makeHexDigitTestArray(hexValue);
    static int[] makeHexValueArray() {
        int[] hex = new int[128];
        for (int ii=0; ii<128; ii++) {
            hex[ii] = -1;
        }
        for (int ii='0'; ii<='9'; ii++) {
            hex[ii] = ii - '0';
        }
        for (int ii='a'; ii<='f'; ii++) {
            hex[ii] = ii - 'a' + 10;
        }
        for (int ii='A'; ii<='F'; ii++) {
            hex[ii] = ii - 'A' + 10;
        }
        return hex;
    }
    static boolean[] makeHexDigitTestArray(int [] hex_characters) {
        boolean[] is_hex = new boolean[hex_characters.length];
        for (int ii=0; ii<hex_characters.length; ii++) {
            is_hex[ii] = (hex_characters[ii] >= 0);
        }
        return is_hex;
    }

    private final boolean isValueTerminatingCharacter(int c) throws IOException
    {
        boolean isTerminator;

        if (c == '/') {
            // this is terminating only if it starts a comment of some sort
            c = read_char();
            unread_char(c);  // we never "keep" this character
            isTerminator = (c == '/' || c == '*');
        }
        else {
            isTerminator = IonTextUtils.isNumericStop(c);
        }

        return isTerminator;
    }

    IonTextBufferedStream _r = null;

    int              _token = -1;

    static final int _token_lookahead_size = 8;
    int              _token_lookahead_current;
    int              _token_lookahead_next_available;
    int[]            _token_lookahead_start = new int[_token_lookahead_size];
    int[]            _token_lookahead_end   = new int[_token_lookahead_size];
    int[]            _token_lookahead_token = new int[_token_lookahead_size];

    StringBuffer     _saved_symbol = new StringBuffer();
    boolean          _has_saved_symbol = false;
    boolean          _has_marked_symbol = false;

    static final int _offset_queue_size = 6; // 5 + 1, character lookahead + 1
    int              _char_lookahead_top = 0;
    int[]            _char_lookahead_stack = new int[_offset_queue_size - 1];
    int[]         _char_lookahead_stack_char_length = new int[_offset_queue_size - 1];
    int              _peek_ahead_char = EMPTY_PEEKAHEAD;
    int              _char_length = 0;
    int              _offset = 0;
    int              _line = 1;
    int              _offset_queue_head = 0;
    int          _offset_queue_tail = 0;
    int[]          _offset_queue = new int[_offset_queue_size];

    /**
     * calculates the first byte of the current character in the input buffer
     * using the current reader position and any pushed back lookahead
     * character lengths
     * @return byte offset
     */
    public final int currentCharStart() {
        int p = this._r.position();
        p -= ((_peek_ahead_char == EMPTY_PEEKAHEAD) ? 0 : 1);
        if (_char_lookahead_top > 0) {
            for (int ii=0; ii<_char_lookahead_top; ii++) {
                p -= _char_lookahead_stack_char_length[ii];
            }
        }
        return p - _char_length;
    }
    /**
     * calculates the last byte of the current character in the input buffer
     * using the current reader position and any pushed back lookahead
     * character lengths
     * @return byte offset
     */
    public final int nextCharStart() {
        int p = this._r.position();
        p -= ((_peek_ahead_char == EMPTY_PEEKAHEAD) ? 0 : 1);
        if (_char_lookahead_top > 0) {
            for (int ii=0; ii<_char_lookahead_top; ii++) {
                p -= _char_lookahead_stack_char_length[ii];
            }
        }
        return p;
    }

    private IonTextTokenizer() {}
    IonTextTokenizer _saved_copy;
    int              _saved_position;
    IonTextTokenizer get_saved_copy() {
        if (_saved_copy == null) {
            _saved_copy = new IonTextTokenizer();
        }
        _saved_position = _r.position();
        copy_state_to(_saved_copy);
        return _saved_copy;
    }
    void restore_state() {
        _saved_copy.copy_state_to(this);
        _r.setPosition(_saved_position);
    }
    void copy_state_to(IonTextTokenizer other) {

        //if (_token_counter_on) {
        //  other._tokens_read = this._tokens_read;
        //  other._tokens_consumed = this._tokens_consumed;
        //}

        other._token = _token;
        //other._start = _start;
        //other._end   = _end;

        other._char_lookahead_top = this._char_lookahead_top;
        System.arraycopy(this._char_lookahead_stack, 0
                        ,other._char_lookahead_stack, 0
                        ,this._char_lookahead_top);
        System.arraycopy(this._char_lookahead_stack_char_length, 0
                        ,other._char_lookahead_stack_char_length, 0
                        ,this._char_lookahead_top);

        other._char_length = this._char_length;
        other._line = this._line;
        other._offset = this._offset;
        other._peek_ahead_char = this._peek_ahead_char;
        other._offset_queue_head = this._offset_queue_head;
        other._offset_queue_tail = this._offset_queue_tail;
        System.arraycopy(this._offset_queue, 0
                ,other._offset_queue, 0
                ,_offset_queue_size);


        other._r = this._r;
        // we deal with "saved position in get_saved_copy and restore_state"
        // not here: _saved_position = this._r.position();

        other._has_marked_symbol = this._has_marked_symbol;
        other._has_saved_symbol = this._has_saved_symbol;
        other._saved_symbol.setLength(0);
        other._saved_symbol.append(this._saved_symbol);
        //other._saved_symbol_len = this._saved_symbol_len;
        //System.arraycopy(this._saved_symbol_buf, 0, other._saved_symbol_buf, 0, other._saved_symbol_len);

        other._token_lookahead_current = this._token_lookahead_current;
        other._token_lookahead_next_available = this._token_lookahead_next_available;
        System.arraycopy(this._token_lookahead_start, 0, this._token_lookahead_start, 0, this._token_lookahead_start.length);
        System.arraycopy(this._token_lookahead_end, 0, this._token_lookahead_end, 0, this._token_lookahead_end.length);
        System.arraycopy(this._token_lookahead_token, 0, this._token_lookahead_token, 0, this._token_lookahead_token.length);
    }

    public IonTextTokenizer(byte[] buffer)
    {
        _r = IonTextBufferedStream.makeStream(buffer);
        reset();
    }
    public IonTextTokenizer(byte[] buffer, int offset, int len)
    {
        _r = IonTextBufferedStream.makeStream(buffer, offset, len);
        reset();
    }
    public IonTextTokenizer(String ionText)
    {
        _r = IonTextBufferedStream.makeStream(ionText);
        reset();
    }

    public final void reset() {
        _r.setPosition(0);
        _char_lookahead_top = 0;
        _token_lookahead_current = 0;
        _token_lookahead_next_available = 0;
        _has_saved_symbol = false;
        _has_marked_symbol = false;
        _saved_symbol.setLength(0);
        _line = 1;
        _offset = 0;
        // _peek_ahead_char = -1;
        //_saved_symbol_len = 0;
        //_start = -1;
        //_end = -1;
    }
    final void enqueueToken(int token) {
        int next = _token_lookahead_next_available;
        _token_lookahead_token[next] = token;
        next++;
        if (next == _token_lookahead_size) next = 0;
        if (next == _token_lookahead_current) {
            throw new IonTextReader.IonParsingException("queue is full enquing token (internal error) "+input_position());
        }
        _token_lookahead_next_available = next;

    }
    // this isn't actually needed, the next start and end values in the
    // _token_lookahead_start (and end) arrays have already been set by
    // setNextStart() and setNextEnd() calls
    //final void enqueueToken(int token, int start, int end) {
    //    setNextStart(start);
    //    setNextEnd(end);
    //    enqueueToken(token);
    //}
    final void dequeueToken() {
        if (_token_lookahead_current == _token_lookahead_next_available) {
            throw new IonTextReader.IonParsingException("token queue empty (internal error) "+input_position());
        }
        _token_lookahead_current++;

        // if (_token_counter_on) _tokens_consumed++;

        if (_token_lookahead_current == _token_lookahead_size) _token_lookahead_current = 0;
    }
    final int queueCount() {
        if (_token_lookahead_current == _token_lookahead_next_available) {
            return 0;
        }
        else if (_token_lookahead_current < _token_lookahead_next_available) {
            return _token_lookahead_next_available - _token_lookahead_current;
        }
        // when next has wrapped and current hasn't adding size restores the
        // "natural" order (so to speak)
        return _token_lookahead_next_available + _token_lookahead_size - _token_lookahead_current;
    }
    final int queuePosition(int lookahead) {
        if (lookahead >= queueCount()) return IonTextTokenizer.TOKEN_EOF;
        int pos = _token_lookahead_current + lookahead;
        if (pos >= _token_lookahead_size) pos -= _token_lookahead_size;
        return pos;
    }
    final void setNextStart(int start) {
        _token_lookahead_start[_token_lookahead_next_available] = start;
    }
    final void setNextEnd(int end) {
        _token_lookahead_end[_token_lookahead_next_available] = end;
    }
    final void setStart(int start, int lookahead) {
        int pos = queuePosition(lookahead);
        _token_lookahead_start[pos] = start;
    }
    final void setEnd(int end, int lookahead) {
        int pos = queuePosition(lookahead);
        _token_lookahead_end[pos] = end;
    }
    final int getStart() {
        int start = this.peekTokenStart(0);
        return start;
    }
    final int getEnd() {
        int end = this.peekTokenEnd(0);
        return end;
    }
    final int peekTokenStart(int lookahead) {
        int pos = queuePosition(lookahead);
        int start = _token_lookahead_start[pos];
        return start;
    }
    final int peekTokenEnd(int lookahead) {
        int pos = queuePosition(lookahead);
        int end = _token_lookahead_end[pos];
        return end;
    }
    final int peekTokenToken(int lookahead) {
        int pos = queuePosition(lookahead);
        int token = _token_lookahead_token[pos];
        return token;
    }
    void scanBase64Value(IonTextReader parser) {
        int c, len = 0;
        int start, end;
        try {
            start = nextCharStart(); // we really start at the next character
            this.setNextStart(start);
            for (;;) {
                c = this.read_char();
                if (c < 0) {
                    throw new UnexpectedEofException();
                }
                if (c > 127) {
                    parser.error();
                }
                if (Character.isWhitespace((char)c)) continue;
                if (!isBase64Character[c]) break;
                len++;
            }
            int filler_len = 0;
            while (c == base64FillerCharacter) {
                filler_len++;
                c = this.read_char();
            }
            if ( filler_len > 3 ) {
                throw new IonTextReader.IonParsingException("base64 allows no more than 3 chars of filler (trailing '=' chars), in tokenizer"+input_position());
            }
            if ( ((filler_len + len) & 0x3) != 0 ) { // if they're using the filler char the len should be divisible by 4
                throw new IonTextReader.IonParsingException("base64 must be a multiple of 4 characters, in tokenizer"+input_position());
            }
            end = currentCharStart();
            this.setNextEnd(end);
            this.unread_char(c);
            _has_marked_symbol = true;
        } catch (IOException e) {
            throw new IonTextReader.IonParsingException(e, "in tokenizer"+input_position());
        }
    }
    public final int lob_lookahead() {
        assert queueCount() == 0;

        int c;
        try {
            do {
                c = this.read_char();
            } while (Character.isWhitespace((char)c));
        } catch (IOException e) {
            throw new IonTextReader.IonParsingException(e, " in scanner"+this.input_position());
        }
        // we don't consume it, we just look at it
        this.unread_char(c);
        return c;
    }
    public final int lookahead(int distance)
    {
        if (distance < 0 || distance >= IonTextTokenizer._token_lookahead_size) {
            throw new IonTextReader.IonParsingException("invalid lookahead distance "+distance+" (internal error)"+input_position());
        }
        if (distance >= queueCount()) {
            try {
                while (distance >= queueCount()) {
                    fill_queue();
                }
            }
            catch (IOException e) {
                throw new IonTextReader.IonParsingException(e, "in tokenizer"+input_position());
            }
        }
        return this.peekTokenToken(distance);
    }


    public final void consumeToken() {
        if (_debug) {
            System.out.println(" consume "+getTokenName(this.lookahead(0)));
        }
        dequeueToken();
    }

    public final void fill_queue() throws IOException
    {
        int t = -1;
        int c2;
        _has_saved_symbol = false;
        _has_marked_symbol = false;

loop:   for (;;) {
            int c = this.read_char();
            switch (c) {
            case -1:
                t = IonTextTokenizer.TOKEN_EOF;
                break loop;
            case ' ':
            case '\t':
            case '\n': // new line normalization is handled in read_char
                break;
            case '/':
                c2 = this.read_char();
                if (c2 == '/') {
                    this.read_single_line_comment();
                } else if (c2 == '*') {
                    this.read_block_comment();
                }
                else {
                    this.unread_char(c2);
                    t = read_symbol_extended(c);
                    break loop;
                }
                break;
            case ':':
                c2 = this.read_char();
                if (c2 != ':') {
                    this.unread_char(c2);
                    t = IonTextTokenizer.TOKEN_COLON;
                }
                else {
                    t = IonTextTokenizer.TOKEN_DOUBLE_COLON;
                }
                break loop;
            case '{':
                c2 = this.read_char();
                if (c2 != '{') {
                    this.unread_char(c2);
                    t = IonTextTokenizer.TOKEN_OPEN_BRACE;
                }
                else {
                    t = IonTextTokenizer.TOKEN_OPEN_DOUBLE_BRACE;
                }
                break loop;
            case '}':
                t = IonTextTokenizer.TOKEN_CLOSE_BRACE;
                // detection of double closing braces is done
                // in the parser in the blob and clob handling
                // state - it's otherwise ambiguous with closing
                // two structs together. see tryForDoubleBrace() below
                //c2 = this.read_char();
                //if (c2 != '}') {
                //    this.unread_char(c2);
                //    t = IonTextTokenizer.TOKEN_CLOSE_BRACE;
                //}
                //else {
                //    t = IonTextTokenizer.TOKEN_CLOSE_DOUBLE_BRACE;
                //}
                break loop;
            case '[':
                t = IonTextTokenizer.TOKEN_OPEN_SQUARE;
                break loop;
            case ']':
                t = IonTextTokenizer.TOKEN_CLOSE_SQUARE;
                break loop;
            case '(':
                t = IonTextTokenizer.TOKEN_OPEN_PAREN;
                break loop;
            case ')':
                t = IonTextTokenizer.TOKEN_CLOSE_PAREN;
                break loop;
            case ',':
                // since we're not calling a helper (which all fill in the
                // start and end) we need to do it ourselves here
                setNextStart(currentCharStart());
                setNextEnd(nextCharStart());
                t = IonTextTokenizer.TOKEN_COMMA;
                break loop;
            case '.':
                // since we're not calling a helper (which all fill in the
                // start and end) we need to do it ourselves here
                setNextStart(currentCharStart());
                setNextEnd(nextCharStart());
                t = IonTextTokenizer.TOKEN_DOT;
                break loop;
            case '\'':
                t = read_quoted_symbol(c);
                break loop;
            case '+': case '#':
            case '<': case '>': case '*': case '=': case '^': case '&': case '|':
            case '~': case ';': case '!': case '?': case '@': case '%': case '`':
                t = this.read_symbol_extended(c);
                break loop;
            case '"':
                t = read_quoted_string(c);
                break loop;
            case 'a': case 'b': case 'c': case 'd': case 'e': case 'f':
            case 'g': case 'h': case 'j': case 'i': case 'k': case 'l':
            case 'm': case 'n': case 'o': case 'p': case 'q': case 'r':
            case 's': case 't': case 'u': case 'v': case 'w': case 'x':
            case 'y': case 'z':
            case 'A': case 'B': case 'C': case 'D': case 'E': case 'F':
            case 'G': case 'H': case 'J': case 'I': case 'K': case 'L':
            case 'M': case 'N': case 'O': case 'P': case 'Q': case 'R':
            case 'S': case 'T': case 'U': case 'V': case 'W': case 'X':
            case 'Y': case 'Z':
            case '$': case '_':
                t = read_symbol(c);
                break loop;
            case '0': case '1': case '2': case '3': case '4':
            case '5': case '6': case '7': case '8': case '9':
                t = read_number(c);
                break loop;
            case '-':
                // see if we have a number or what might be an extended symbol
                c2 = this.read_char();
                this.unread_char(c2);
                if (Character.isDigit(c2)) {
                    t = read_number(c);
                }
                else {
                    t = read_symbol_extended(c);
                }
                break loop;
            default:
                bad_token_start(c);
            }
        }

        // really we just enqueue this, we don't care about it otherwise
        this.enqueueToken(t);
        return;
    }

    public String input_position() {
        String s = " at line " + _line + " offset " + _offset;
        return s;
    }
    void enqueue_offset(int offset)
    {
        _offset_queue_head++;
        if (_offset_queue_head >= _offset_queue_size) {
            _offset_queue_head = 0;
        }
        if (_offset_queue_head == _offset_queue_tail) {
            _offset_queue_tail++;
            if (_offset_queue_tail >= _offset_queue_size) {
                _offset_queue_tail = 0;
            }
        }
        _offset_queue[_offset_queue_head] = offset;
    }
    int dequeue_offset()
    {
        int offset = _offset_queue[_offset_queue_head];

        _offset_queue_head--;
        if (_offset_queue_head < 0) {
            _offset_queue_head = _offset_queue_size - 1;
        }
        if (_offset_queue_head == _offset_queue_tail) {
            _offset_queue_tail--;
            if (_offset_queue_tail < 0) {
                _offset_queue_tail = _offset_queue_size - 1;
            }
        }
        return offset;
    }
    private final void unread_char(int c) {
        int next_length = _char_lookahead_stack_char_length[_char_lookahead_top];
        _char_lookahead_stack_char_length[_char_lookahead_top] = _char_length;
        _char_lookahead_stack[_char_lookahead_top++] = c;
        _char_length = next_length;
        if (c == '\n') {
            _line--;
            _offset = dequeue_offset();
        }
        else {
            _offset--;
        }
    }
    // this is a variant of unread that is used to push the next char in a
    // surrogate pair into the "pending character" stream, it's never a
    // new line (since it is, by definition, a low surrogate character)
    // but it also never incremented the offset, so we don't decrement it
    private final void preread_char(int c) {
        int half_length = _char_length / 2;   // we don't really use this when there are surrogates
        _char_length -= half_length;          // in play, but we still need values - they may as well be reasonable
        _char_lookahead_stack_char_length[_char_lookahead_top] = half_length;
        _char_lookahead_stack[_char_lookahead_top++] = c;
    }
    final int peek_char()
    {
        int c;
        try {
            c = read_char();
        } catch (IOException e) {
            throw new IonTextReader.IonParsingException(e
                                                        , " getting character in tokenizer"+input_position());
        }
        unread_char(c);
        return c;
    }
    private final int read_char() throws IOException
    {
        int c;

        if (_char_lookahead_top > 0) {
            _char_lookahead_top--;
            c = _char_lookahead_stack[_char_lookahead_top];
            _char_length = _char_lookahead_stack_char_length[_char_lookahead_top];
        }
        else {
            // save the length so that if we unread a character it will restore
            // the length of the character we will have "re-exposed"
            _char_lookahead_stack_char_length[_char_lookahead_top] = _char_length;
            c = fetchchar();
        }
        // watch for end of lines to handle the offset processing
        if (c == '\n') {
            enqueue_offset(_offset);
            _offset = 0;
            _line++;
        }
        else {
            _offset++;
        }
        return c;
    }
    private final int fetchchar() throws IOException
    {
        int c, c2;
        _char_length = 0;

        // look for the 1 char peek ahead used to convert all
        // end of line sequences to the simple new line, this
        // means we need a 1 byte lookahead

        // FIXME the PEEKAHEAD logic can probably be replaced by
        //       use of unread, it does require some careful
        //       length analysis (making sure it's right)
        //       and all the should fall to ascii scanning anyway
        if (_peek_ahead_char == EMPTY_PEEKAHEAD) {
            c = _r.read();
            _char_length++;
        }
        else {
            c = _peek_ahead_char;
            _peek_ahead_char = EMPTY_PEEKAHEAD;
            _char_length++;
        }
        // first the common, easy, case - it's an ascii character
        // or an EOF "marker"
        if (c < 0) {
            _char_length = 0;
            return c;
        }
        if (c < 128) {
            // this includes -1 (eof) and characters in the ascii char set
            if (c == '\r') {
                c2 = _r.read();
                if (c2 != '\n') {
                    _peek_ahead_char = c2;
                }
                else {
                    _char_length++;
                }
                c = '\n';
            }
            // not needed
            //else if (c == '\n') {
            //    c2 = _r.read();
            //    if (c2 != '\r') {
            //        _peek_ahead_char = c2;
            //    }
            //    else {
            //        _char_length++;
            //    }
            //}
            return c;
        }
        else if ((c & (0x80 | 0x40 | 0x20)) == (0x80 | 0x40)) {
            // 2 byte unicode character >=128 and <= 0x7ff or <= 2047)
            // 110yyyyy 10zzzzzz
            c2 = _r.read();
            _char_length++;
            if ((c2 & (0x80 | 0x40)) == 0x80) bad_character();
            c = ((c & 0x1f) << 6) | (c2 & 0x3f);
        }
        else if ((c & (0x80 | 0x40 | 0x20 | 0x10)) == (0x80 | 0x40 | 0x20)) {
            // 3 byte unicode character >=2048 and <= 0xffff, <= 65535
            // 1110xxxx 10yyyyyy 10zzzzzz
            c2 = _r.read();
            _char_length++;
            if ((c2 & (0x80 | 0x40)) == 0x80) bad_character();
            int c3 = _r.read();
            _char_length++;
            if ((c3 & (0x80 | 0x40)) == 0x80) bad_character();
            c = ((c & 0x0f) << 12) | ((c2 & 0x3f) << 6) | (c3 & 0x3f);
            if (c >= 0x10000) {
                c2 = IonConstants.makeLowSurrogate(c);
                c  = IonConstants.makeHighSurrogate(c);
                preread_char(c2); // we'll put the low order bits in the queue for later
            }
        }
        else if ((c & (0x80 | 0x40 | 0x20 | 0x10 | 0x08)) == (0x80 | 0x40 | 0x20| 0x10)) {
            // 4 byte unicode character > 65535 (0xffff) and <= 2097151 <= 10xFFFFF
            // 11110www 10xxxxxx 10yyyyyy 10zzzzzz
            c2 = _r.read();
            _char_length++;
            if ((c2 & (0x80 | 0x40)) == 0x80) bad_character();
            int c3 = _r.read();
            _char_length++;
            if ((c3 & (0x80 | 0x40)) == 0x80) bad_character();
            int c4 = _r.read();
            _char_length++;
            if ((c4 & (0x80 | 0x40)) == 0x80) bad_character();
            c = ((c & 0x07) << 18) | ((c2 & 0x3f) << 12) | ((c3 & 0x3f) << 6) | (c4 & 0x3f);
            if (c >= 0x10000) {
                c2 = IonConstants.makeLowSurrogate(c);
                c  = IonConstants.makeHighSurrogate(c);
                preread_char(c2); // we'll put the low order bits in the queue for later
            }
        }
        else if (c != -1) {
            // at this point anything except EOF is a bad UTF-8 character
            bad_character();
        }
        return c;
    }

    public boolean isReallyDoubleBrace()
    {
        int c2;
        if (lookahead(0) != TOKEN_CLOSE_BRACE) return false;
        try {
            c2 = this.read_char();
        } catch (IOException e) {
            throw new IonTextReader.IonParsingException(e, this.input_position());
        }
        if (c2 < 0) {
            throw new UnexpectedEofException("Unterminated blob or clob");
        }
        if (c2 != '}') {
            this.unread_char(c2);
            return false;
        }
        return true;
    }
    private final void read_single_line_comment() throws IOException
    {
        int c;
        for (;;) {
            c = this.read_char();
            switch (c) {
                case '\n':
                    return;
                case -1:
                    return;
                default:
                    break;
            }
        }
    }

    private final void read_block_comment() throws IOException
    {
        int c;
        for (;;) {
            c = this.read_char();
            switch (c) {
                case '*':
                    // read back to back '*'s until you hit a '/' and terminate the comment
                    // or you see a non-'*'; in which case you go back to the outer loop.
                    // this just avoids the read-unread pattern on every '*' in a line of '*'
                    // commonly found at the top and bottom of block comments
                    for (;;) {
                        c = this.read_char();
                        if (c == '/') return;
                        if (c != '*') break;
                    }
                    break;
                case -1:
                    bad_token_start(c);
                default:
                    break;
            }
        }
    }

    private final int read_symbol(int c) throws IOException
    {
        int start, end;
        _saved_symbol.setLength(0);
        _saved_symbol.append((char)c);
        //_saved_symbol_len = 0;
        //saved_symbol_append((char)c);
        start = currentCharStart();
        setNextStart(start);

loop:   for (;;) {
            c = this.read_char();
            switch (c) {
            case 'a': case 'b': case 'c': case 'd': case 'e': case 'f':
            case 'g': case 'h': case 'j': case 'i': case 'k': case 'l':
            case 'm': case 'n': case 'o': case 'p': case 'q': case 'r':
            case 's': case 't': case 'u': case 'v': case 'w': case 'x':
            case 'y': case 'z':
            case 'A': case 'B': case 'C': case 'D': case 'E': case 'F':
            case 'G': case 'H': case 'J': case 'I': case 'K': case 'L':
            case 'M': case 'N': case 'O': case 'P': case 'Q': case 'R':
            case 'S': case 'T': case 'U': case 'V': case 'W': case 'X':
            case 'Y': case 'Z':
            case '$': case '_':
            case '0': case '1': case '2': case '3': case '4':
            case '5': case '6': case '7': case '8': case '9':
                _saved_symbol.append((char)c);
                //saved_symbol_append((char)c);
                break;
            default:
                this.unread_char(c);
                break loop;
            }
        }
        end = nextCharStart(); // we don't want the character that bumped us out but we've unread it already
        setNextEnd(end);
        _has_marked_symbol = true;
        _has_saved_symbol = true;
        return IonTextTokenizer.TOKEN_SYMBOL1;
    }
    private final int read_quoted_symbol(int c) throws IOException
    {
        int start, end;
        c = this.read_char();

        if (c == '\'') {
            c = this.read_char();
            if (c == '\'') {
                return read_quoted_long_string();
            }
            this.unread_char(c);
            return IonTextTokenizer.TOKEN_SYMBOL2;
        }

        // the position should always be correct here
        // since there's no reason to lookahead into a
        // quoted symbol
        start = currentCharStart();
        setNextStart(start);

loop:   for (;;) {
            switch (c) {
                case -1: unexpected_eof();
                case '\'':
                    end = currentCharStart();  // here we don't +1 because we are on the closing quote
                    setNextEnd(end);
                    _has_marked_symbol = true;
                    break loop;
                case '\\':
                    c = read_escaped_char();
                    break;
            }
            c = this.read_char();
        }

        return IonTextTokenizer.TOKEN_SYMBOL2;
    }

    private final int read_symbol_extended(int c) throws IOException
    {
        int start, end;
        _saved_symbol.setLength(0);
        _saved_symbol.append((char)c);
        //_saved_symbol_len = 0;
        //saved_symbol_append((char)c);
        start = currentCharStart();
        setNextStart(start);

loop:   for (;;) {
            c = this.read_char();
            switch (c) {
            //case 'a': case 'b': case 'c': case 'd': case 'e': case 'f':
            //case 'g': case 'h': case 'j': case 'i': case 'k': case 'l':
            //case 'm': case 'n': case 'o': case 'p': case 'q': case 'r':
            //case 's': case 't': case 'u': case 'v': case 'w': case 'x':
            //case 'y': case 'z':
            //case 'A': case 'B': case 'C': case 'D': case 'E': case 'F':
            //case 'G': case 'H': case 'J': case 'I': case 'K': case 'L':
            //case 'M': case 'N': case 'O': case 'P': case 'Q': case 'R':
            //case 'S': case 'T': case 'U': case 'V': case 'W': case 'X':
            //case 'Y': case 'Z':
            //case '$': case '_':
            //case '.': case '+': case '-':
            //case '0': case '1': case '2': case '3': case '4':
            //case '5': case '6': case '7': case '8': case '9':
            //case '~': case '`': case '!': case '@': case '#':
            //case '%': case '^': case '&': case '*':
            //case '=': case ';': case '?':
            //case '/': case '>': case '<': case '|': case '\\':

            case '.': case '+': case '-': case '/':
            case '<': case '>': case '*': case '=': case '^': case '&': case '|':
            case '~': case ';': case '!': case '?': case '@': case '%': case '`':
                _saved_symbol.append((char)c);
                //saved_symbol_append((char)c);
                break;
            default:
                this.unread_char(c);
                break loop;
            }
        }
        end = nextCharStart();  // we don't want the character that bumped us out but we "unread" it already
        setNextEnd(end);
        _has_marked_symbol = true;
        _has_saved_symbol = true;
        return IonTextTokenizer.TOKEN_SYMBOL3;
    }
    private final int read_escaped_char() throws IOException
    {
        int c = this.read_char();
        switch (c) {
        case '0':
            //    \u0000  \0  alert NUL
            c = '\u0000';
            break;
        case '\n':  // slash-new line the new line eater
            c = EMPTY_ESCAPE_SEQUENCE;
            break;
        case 'a':
            //    \u0007  \a  alert BEL
            c = '\u0007';
            break;
        case 'b':
            //    \u0008  \b  backspace BS
            c = '\u0008';
            break;
        case 't':
            //    \u0009  \t  horizontal tab HT
            c = '\u0009';
            break;
        case 'n':
            //    \ u000A  \ n  linefeed LF
            c = '\n';
            break;
        case 'f':
            //    \u000C  \f  form feed FF
            c = '\u000C';
            break;
        case 'r':
            //    \ u000D  \ r  carriage return CR
            c = '\r';
            break;
        case 'v':
            //    \u000B  \v  vertical tab VT
            c = '\u000B';
            break;
        case '"':
            //    \u0022  \"  double quote
            c = '\u0022';
            break;
        case '\'':
            //    \u0027  \'  single quote
            c = '\'';
            break;
        case '?':
            //    \u003F  \?  question mark
            c = '\u003F';
            break;
        case '\\':
            //    \u005C  \\  backslash
            c = '\\';
            break;
        case '/':
            //    \u002F  \/  forward slash nothing  \NL  escaped NL expands to nothing
            c = '\u002F';
            break;
        case 'x':
            //    any  \xHH  2-digit hexadecimal unicode character equivalent to \ u00HH
            c = read_hex_escape_value(2);
            break;
        case 'u':
            //    any  \ uHHHH  4-digit hexadecimal unicode character
            c = read_hex_escape_value(4);
            break;
        case 'U':
            //    any  \ UHHHHHHHH  8-digit hexadecimal unicode character
            c = read_hex_escape_value(8);
            break;
        case -1:
            throw new UnexpectedEofException();
        default:
            bad_escape_sequence();
        }
        return c;
    }

    private final int read_hex_escape_value(int len) throws IOException
    {
        int c2, hexchar = 0;
        while (len > 0) {
            len--;
            int c = this.read_char();
            if (c < 0 || c > 127) bad_escape_sequence();
            int d = hexValue[c];
            if (d < 0) bad_escape_sequence();
            hexchar = hexchar * 16 + d;
        }
        if (hexchar >= 0x10000) {
            c2 = IonConstants.makeLowSurrogate(hexchar);
            hexchar  = IonConstants.makeHighSurrogate(hexchar);
            preread_char(c2); // we'll put the low order bits in the queue for later
        }
        else if (IonConstants.isSurrogate(hexchar)) {
            bad_character();
        }
        return hexchar;
    }
    private final int read_quoted_string(int c) throws IOException
    {
        int start, end;
        // the position should always be correct here
        // since there's no reason to lookahead into a
        // quoted symbol
        start = this.nextCharStart(); // cas 19 mar 2008 removed: - _char_length;
        setNextStart(start);
        boolean has_big_char = false;
        while ((c = this.read_char()) != '"') {
            switch (c) {
            case -1:   unexpected_eof();
            case '\n': bad_token();
            case '\\':
                c = read_escaped_char();
                break;
            }
            if (c > 255) has_big_char = true;
        }
        end = currentCharStart();  // no +1 we're on the closing quote
        setNextEnd(end);
        _has_marked_symbol = true;

        return has_big_char ? IonTextTokenizer.TOKEN_STRING1 : IonTextTokenizer.TOKEN_STRING3;
    }
    private final int read_quoted_long_string( ) throws IOException
    {
        int start, end, c;

        // the position should always be correct here
        // since there's no reason to lookahead into a
        // quoted symbol
        start = nextCharStart();
        setNextStart(start);

        boolean has_big_char = false;
loop:   for (;;) {
            c = this.read_char();
            switch (c) {
            case -1: unexpected_eof();
            case '\'':
                end = currentCharStart(); // not +1 we're on the closing quote
                setNextEnd(end);
                c = this.read_char();
                if (c == '\'') {
                    c = this.read_char();
                    if (c == '\'') {
                        break loop;
                    }
                    this.unread_char(c);
                }
                this.unread_char(c);
                break;
            case '\\':
                c = read_escaped_char();
                break;
            }
            if (c > 255) has_big_char = true;
        }
        _has_marked_symbol = true;

        return has_big_char ? IonTextTokenizer.TOKEN_STRING2 : IonTextTokenizer.TOKEN_STRING4;
    }

    private final int read_number(int c) throws IOException
    {
        boolean has_sign = false;
        int start, end, t;

        // this reads int, float, decimal and timestamp strings
        // anything staring with a +, a - or a digit
        //case '0': case '1': case '2': case '3': case '4':
        //case '5': case '6': case '7': case '8': case '9':
        //case '-': case '+':

        // we've already read the first character so we have
        // to back it out in saving our starting position
        has_sign = ((c == '-') || (c == '+'));
        start = currentCharStart();
        setNextStart(start);

        // first leading digit - to look for hex and
        // to make sure that there is at least 1 digit (or
        // this isn't really a number
        if (has_sign) {
            // if there is a sign character, we just consume it
            // here and get whatever is next in line
            c = this.read_char();
        }
        if (!Character.isDigit(c)) {
            // if it's not a digit, this isn't a number
            // the only non-digit it could have been was a
            // sign character, and we'll have read past that
            // by now
            return IonTextTokenizer.TOKEN_ERROR;
        }

        // the first digit is a special case
        boolean starts_with_zero = (c == '0');
        if (starts_with_zero) {
            // if it's a leading 0 check for a hex value
            int c2 = this.read_char();
            if (c2 == 'x' || c2 == 'X') {
                t = read_hex_value(has_sign);
                return t;
            }
            // not a next value, back up and try again
            this.unread_char(c2);
        }

        // leading digits
        for (;;) {
            c = this.read_char();
            if (!Character.isDigit(c)) break;
        }

        if (c == '.') {
            // so it's probably a float of some sort (unless we change our minds below)
            // but ... if it started with a 0 we only allow that when it's
            // only the single digit (sign not withstanding)
            if (starts_with_zero
                && ( (currentCharStart() - start) != (has_sign ? 2 : 1) )) {
                bad_token();
            }

            // now it's a decimal or a float
            // read the "fraction" digits
            for (;;) {
                c = this.read_char();
                if (!Character.isDigit(c)) break;
            }
            t = IonTextTokenizer.TOKEN_DECIMAL;
        }
        else if (c == '-') {
            // this better be a timestamp and it starts with a 4 digit
            // year followed by a dash and no leading sign
            if (has_sign) bad_token();
            if (currentCharStart() - start != 4) bad_token();
            t = read_timestamp(read_timestamp_get_year(start));
            return t;
        }
        else {
            // to it's probably an int (unless we change our minds below)
            // but ... if it started with a 0 we only allow that when it's
            // only the single digit (sign not withstanding)
            if (starts_with_zero
                && ((currentCharStart() - start) != (has_sign ? 2 : 1))
            ) {
                bad_token();
            }
            t = IonTextTokenizer.TOKEN_INT;
        }

        // see if we have an exponential as in 2d+3
        if (c == 'e' || c == 'E') {
            t = IonTextTokenizer.TOKEN_FLOAT;
            c = read_exponent();  // the unused lookahead char
        }
        else if (c == 'd' || c == 'D') {
            t = IonTextTokenizer.TOKEN_DECIMAL;
            c = read_exponent();
        }

        // all forms of numeric need to stop someplace rational
        if (!isValueTerminatingCharacter(c)) bad_token();

        // we read off the end of the number, so put back
        // what we don't want, but what ever we have is an int
        end = currentCharStart();  // and we don't want to include it in start:end
        setNextEnd(end);
        this.unread_char(c);
        _has_marked_symbol = true;
        return t;
    }
    // this returns the lookahead character it didn't use so the caller
    // can unread it
    final int read_exponent() throws IOException
    {
        int c = read_char();
        if (c == '-' || c == '+') {
            c = read_char();
        }
        while (Character.isDigit(c)) {
            c = read_char();
        }
        if (c != '.') {
            return c;
        }
        while (Character.isDigit(c)) {
            c = read_char();
        }
        return c;
    }
    final int read_timestamp_get_year(int start) throws IOException {
        int pos = _r.position();
        _r.setPosition(start);

        int year = 0;
        for (int ii=0; ii<4; ii++) {
            int c = _r.read();
            year *= 10;
            year += c - '0'; // we already check for digits before we got here
        }

        _r.setPosition(pos);
        return year;
    }
    final boolean read_timestamp_is_leap_year(int year) {
        boolean is_leap = false;
        if ((year & 0x3) == 0) {
            is_leap = true; // divisible by 4 generally is a leap year
            int topdigits = (year / 100);
            if (year - (topdigits * 100) == 0) {
                is_leap = false; // but mostly not on even centuries
                if ((topdigits & 0x3) == 0) is_leap = true; // but it's still a leap year if we divide by 400 evenly
            }
        }
        return is_leap;
    }
    final int read_timestamp(int year) throws IOException
    {
        int c, c1, c2;
        int end;

        // read month
        c1 = read_char();
        if (!Character.isDigit(c1)) bad_token();
        c2 = read_char();
        if (!Character.isDigit(c1)) bad_token();
        int month = (c1 - '0') * 10 + (c2 - '0');
        if (month < 1 || month > 12) bad_token();

        c = read_char();
        if (c != '-') bad_token();

        // read day
        c1 = read_char();
        if (!Character.isDigit(c1)) bad_token();
        c2 = read_char();
        if (!Character.isDigit(c1)) bad_token();
        c = read_char();

        /// now we validate the month values
        switch (c1) {
        case '0':
            if (c2 < '1' || c2 > '9') bad_token();
            break;
        case '1':
            if (c2 < '0' || c2 > '9') bad_token();
            break;
        case '2':
            if (c2 < '0' || c2 > '9') bad_token();
            // I guess we do try to figure out leap years here
            if (c2 == '9' && month == 2 && !read_timestamp_is_leap_year(year)) bad_token();
            break;
        case '3':
            if (month == 2) bad_token();
            if (c2 > '1') bad_token();
            if (c2 == '0') break;
            // c2 == '1'
            switch (month) {
            case  2: // feb
            case  4: // apr
            case  6: // jun
            case  9: // sept
            case 11: // nov
                bad_token();
            default:
                break;
            }
            break;
        default:
            bad_token();
        }

        // look for the 't', otherwise we're done (and happy about it)
        if (c == 'T') {

            // hour
            c1 = read_char();
            if (!Character.isDigit(c1)) bad_token();
            c2 = read_char();
            if (!Character.isDigit(c2)) bad_token();
            int tmp = (c1 - '0')*10 + (c2 - '0');
            if (tmp < 0 || tmp > 23) bad_token();
            c = read_char();
            if (c != ':') bad_token();

            // minutes
            c1 = read_char();
            if (!Character.isDigit(c1)) bad_token();
            c2 = read_char();
            if (!Character.isDigit(c2)) bad_token();
            tmp = (c1 - '0')*10 + (c2 - '0');
            if (tmp < 0 || tmp > 59) bad_token();
            c = read_char();
            if (c == ':') {
                // seconds are optional
                // and first we'll have the whole seconds
                c1 = read_char();
                if (!Character.isDigit(c1)) bad_token();
                c2 = read_char();
                if (!Character.isDigit(c2)) bad_token();
                tmp = (c1 - '0')*10 + (c2 - '0');
                if (tmp < 0 || tmp > 59) bad_token();
                c = read_char();
                if (c == '.') {
                    // then the optional fractional seconds
                    do {
                        c = read_char();
                    } while (Character.isDigit(c));
                }
            }

            // since we have a time, we have to have a timezone of some sort

            // the timezone offset starts with a '+' '-' 'Z' or 'z'
            if (c == 'z' || c == 'Z') {
                c = read_char(); // read ahead since we'll check for a valid ending in a bit
            }
            else  if (c != '+' && c != '-') {
                // some sort of offset is required with a time value
                // if it wasn't a 'z' (above) then it has to be a +/- hours { : minutes }
                bad_token();
            }
            else {
                // then ... hours of time offset
                c1 = read_char();
                if (!Character.isDigit(c1)) bad_token();
                c2 = read_char();
                if (!Character.isDigit(c2)) bad_token();
                tmp = (c1 - '0')*10 + (c2 - '0');
                if (tmp < 0 || tmp > 23) bad_token();
                c = read_char();
                if (c != ':') {
                    // those hours need their minutesif it wasn't a 'z' (above) then it has to be a +/- hours { : minutes }
                    bad_token();
                }
                // and finally the *not* optional minutes of time offset
                c1 = read_char();
                if (!Character.isDigit(c1)) bad_token();
                c2 = read_char();
                if (!Character.isDigit(c2)) bad_token();
                tmp = (c1 - '0')*10 + (c2 - '0');
                if (tmp < 0 || tmp > 59) bad_token();
                c = read_char();
            }
        }
        // make sure we ended on a reasonable "note"
        if (!isValueTerminatingCharacter(c)) bad_token();

        // now do the "paper work" to close out a valid value
        end = currentCharStart(); // and we don't want to include the next character in start:end
        setNextEnd(end);
        this.unread_char(c);
        _has_marked_symbol = true;
        return IonTextTokenizer.TOKEN_TIMESTAMP;
    }

    final int read_hex_value(boolean has_sign) throws IOException
    {
        int c;

        // read the hex digits
        for (;;) {
            c = read_char();
            if (c < 0
                || (c < isHexDigit.length && !isHexDigit[c])) {
                break;
            }
        }

        // all forms of numeric need to stop someplace rational
        if (!isValueTerminatingCharacter(c)) bad_token(c);

        int end = currentCharStart(); // and we don't want to include it in start:end
        setNextEnd(end);
        this.unread_char(c);
        _has_marked_symbol = true;
        // we have to do this later because of the optional sign _start += has_sign ? 1 : 2; //  skip over the "0x" (they're ascii so 2 is correct)
        return IonTextTokenizer.TOKEN_HEX;
    }


    private final void bad_character()
    {
        throw new IonTextReader.IonParsingException("invalid UTF-8 sequence encountered"+input_position());
    }
    private final void unexpected_eof()
    {
        throw new UnexpectedEofException(input_position());
    }
    private final void bad_escape_sequence()
    {
        throw new IonTextReader.IonParsingException("bad escape character encountered"+input_position());
    }
    private final void bad_token_start(int c)
    {
        // TODO improve error message, print the codepoint
        throw new IonTextReader.IonParsingException("bad character ["+c+"] encountered where a token was supposed to start"+input_position());
    }
    private final void bad_token()
    {
        throw new IonTextReader.IonParsingException("a bad character was encountered in a token"+input_position());
    }
    private final void bad_token(int c)
    {
        String charStr = printCodePointAsString(c);
        throw new IonTextReader.IonParsingException("a bad character " + charStr + " was encountered in a token"+input_position());
    }
    /*
    public final static class xxxBufferedStream extends InputStream
    {
        byte [] _buffer;
        int     _len;
        int     _pos;

        public BufferedStream(byte[] buffer)
        {
            _buffer = buffer;
            _pos = 0;
            _len = buffer.length;
        }

        public final int getByte(int pos) {
            if (pos < 0 || pos >= _len) return -1;
            return _buffer[pos] & 0xff;
        }

        @Override
        public final int read()
            throws IOException
        {
            if (_pos >= _len) return -1;
            return _buffer[_pos++];
        }

        @Override
        public final int read(byte[] bytes, int offset, int len) throws IOException
        {
            int copied = 0;
            if (offset < 0 || offset > _len) throw new IllegalArgumentException();
            copied = len;
            if (_pos + len > _len) copied = _len - _pos;
            System.arraycopy(_buffer, _pos, bytes, offset, copied);
            _pos += copied;
            return copied;
        }

        public final int position() {
            return _pos;
        }

        public final BufferedStream setPosition(int pos)
        {
            if (_pos < 0 || _pos > _len) throw new IllegalArgumentException();
            _pos = pos;
            return this;
        }
    }
*/

    String consumeTokenAsString()
    {
        String value = getValueAsString();
        consumeToken();
        return value;
    }
    int getByte(int pos) {
        int b = _r.getByte(pos);
        return b;
    }
    String getValueAsString() {
        int start = peekTokenStart(0);
        int end   = peekTokenEnd(0);
        return getValueAsString(start, end);
    }
    String getValueAsString(int start, int end)
    {
        int pos = startValueAsString();
        continueValueAsString(start, end);
        String value = closeValueAsString(pos);
        return value;
    }


    /*
    private char[] _saved_symbol_buf = new char[32];
    private int    _saved_symbol_len = 0;
    private final void saved_symbol_grow(int needed) {
        int len;
        for (len = _saved_symbol_buf.length; len < needed; len *= 2);
        char[] temp = new char[len];
        System.arraycopy(_saved_symbol_buf, 0, temp, 0, _saved_symbol_len);
        _saved_symbol_buf = temp;
    }
    private final void saved_symbol_append(char c) {
        if (_saved_symbol_len >= _saved_symbol_buf.length) {
            saved_symbol_grow(_saved_symbol_len + 1);
        }
        _saved_symbol_buf[_saved_symbol_len++] = c;
    }
    */

    int startValueAsString() {
        int pos = _r.position();
        _saved_symbol.setLength(0);
        //_saved_symbol_len = 0;
        return pos;
    }
    void continueValueAsString(int start, int end) {
        int lookahead = IonTextTokenizer.EMPTY_PEEKAHEAD;
        _r.setPosition(start);
        //if (_saved_symbol_len + end - start > _saved_symbol_buf.length) {
        //    saved_symbol_grow(_saved_symbol_len + end - start);
        //}
        try {
            while (lookahead != IonTextTokenizer.EMPTY_PEEKAHEAD || _r.position() < end) {
                int c;
                if (lookahead == IonTextTokenizer.EMPTY_PEEKAHEAD) {
                    c = _r.read();
                }
                else {
                    c = lookahead;
                    lookahead = IonTextTokenizer.EMPTY_PEEKAHEAD;
                }
                if (c == '\n') {
                    c = (_r.position() < end) ? _r.read() : IonTextTokenizer.EMPTY_PEEKAHEAD;
                    if (c != '\r') {
                        lookahead = c;
                    }
                    c = '\n';
                }
                else if (c == '\r') {
                    c = (_r.position() < end) ? _r.read() : IonTextTokenizer.EMPTY_PEEKAHEAD;
                    if (c != '\n') {
                        lookahead = c;
                    }
                    c = '\n';
                }
                else if (c == '\\') {
                    c = read_escaped_char_in_string();
                    if (c == EMPTY_ESCAPE_SEQUENCE) continue;
                    if (IonConstants.isSurrogate(c)) {
                        bad_character();
                    }
                    else if (c >= 0x10000) {
                        int c2 = IonConstants.makeLowSurrogate(c);
                        c  = IonConstants.makeHighSurrogate(c);
                        _saved_symbol.append((char)c2);
                        //saved_symbol_append((char)c2);
                    }
                }
                _saved_symbol.append((char)c);
                //saved_symbol_append((char)c);
            }
        }
        catch (IOException e){
            throw new IonTextReader.IonParsingException(e
                    , " getting string value in tokenizer"+input_position());
        }
    }

    private final int read_escaped_char_in_string() throws IOException
    {
        int c = _r.read();
        switch (c) {
        case '0':
            //    \u0000  \0  alert NUL
            c = '\u0000';
            break;
        case '\r':
            int pos = _r.position();
            c = _r.read();
            if (c != '\n') {
                // if it's not a <CR><NL> we back up since we are not
                // going to consume more then the <CR> in this case
                _r.setPosition(pos);
            }
            else {
                // do nothing, we eat a <nl> after the <CR> as the
                // DOS new line character sequence
            }
            // we eat backslash new line sequences
            c = EMPTY_ESCAPE_SEQUENCE;
            break;
        case '\n':  // slash-new line the new line eater
            c = EMPTY_ESCAPE_SEQUENCE;
            break;
        case 'a':
            //    \u0007  \a  alert BEL
            c = '\u0007';
            break;
        case 'b':
            //    \u0008  \b  backspace BS
            c = '\u0008';
            break;
        case 't':
            //    \u0009  \t  horizontal tab HT
            c = '\u0009';
            break;
        case 'n':
            //    \ u000A  \ n  linefeed LF
            c = '\n';
            break;
        case 'f':
            //    \u000C  \f  form feed FF
            c = '\u000C';
            break;
        case 'r':
            //    \ u000D  \ r  carriage return CR
            c = '\r';
            break;
        case 'v':
            //    \u000B  \v  vertical tab VT
            c = '\u000B';
            break;
        case '"':
            //    \u0022  \"  double quote
            c = '\u0022';
            break;
        case '\'':
            //    \u0027  \'  single quote
            c = '\'';
            break;
        case '?':
            //    \u003F  \?  question mark
            c = '\u003F';
            break;
        case '\\':
            //    \u005C  \\  backslash
            c = '\\';
            break;
        case '/':
            //    \u002F  \/  forward slash nothing  \NL  escaped NL expands to nothing
            c = '\u002F';
            break;
        case 'x':
            //    any  \xHH  2-digit hexadecimal unicode character equivalent to \ u00HH
            c = read_hex_escape_in_string(2);
            break;
        case 'u':
            //    any  \ uHHHH  4-digit hexadecimal unicode character
            c = read_hex_escape_in_string(4);
            break;
        case 'U':
            //    any  \ UHHHHHHHH  8-digit hexadecimal unicode character
            c = read_hex_escape_in_string(8);
            break;
        default:
            bad_escape_sequence();
        }
        return c;
    }
    private final int read_hex_escape_in_string(int len) throws IOException
    {
        int hexchar = 0;
        while (len > 0) {
            len--;
            int c = _r.read();
            if (c < 0 || c > 127) bad_escape_sequence();
            int d = hexValue[c];
            if (d < 0) bad_escape_sequence();
            hexchar = hexchar * 16 + d;
        }
        return hexchar;
    }


    String closeValueAsString(int pos) {
        String value = _saved_symbol.toString();
        //String value = new String(_saved_symbol_buf, 0, _saved_symbol_len);
        _r.setPosition(pos);
        return value;
    }

    void append_character(int unicodeScalar) {
        int c2 = 0;
        if (unicodeScalar >= 0x10000) {
            c2 = IonConstants.makeLowSurrogate(unicodeScalar);
            unicodeScalar = IonConstants.makeHighSurrogate(unicodeScalar);
        }
        _saved_symbol.append((char)unicodeScalar);
        //saved_symbol_append((char)unicodeScalar);

        // this odd construct is in place to prepare for converting _saved_symbol
        // to a locally managed character array instead of the more expensive
        // stringbuffer
        if (c2 != 0) {
            _saved_symbol.append((char)c2);
            //saved_symbol_append((char)c2);
        }
    }

    int currentToken() {
        return this.peekTokenToken(0);
    }

    int keyword(int start_word, int end_word)
    {
        if (start_word >= end_word) {
            throw new IonTextReader.IonParsingException("start past end getting keyword (internal error)"+input_position());
        }
        int c = _r.getByte(start_word);
        int len = end_word - start_word; // +1 but we build that into the constants below
        switch (c) {
        case 'b':
            if (len == 4) {
                if (_r.getByte(start_word+1) == 'o'
                 && _r.getByte(start_word+2) == 'o'
                 && _r.getByte(start_word+3) == 'l'
                ) {
                    return KEYWORD_BOOL;
                }
                if (_r.getByte(start_word+1) == 'l'
                 && _r.getByte(start_word+2) == 'o'
                 && _r.getByte(start_word+3) == 'b'
                ) {
                    return KEYWORD_BLOB;
                }
            }
            return -1;
        case 'c':
            if (len == 4) {
                if (_r.getByte(start_word+1) == 'l'
                 && _r.getByte(start_word+2) == 'o'
                 && _r.getByte(start_word+3) == 'b'
                ) {
                    return KEYWORD_CLOB;
                }
            }
            return -1;
        case 'd':
            if (len == 7) {
                if (_r.getByte(start_word+1) == 'e'
                 && _r.getByte(start_word+2) == 'c'
                 && _r.getByte(start_word+3) == 'i'
                 && _r.getByte(start_word+4) == 'm'
                 && _r.getByte(start_word+5) == 'a'
                 && _r.getByte(start_word+6) == 'l'
                ) {
                    return KEYWORD_DECIMAL;
                }
            }
            return -1;
        case 'f':
            if (len == 5) {
                if (_r.getByte(start_word+1) == 'a'
                 && _r.getByte(start_word+2) == 'l'
                 && _r.getByte(start_word+3) == 's'
                 && _r.getByte(start_word+4) == 'e'
                ) {
                    return KEYWORD_FALSE;
                }
                if (_r.getByte(start_word+1) == 'l'
                 && _r.getByte(start_word+2) == 'o'
                 && _r.getByte(start_word+3) == 'a'
                 && _r.getByte(start_word+4) == 't'
                ) {
                    return KEYWORD_FLOAT;
                }
            }
            return -1;
        case 'i':
            if (len == 3) {
                if (_r.getByte(start_word+1) == 'n'
                 && _r.getByte(start_word+2) == 't'
                ) {
                    return KEYWORD_INT;
                }
            }
            return -1;
        case 'l':
            if (len == 4) {
                if (_r.getByte(start_word+1) == 'i'
                 && _r.getByte(start_word+2) == 's'
                 && _r.getByte(start_word+3) == 't'
                ) {
                    return KEYWORD_LIST;
                }
            }
            return -1;
        case 'n':
            if (len == 4) {
                if (_r.getByte(start_word+1) == 'u'
                 && _r.getByte(start_word+2) == 'l'
                 && _r.getByte(start_word+3) == 'l'
                ) {
                    return KEYWORD_NULL;
                }
            }
            return -1;
        case 's':
            if (len == 4) {
                if (_r.getByte(start_word+1) == 'e'
                 && _r.getByte(start_word+2) == 'x'
                 && _r.getByte(start_word+3) == 'p'
                ) {
                    return KEYWORD_SEXP;
                }
            }
            else if (len == 6) {
                if (_r.getByte(start_word+1) == 't'
                 && _r.getByte(start_word+2) == 'r'
                ) {
                    if (_r.getByte(start_word+3) == 'i'
                     && _r.getByte(start_word+4) == 'n'
                     && _r.getByte(start_word+5) == 'g'
                    ) {
                        return KEYWORD_STRING;
                    }
                    if (_r.getByte(start_word+3) == 'u'
                     && _r.getByte(start_word+4) == 'c'
                     && _r.getByte(start_word+5) == 't'
                    ) {
                        return KEYWORD_STRUCT;
                    }
                    return -1;
                }
                if (_r.getByte(start_word+1) == 'y'
                 && _r.getByte(start_word+2) == 'm'
                 && _r.getByte(start_word+3) == 'b'
                 && _r.getByte(start_word+4) == 'o'
                 && _r.getByte(start_word+5) == 'l'
                ) {
                    return KEYWORD_SYMBOL;
                }
            }
            return -1;
        case 't':
            if (len == 4) {
                if (_r.getByte(start_word+1) == 'r'
                 && _r.getByte(start_word+2) == 'u'
                 && _r.getByte(start_word+3) == 'e'
                ) {
                    return KEYWORD_TRUE;
                }
            }
            else if (len == 9) {
                if (_r.getByte(start_word+1) == 'i'
                 && _r.getByte(start_word+2) == 'm'
                 && _r.getByte(start_word+3) == 'e'
                 && _r.getByte(start_word+4) == 's'
                 && _r.getByte(start_word+5) == 't'
                 && _r.getByte(start_word+6) == 'a'
                 && _r.getByte(start_word+7) == 'm'
                 && _r.getByte(start_word+8) == 'p'
                ) {
                    return KEYWORD_TIMESTAMP;
                }
            }
            return -1;
        default:
            return -1;
        }
    }
}
