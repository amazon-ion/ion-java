/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.streaming;

import com.amazon.ion.IonException;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.NoSuchElementException;

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
    
    public static final int TOKEN_EOF     =  0;

    public static final int TOKEN_INT     =  1;
    public static final int TOKEN_DECIMAL =  2;
    public static final int TOKEN_FLOAT   =  3;
    public static final int TOKEN_TIMESTAMP= 4;
    public static final int TOKEN_SYMBOL1 =  5; // java identifier
    public static final int TOKEN_SYMBOL2 =  6; // single quoted string
    public static final int TOKEN_SYMBOL3 =  7; // operator sequence for sexp
    public static final int TOKEN_STRING1 =  8; // string in double quotes (")
    public static final int TOKEN_STRING2 =  9; // part of a string in triple quotes (''')
    
    public static final int TOKEN_DOT     = 10;
    public static final int TOKEN_COMMA   = 11;
    public static final int TOKEN_COLON   = 12;
    public static final int TOKEN_DOUBLE_COLON = 13;

    public static final int TOKEN_OPEN_PAREN   = 14;
    public static final int TOKEN_CLOSE_PAREN  = 15;
    public static final int TOKEN_OPEN_BRACE   = 16;
    public static final int TOKEN_CLOSE_BRACE  = 17;
    public static final int TOKEN_OPEN_SQUARE  = 18;
    public static final int TOKEN_CLOSE_SQUARE = 19;
    public static final int TOKEN_OPEN_DOUBLE_BRACE = 20;
    public static final int TOKEN_CLOSE_DOUBLE_BRACE = 21;

    public static final int TOKEN_MAX = 21;

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
        case TOKEN_EOF: return "TOKEN_EOF";
        case TOKEN_INT: return "TOKEN_INT";
        case TOKEN_DECIMAL: return "TOKEN_DECIMAL";
        case TOKEN_FLOAT: return "TOKEN_FLOAT";
        case TOKEN_TIMESTAMP: return "TOKEN_TIMESTAMP";
        case TOKEN_SYMBOL1: return "TOKEN_SYMBOL1";
        case TOKEN_SYMBOL2: return "TOKEN_SYMBOL2";
        case TOKEN_SYMBOL3: return "TOKEN_SYMBOL3";
        case TOKEN_STRING1: return "TOKEN_STRING1";
        case TOKEN_STRING2: return "TOKEN_STRING2";
        
        case TOKEN_DOT: return "TOKEN_DOT";
        case TOKEN_COMMA: return "TOKEN_COMMA";
        case TOKEN_COLON: return "TOKEN_COLON";
        case TOKEN_DOUBLE_COLON: return "TOKEN_DOUBLE_COLON";
        
        case TOKEN_OPEN_PAREN: return "TOKEN_OPEN_PAREN";
        case TOKEN_CLOSE_PAREN: return "TOKEN_CLOSE_PAREN";
        case TOKEN_OPEN_BRACE: return "TOKEN_OPEN_BRACE";
        case TOKEN_CLOSE_BRACE: return "TOKEN_CLOSE_BRACE";
        case TOKEN_OPEN_SQUARE: return "TOKEN_OPEN_SQUARE";
        case TOKEN_CLOSE_SQUARE: return "TOKEN_CLOSE_SQUARE";
        case TOKEN_OPEN_DOUBLE_BRACE: return "TOKEN_OPEN_DOUBLE_BRACE";
        case TOKEN_CLOSE_DOUBLE_BRACE: return "TOKEN_CLOSE_DOUBLE_BRACE";
        default: return "<invalid token "+t+">";
        }        
    }

    static boolean[] isBase64Characer = makeBase64Array();
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

    static int[] hexValue = makeHexArray();
    static int[] makeHexArray() {
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
    
    IonTextBufferedStream _r = null;
    
    int              _token = -1; 
    int              _start = -1; 
    int              _end = -1;
    
    static final int _token_lookahead_size = 8;
    int              _token_lookahead_current;
    int              _token_lookahead_next_available;
    int[]            _token_lookahead_start = new int[_token_lookahead_size];
    int[]            _token_lookahead_end   = new int[_token_lookahead_size];
    int[]            _token_lookahead_token = new int[_token_lookahead_size];
    
    StringBuffer     _saved_symbol = new StringBuffer();
    boolean          _has_saved_symbol = false;
    boolean          _has_marked_symbol = false;

    int              _char_lookahead_top = 0;
    int[]            _char_lookahead_stack = new int[5];
    
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
        other._start = _start;
        other._end   = _end;

        other._char_lookahead_top = this._char_lookahead_top;
        System.arraycopy(this._char_lookahead_stack, 0
                        ,other._char_lookahead_stack, 0
                        ,this._char_lookahead_top);

        other._r = this._r;
        // we deal with "saved position in get_saved_copy and restore_state" 
        // not here: _saved_position = this._r.position();

        other._has_marked_symbol = this._has_marked_symbol;
        other._has_saved_symbol = this._has_saved_symbol;
        other._saved_symbol.setLength(0);
        other._saved_symbol.append(this._saved_symbol);

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
        _start = -1; 
        _end = -1;
    }

    final void enqueueToken(int token, int start, int end) {
        int next = _token_lookahead_next_available;
        _token_lookahead_token[next] = token;
        _token_lookahead_start[next] = start;
        _token_lookahead_end[next]   = end;
        next++;
        if (next == _token_lookahead_size) next = 0;
        if (next == _token_lookahead_current) {
            throw new IllegalStateException("queue is full");
        }
        _token_lookahead_next_available = next;
        
        // if (_token_counter_on) {
        //    _tokens_read++;
        //    if (_tokens_read == 4) {
        //        _tokens_read = _tokens_read + 1 - 1;        
        //    }
        // }
    }
    

    
    final void dequeueToken() {
        if (_token_lookahead_current == _token_lookahead_next_available) {
            throw new NoSuchElementException();
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

    public final int readBase64String() throws IOException
    {
        _start = _r.position();
        
        for (;;) {
            int c = this.read_char();
            if (c < 0 || c > 127) break;
            if (isBase64Characer[c]) continue;
            if (Character.isWhitespace(c)) continue;
            break; // we break whenever we run out of good base64 stuff
        }
        _end = _r.position() - 1;
        _has_marked_symbol = true;
        return _end - _start;
    }
    public final int lookahead(int distance)
    {
        if (distance < 0 || distance >= IonTextTokenizer._token_lookahead_size) {
            throw new IllegalArgumentException();
        }
        if (distance >= queueCount()) {
            try {
                while (distance >= queueCount()) {
                    fill_queue();
                }
            }
            catch (IOException e) {
                throw new UndeclaredThrowableException(e);
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
                break;
            case '\n':
                c2 = this.read_char();
                if (c2 != '\r') {
                    this.unread_char(c2);
                }
                break;
            case '\r':
                c2 = this.read_char();
                if (c2 != '\n') {
                    this.unread_char(c2);
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
                c2 = this.read_char();
                if (c2 != '}') {
                    this.unread_char(c2);
                    t = IonTextTokenizer.TOKEN_CLOSE_BRACE;
                }
                else {
                    t = IonTextTokenizer.TOKEN_CLOSE_DOUBLE_BRACE;
                }
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
                t = IonTextTokenizer.TOKEN_COMMA;
                break loop;
            case '.':
                t = IonTextTokenizer.TOKEN_DOT;
                break loop;
            case '\'':
                t = read_quoted_symbol(c);
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
            case '-': case '+':
                t = read_number(c);
                break loop;
            default:
                bad_token_start();
            }
        }
        
        // really we just enqueue this, we don't care about it otherwise
        this.enqueueToken(t, _start, _end);
        return;
    }

    private final void unread_char(int c) {
        _char_lookahead_stack[_char_lookahead_top++] = c;
    }
    private final int read_char() throws IOException
    {
        if (_char_lookahead_top > 0) {
            _char_lookahead_top--;
            return _char_lookahead_stack[_char_lookahead_top];
        }
        return fetchchar();
    }
    private final int fetchchar() throws IOException { 
        int c = _r.read();
        // first the common, easy, case - it's an ascii character
        // or an EOF "marker"
        if (c < 128) {
            // this includes -1 (eof) and characters in the ascii char set
            return c;
        }
        else if ((c & (0x80 | 0x40 | 0x20)) == (0x80 | 0x40)) {
            // 2 byte unicode character >=128 and <= 0x7ff or <= 2047)
            // 110yyyyy 10zzzzzz
            int c2 = _r.read();
            if ((c2 & (0x80 | 0x40)) == 0x80) bad_character();
            c = ((c & 0x1f) << 6) | (c2 & 0x3f);
        }
        else if ((c & (0x80 | 0x40 | 0x20 | 0x10)) == (0x80 | 0x40 | 0x20)) {
            // 3 byte unicode character >=2048 and <= 0xffff, <= 65535
            // 1110xxxx 10yyyyyy 10zzzzzz
            int c2 = _r.read();
            if ((c2 & (0x80 | 0x40)) == 0x80) bad_character();
            int c3 = _r.read();
            if ((c3 & (0x80 | 0x40)) == 0x80) bad_character();
            c = ((c & 0x0f) << 12) | ((c2 & 0x3f) << 6) | (c3 & 0x3f);
            if (c >= 0x10000) {
                c2 = (c & 0x3ff) | 0xDC00;
                c = ((c >> 10) & 0x3ff) | 0xD800 ;
                unread_char(c2); // we'll put the low order bits in the queue for later
                c = c3;
            }
        }
        else if ((c & (0x80 | 0x40 | 0x20 | 0x10 | 0x08)) == (0x80 | 0x40 | 0x20| 0x10)) {
            // 4 byte unicode character > 65535 (0xffff) and <= 2097151 <= 10xFFFFF
            // 11110www 10xxxxxx 10yyyyyy 10zzzzzz
            int c2 = _r.read();
            if ((c2 & (0x80 | 0x40)) == 0x80) bad_character();
            int c3 = _r.read();
            if ((c3 & (0x80 | 0x40)) == 0x80) bad_character();
            int c4 = _r.read();
            if ((c4 & (0x80 | 0x40)) == 0x80) bad_character();
            c = ((c & 0x07) << 18) | ((c2 & 0x3f) << 12) | ((c3 & 0x3f) << 6) | (c4 & 0x3f);
            if (c >= 0x10000) {
                c2 = (c & 0x3ff) | 0xDC00;
                c = ((c >> 10) & 0x3ff) | 0xD800 ;
                unread_char(c2); // we'll put the low order bits in the queue for later
                c = c3;
            }
        }
        else if (c != -1) {
            // at this point anything except EOF is a bad UTF-8 character
            bad_character();
        }
        return c;
    }

    private final int read_symbol(int c) throws IOException
    {
        _saved_symbol.setLength(0);
        _saved_symbol.append((char)c);
        _start = _r.position() - 1;
        
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
                break;
            default:
                this.unread_char(c);
                break loop;
            }
        }
        _end = _r.position() - 1;
        _has_marked_symbol = true;
        _has_saved_symbol = true;
        return IonTextTokenizer.TOKEN_SYMBOL1;
    }
    private final int read_quoted_symbol(int c) throws IOException
    {
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
        _start = _r.position();
        
loop:   for (;;) {
            switch (c) {
                case -1: unexpected_eof();
                case '\'':
                    _end = _r.position() - 1;
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
    private final int read_escaped_char() throws IOException
    {
        int c = this.read_char();
        switch (c) {
        case '0':
            //    \u0000  \0  alert NUL
            c = '\u0000';
            break;
        case 'a':
            //    \u0007  \a  alert BEL
            c = '\u0007';
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
        default:
            bad_escape_sequence();
        }
        return c;
    }

    private final int read_hex_escape_value(int len) throws IOException
    {
        int hexchar = 0;
        while (len > 0) {
            len--;
            int c = this.read_char();
            if (c < 0 || c > 127) bad_escape_sequence();
            int d = hexValue[c];
            if (d < 0) bad_escape_sequence();
            hexchar = hexchar * 16 + d;
        }
        return hexchar;
    }
    private final int read_quoted_string(int c) throws IOException
    {
        // the position should always be correct here
        // since there's no reason to lookahead into a
        // quoted symbol
        _start = _r.position();
        
        while ((c = this.read_char()) != '"') {
            switch (c) {
            case -1: unexpected_eof();
            case '\\':
                c = read_escaped_char();
                break;
            }
        }
        _end = _r.position() - 1;
        _has_marked_symbol = true;
        
        return IonTextTokenizer.TOKEN_STRING1;
    }
    private final int read_quoted_long_string( ) throws IOException
    {
        int c;
        
        // the position should always be correct here
        // since there's no reason to lookahead into a
        // quoted symbol
        _start = _r.position();
        
loop:   for (;;) {
            c = this.read_char();
            switch (c) {
            case -1: unexpected_eof();
            case '\'':
                _end = _r.position() - 1;
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
        }
        _has_marked_symbol = true;
        return IonTextTokenizer.TOKEN_STRING2;
    }
    
    private final int read_number(int c) throws IOException
    {
        boolean has_sign = false;
        
        // this reads int, float, decimal and timestamp strings
        // anything staring with a +, a - or a digit
        //case '0': case '1': case '2': case '3': case '4': 
        //case '5': case '6': case '7': case '8': case '9':
        //case '-': case '+':

        // we've already read the first character so we have
        // to back it out in saving our starting position
        _start = _r.position() - 1;
        has_sign = ((c == '-') || (c == '+'));
        
        // leading digits
        for (;;) {
            c = this.read_char();
            if (!Character.isDigit(c)) break;
        }
        
        if (c == '.') {
            // now it's a decimal or a float
            // fraction digits
            for (;;) {
                c = this.read_char();
                if (!Character.isDigit(c)) break;
            }
            int t = IonTextTokenizer.TOKEN_DECIMAL;
            if (c == 'e' || c == 'E') {
                t = IonTextTokenizer.TOKEN_FLOAT;
                read_exponent();
            }
            else if (c == 'd' || c == 'D') {
                read_exponent();
            }
            // we have to unread the non-digit the broke the loop above
            // or we have to unread the character the ended the exponent
            unread_char(c);
            _end = _r.position() - 1;
            _has_marked_symbol = true;
            return t;
        }
        else if (c == '-') {
            // this better be a timestamp
            if (has_sign) bad_token();
            read_timestamp();
            return IonTextTokenizer.TOKEN_TIMESTAMP;
        }
        
        // we read off the end of the number, so put back
        // what we don't want, but what ever we have is an int
        this.unread_char(c);
        _end = _r.position() - 1;
        _has_marked_symbol = true;
        return IonTextTokenizer.TOKEN_INT;
    }
    final void read_exponent() throws IOException
    {
        int c = read_char();
        if (c == '-' || c == '+') {
            c = read_char();
        }
        while (Character.isDigit(c)) {
            c = read_char();
        }
        if (c != '.') {
            return;
        }
        while (Character.isDigit(c)) {
            c = read_char();
        }
        return;
    }
    final void read_timestamp() throws IOException
    {
        int c;
        
        // read month
        c = read_char();
        if (!Character.isDigit(c)) bad_token();
        c = read_char();
        if (Character.isDigit(c)) {
            c = read_char();
        }
        if (c != '-') bad_token();
        
        // read day
        c = read_char();
        if (!Character.isDigit(c)) bad_token();
        c = read_char();
        if (Character.isDigit(c)) {
            c = read_char();
        }
        // look for the 't', otherwise we're done (and happy about it)
        if (c == 'T') {
        
            // hour
            c = read_char();
            if (!Character.isDigit(c)) bad_token();
            c = read_char();
            if (Character.isDigit(c)) {
                c = read_char();
            }
            if (c != ':') bad_token();
            
            // minutes
            c = read_char();
            if (!Character.isDigit(c)) bad_token();
            c = read_char();
            if (Character.isDigit(c)) {
                c = read_char();
            }
            if (c == ':') {
                // seconds are optional 
                // and first we'll have the whole seconds
                c = read_char();
                if (!Character.isDigit(c)) bad_token();
                c = read_char();
                if (Character.isDigit(c)) {
                    c = read_char();
                }
                if (c == '.') {
                    // then the optional fractional seconds
                    do {
                        c = read_char();
                    } while (Character.isDigit(c));
                }
            }
        }
        // the timezone offset isn't optional
        // now + - or z
        if (c == 'z') {
            _end = _r.position();
            _has_marked_symbol = true;
            return; // done
        }
        if (c != '+' && c != '-') bad_token();
        // then ... hours of time offset
        c = read_char();
        if (!Character.isDigit(c)) bad_token();
        c = read_char();
        if (Character.isDigit(c)) {
            c = read_char();
        }
        if (c == ':') {
            // and finally the optional minutes of time offset
            c = read_char();
            if (!Character.isDigit(c)) bad_token();
            c = read_char();
            if (Character.isDigit(c)) {
                c = read_char();
                if (Character.isDigit(c)) bad_token();  // only 2 digits in the minutes me bucko
            }
        }
        this.unread_char(c);
        _end = _r.position() - 1;
        _has_marked_symbol = true;
    }
        
    private final void bad_character() throws IOException 
    {
        throw new IOException("invalid UTF-8 sequence encountered");
    }
    private final void unexpected_eof() throws IOException 
    {
        throw new IOException("unexpected EOF encountered");
    }
    private final void bad_escape_sequence() throws IOException 
    {
        throw new IOException("bad escape character encountered");
    }
    private final void bad_token_start() throws IOException 
    {
        throw new IOException("bad character encountered where a token was supposed to start");
    }
    private final void bad_token() throws IOException 
    {
        throw new IOException("a bad character was encountered in a token");
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
    
    // TODO:
    String consumeTokenAsString()
    {
        String value = getValueAsString();
        consumeToken();
        return value;
    }
    String getValueAsString() {
        int start = peekTokenStart(0);
        int end   = peekTokenEnd(0);
        return getValueAsString(start, end);
    }
    String getValueAsString(int start, int end) 
    {
        int pos = _r.position();
        _r.setPosition(start);
        _saved_symbol.setLength(0);
        try {
            while (_r.position() < end) {
                int c = _r.read();
                _saved_symbol.append((char)c);
            }
        }
        catch (IOException e){
            throw new IonException(e);
        }
        String value = _saved_symbol.toString();
        _r.setPosition(pos);
        
        return value;
    }

    void scanBase64Value() {
        throw new RuntimeException("E_NOT_IMPL");
    }
    int getValueStart() {
        return this.peekTokenStart(0);
    }
    int getValueEnd() {
        return this.peekTokenEnd(0);
    }
    int currentToken() {
        return this.peekTokenToken(0);
    }
    
    int keyword(int start_word, int end_word)
    {
        if (start_word >= end_word) throw new IllegalArgumentException();
        int c = _r.getByte(start_word);
        int len = end_word - start_word; // +1 but we build that into the constants below
        switch (c) {
        case 'b':
            if (len == 3) {
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
            if (len == 3) {
                if (_r.getByte(start_word+1) == 'l'
                 && _r.getByte(start_word+2) == 'o'
                 && _r.getByte(start_word+3) == 'b'
                ) {
                    return KEYWORD_CLOB;
                }
            }
            return -1;
        case 'd':
            if (len == 6) {
                if (_r.getByte(start_word+1) == 'e'
                 && _r.getByte(start_word+2) == 'd'
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
            if (len == 4) {
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
            if (len == 2) {
                if (_r.getByte(start_word+1) == 'n'
                 && _r.getByte(start_word+2) == 't'
                ) {
                    return KEYWORD_INT;
                }
            }
            return -1;
        case 'l':
            if (len == 3) {
                if (_r.getByte(start_word+1) == 'i'
                 && _r.getByte(start_word+2) == 's'
                 && _r.getByte(start_word+3) == 't'
                ) {
                    return KEYWORD_LIST;
                }
            }
            return -1;
        case 'n':
            if (len == 3) {
                if (_r.getByte(start_word+1) == 'u'
                 && _r.getByte(start_word+2) == 'l'
                 && _r.getByte(start_word+3) == 'l'
                ) {
                    return KEYWORD_NULL;
                }
            }
            return -1;
        case 's':
            if (len == 3) {
                if (_r.getByte(start_word+1) == 'e'
                 && _r.getByte(start_word+2) == 'x'
                 && _r.getByte(start_word+3) == 'p'
                ) {
                    return KEYWORD_SEXP;
                }
            }
            else if (len == 5) {
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
            if (len == 3) {
                if (_r.getByte(start_word+1) == 'r'
                 && _r.getByte(start_word+2) == 'u'
                 && _r.getByte(start_word+3) == 'e'
                ) {
                    return KEYWORD_TRUE;
                }
            }
            else if (len == 8) {
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
