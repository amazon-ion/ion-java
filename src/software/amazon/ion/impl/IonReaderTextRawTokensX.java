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

import static software.amazon.ion.impl.IonTokenConstsX.CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_1;
import static software.amazon.ion.impl.IonTokenConstsX.CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_2;
import static software.amazon.ion.impl.IonTokenConstsX.CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_3;
import static software.amazon.ion.impl.IonTokenConstsX.CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_1;
import static software.amazon.ion.impl.IonTokenConstsX.CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_2;
import static software.amazon.ion.impl.IonTokenConstsX.CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_3;
import static software.amazon.ion.util.IonTextUtils.printCodePointAsString;

import java.io.IOException;
import software.amazon.ion.IonException;
import software.amazon.ion.IonType;
import software.amazon.ion.UnexpectedEofException;
import software.amazon.ion.impl.IonTokenConstsX.CharacterSequence;
import software.amazon.ion.impl.UnifiedSavePointManagerX.SavePoint;
import software.amazon.ion.util.IonTextUtils;

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
 *
 *  This is a copy and paste from IonTextTokenize on the introduction of
 *  the new input abstraction IonInputStream as the source of characters
 *  and bytes for the reader.
 *
 *  This variation does NOT make local copies of the tokens.  It does
 *  start "marking" at the beginning of the token and the end.  The stream
 *  will buffer the input until the mark is released.
 *
 *  The result is that only the most recent token is available to the
 *  calling reader.
 *
 */
final class IonReaderTextRawTokensX
{
    static final boolean _debug = false;

    private static final Appendable NULL_APPENDABLE = new Appendable()
    {
        public Appendable append(CharSequence csq) throws IOException
        {
            return this;
        }

        public Appendable append(CharSequence csq, int start, int end)
            throws IOException
        {
            return this;
        }

        public Appendable append(char c) throws IOException
        {
            return this;
        }
    };

    static final int   BASE64_EOF = 128; // still a byte, not -1, none of the low 6 bits on
    static final int[] BASE64_CHAR_TO_BIN = Base64Encoder.Base64EncodingCharToInt;
    static final int   BASE64_TERMINATOR_CHAR = Base64Encoder.Base64EncodingTerminator;

    private UnifiedInputStreamX  _stream = null;
    private int                 _token = -1;
    /** are we at the beginning of this token (false == done with it) */
    private boolean             _unfinished_token;
    private long                _line_count;
    private long                _line_starting_position;
    private boolean             _line_count_has_cached = false;
    private long                _line_count_cached;
    private long                _line_offset_cached;

    /** number of base64 decoded bytes in the stack, used to decode base64 */
    private int                 _base64_prefetch_count;
    /**
     * since this "stack" will only 0-2 bytes deep, we'll just shift them
     * into an int
     */
    private int                 _base64_prefetch_stack;


    /**
     * IonTokenReader constructor requires a UnifiedInputStream
     * as the source of bytes/chars that serve as the basic input
     *
     * @param iis wrapped input stream
     */
    public IonReaderTextRawTokensX(UnifiedInputStreamX iis) {
        this(iis, 1, 1);
    }

    public IonReaderTextRawTokensX(UnifiedInputStreamX iis, long starting_line,
                                   long starting_column)
    {
        _stream = iis;
        _line_count = starting_line;
        _line_starting_position = _stream.getPosition() - starting_column;
    }

    public void close()
        throws IOException
    {
        _stream.close();
    }

    public int  getToken()      { return _token; }
    public long getLineNumber() { return _line_count; }
    public long getLineOffset() {
        long stream_position = _stream.getPosition();
        long offset = stream_position - _line_starting_position;
        return offset;
    }

    UnifiedInputStreamX getSourceStream() { return this._stream; }

    public final boolean isBufferedInput()
    {
        boolean is_buffered = ! _stream._is_stream;
        return is_buffered;
    }

    protected String input_position() {
        String s = " at line "
                + getLineNumber()
                + " offset "
                + getLineOffset();
        return s;
    }
    public final boolean isUnfinishedToken() { return  _unfinished_token; }

    public final void tokenIsFinished() {
        _unfinished_token = false;
        _base64_prefetch_count = 0;
    }

    //
    //  character routines to fetch characters and
    //  handle look ahead and line counting and such
    //
    protected final int read_char() throws IOException
    {
        int c = _stream.read();
        if (c == '\r' || c == '\n') {
            c = line_count(c);
        }
        return c;
    }

    /**
     * NOT for use outside of string/symbol/clob!
     * Absorbs backslash-NL pairs, returning
     * {@link #CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_1} etc.
     */
    protected final int read_string_char(ProhibitedCharacters prohibitedCharacters) throws IOException
    {
        int c = _stream.read();
        if (prohibitedCharacters.includes(c)) {
            error("invalid character [" + printCodePointAsString(c) + "]");
        }
        // the c == '\\' clause will cause us to eat ALL slash-newlines
        if (c == '\r' || c == '\n' || c == '\\') {
            c = line_count(c);
        }
        return c;
    }

    private final void unread_char(int c)
    {
        if (c < 0) {
            switch (c) {
            case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_1:
                line_count_unread(c);
                _stream.unread('\n');
                break;
            case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_2:
                line_count_unread(c);
                _stream.unread('\r');
                break;
            case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_3:
                line_count_unread(c);
                _stream.unread('\n');
                _stream.unread('\r');
                break;
            case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_1:
                _stream.unread('\n');
                _stream.unread('\\');
                break;
            case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_2:
                _stream.unread('\r');
                _stream.unread('\\');
                break;
            case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_3:
                _stream.unread('\n');
                _stream.unread('\r');
                _stream.unread('\\');
                break;
            case UnifiedInputStreamX.EOF:
                _stream.unread(UnifiedInputStreamX.EOF);
                break;
            default:
                assert false
                    : "INVALID SPECIAL CHARACTER ENCOUNTERED: " + c;
            }
        }
        else  {
            _stream.unread(c);
        }
    }

    private final int line_count_unread(int c) {
        assert( c == CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_1
             || c == CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_2
             || c == CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_3
             || c == CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_1
             || c == CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_2
             || c == CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_3
        );
        if (_line_count_has_cached) {
            _line_count = _line_count_cached;
            _line_starting_position = _line_offset_cached;
            _line_count_has_cached = false;
        }
        return c;
    }
    private final int line_count(int c) throws IOException
    {
        // check for the slash new line case (and we'l
        // consume both here it that's what we find
        switch (c) {
        case '\\':
            {
                int c2 = _stream.read();
                switch (c2) {
                case '\r':  // DOS <cr><lf>  or old Mac <cr>
                    int c3 = _stream.read();
                    if (c3 != '\n') {
                        unread_char(c3);
                        c = CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_2;
                    }
                    else {
                        c = CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_3;
                    }
                    break;
                case '\n':
                    // Unix and new Mac (also Unix) <lf>
                    c = CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_1;
                    break;
                default:
                    // not a slash new line, so we'll just return the slash
                    // leave it to be handled elsewhere
                    unread_char(c2);
                    return c;
                }
            }
            break;
        case '\r':
            {
                // convert '\r' or '\r\n' into the appropriate CHAR_SEQ
                // pseudo character
                int c2 = _stream.read();
                if (c2 == '\n') {
                    c = CHAR_SEQ_NEWLINE_SEQUENCE_3;
                }
                else {
                    unread_char(c2);
                    c = CHAR_SEQ_NEWLINE_SEQUENCE_2;
                }
            }
            break;
        case '\n':
            c = CHAR_SEQ_NEWLINE_SEQUENCE_1;
            break;
        default:
            throw new IllegalStateException();
        }

        // before we adjust the line count we save it so that
        // we can recover from a unread of a line terminator
        // note that we can only recover from a single line
        // terminator unread, but that should be enough.  We
        // only unread whitespace if it's a delimiter, and
        // then we only have to unread a single instance.
        _line_count_cached = _line_count;
        _line_offset_cached = _line_starting_position;
        _line_count_has_cached = true;

        // anything else (and that should only be either a new line
        // of IonTokenConsts.ESCAPED_NEWLINE_SEQUENCE passed in) we will
        // return the char unchanged and line count
        _line_count++;
        // since we want the first character of the line to be 1, not 0:
        _line_starting_position = _stream.getPosition() - 1;

        return c;
    }

    /**
     * peeks into the input stream to see if the next token
     * would be a double colon.  If indeed this is the case
     * it skips the two colons and returns true.  If not
     * it unreads the 1 or 2 real characters it read and
     * return false.
     * It always consumes any preceding whitespace.
     * @return true if the next token is a double colon, false otherwise
     * @throws IOException
     */
    public final boolean skipDoubleColon() throws IOException
    {
        int c = skip_over_whitespace();
        if (c != ':') {
            unread_char(c);
            return false;
        }
        c = read_char();
        if (c != ':') {
            unread_char(c);
            unread_char(':');
            return false;
        }
        return true;
    }


    /**
     * peeks into the input stream to see if we have an
     * unquoted symbol that resolves to one of the ion
     * types.  If it does it consumes the input and
     * returns the type keyword id.  If not is unreads
     * the non-whitespace characters and the dot, which
     * the input argument 'c' should be.
     */
    public final int peekNullTypeSymbol() throws IOException
    {
        // the '.' has to follow the 'null' immediately
        int c = read_char();
        if (c != '.') {
            unread_char(c);
            return IonTokenConstsX.KEYWORD_none;
        }

        // we have a dot, start reading through the following non-whitespace
        // and we'll collect it so that we can unread it in the event
        // we don't actually see a type name
        int[] read_ahead = new int[IonTokenConstsX.TN_MAX_NAME_LENGTH + 1];
        int read_count = 0;
        int possible_names = IonTokenConstsX.KW_ALL_BITS;

        while (read_count < IonTokenConstsX.TN_MAX_NAME_LENGTH + 1) {
            c = read_char();
            read_ahead[read_count++] = c;
            int letter_idx = IonTokenConstsX.typeNameLetterIdx(c);
            if (letter_idx < 1) {
                if (IonTokenConstsX.isValidTerminatingCharForInf(c)) {
                    // it's not a letter we care about but it is
                    // a valid end of const, so maybe we have a keyword now
                    // we always exit the loop here since we look
                    // too far so any letter is invalid at pos 10
                    break;
                }
                return peekNullTypeSymbolUndo(read_ahead, read_count);
            }
            int mask = IonTokenConstsX.typeNamePossibilityMask(read_count - 1, letter_idx);
            possible_names &= mask;
            if (possible_names == 0) {
                // in this case it can't be a valid keyword since
                // it has identifier chars (letters) at 1 past the
                // last possible end (at least)
                return peekNullTypeSymbolUndo(read_ahead, read_count);
            }
        }
        // now lets get the keyword value from our bit mask
        // at this point we can fail since we may have hit
        // a valid terminator before we're done with all key
        // words.  We even have to check the length.
        // for example "in)" matches both letters to the
        // typename int and terminates validly - but isn't
        // long enough, but with length we have enough to be sure
        // with the actual type names we're using in 1.0
        int kw = IonTokenConstsX.typeNameKeyWordFromMask(possible_names, read_count-1);
        if (kw == IonTokenConstsX.KEYWORD_unrecognized) {
            peekNullTypeSymbolUndo(read_ahead, read_count);
        }
        else {
            // since we're accepting the rest we aren't unreading anything
            // else - but we still have to unread the character that stopped us
            unread_char(c);
        }
        return kw;
    }
    private final int peekNullTypeSymbolUndo(int[] read_ahead, int read_count)
    {
        String type_error = "";
        for (int ii=0; ii<read_count; ii++) {
            // this (string concatenation) is horrible, but we're about throw anyway
            type_error += (char)read_ahead[ii];
        }

        String message = "invalid type name on a typed null value";
        error(message); // this throws so we won't actually return
        return IonTokenConstsX.KEYWORD_unrecognized;
    }

    /**
     * peeks into the input stream to see what non-whitespace
     * character is coming up.  If it is a double quote or
     * a triple quote this returns true as either distinguished
     * the contents of a lob as distinctly a clob.  Otherwise
     * it returns false.
     * In either case it unreads whatever non-whitespace it read
     * to decide.
     * @return true if the next token is a double or triple quote, false otherwise
     * @throws IOException
     */
    public final int peekLobStartPunctuation() throws IOException
    {
        int c = skip_over_lob_whitespace();
        if (c == '"') {
            //unread_char(c);
            return IonTokenConstsX.TOKEN_STRING_DOUBLE_QUOTE;
        }
        if (c != '\'') {
            unread_char(c);
            return IonTokenConstsX.TOKEN_ERROR;
        }
        c = read_char();
        if (c != '\'') {
            unread_char(c);
            unread_char('\'');
            return IonTokenConstsX.TOKEN_ERROR;
        }
        c = read_char();
        if (c != '\'') {
            unread_char(c);
            unread_char('\'');
            unread_char('\'');
            return IonTokenConstsX.TOKEN_ERROR;
        }
        return IonTokenConstsX.TOKEN_STRING_TRIPLE_QUOTE;
    }

    /** Expects optional whitespace then }} */
    protected final void skip_clob_close_punctuation() throws IOException {
        int c = skip_over_clob_whitespace();
        if (c == '}') {
            c = read_char();
            if (c == '}') {
                return;
            }
            unread_char(c);
            c = '}';
        }
        unread_char(c);
        error("invalid closing puctuation for CLOB");
    }


    protected final void finish_token(SavePoint sp) throws IOException
    {
        if (_unfinished_token) {
            int c = skip_to_end(sp);
            unread_char(c);
            _unfinished_token = false;
        }
    }

    private final int skip_to_end(SavePoint sp)  throws IOException
    {
        int c;

        // FIXME lots of inconsistency here!
        // Sometimes the token's first character is still on the stream,
        // sometimes it's already been consumed.

        switch (_token) {
        case IonTokenConstsX.TOKEN_UNKNOWN_NUMERIC:
            c = skip_over_number(sp);
            break;
        case IonTokenConstsX.TOKEN_INT:
            c = skip_over_int(sp);
            break;
        case IonTokenConstsX.TOKEN_HEX:
            c = skipOverRadix(sp, Radix.HEX);
            break;
        case IonTokenConstsX.TOKEN_BINARY:
            c = skipOverRadix(sp, Radix.BINARY);
            break;
        case IonTokenConstsX.TOKEN_DECIMAL:
            c = skip_over_decimal(sp);
            break;
        case IonTokenConstsX.TOKEN_FLOAT:
            c = skip_over_float(sp);
            break;
        case IonTokenConstsX.TOKEN_TIMESTAMP:
            c = skip_over_timestamp(sp);
            break;
        case IonTokenConstsX.TOKEN_SYMBOL_IDENTIFIER:
            c = skip_over_symbol_identifier(sp);
            break;
        case IonTokenConstsX.TOKEN_SYMBOL_QUOTED:
            // Initial single-quote has been consumed!
            assert(!is_2_single_quotes_helper());
            c = skip_single_quoted_string(sp);
            break;
        case IonTokenConstsX.TOKEN_SYMBOL_OPERATOR:
            // Initial operator char has NOT been consumed
            c = skip_over_symbol_operator(sp);
            break;
        case IonTokenConstsX.TOKEN_STRING_DOUBLE_QUOTE:
            skip_double_quoted_string_helper(); // FIXME Why no sp here?
            c = skip_over_whitespace();
            break;
        case IonTokenConstsX.TOKEN_STRING_TRIPLE_QUOTE:
            skip_triple_quoted_string(sp);
            c = skip_over_whitespace();
            break;

        case IonTokenConstsX.TOKEN_OPEN_DOUBLE_BRACE:
            // works just like a pair of nested structs
            // since "skip_over" doesn't care about formal
            // syntax (like requiring field names);
            skip_over_blob(sp);
            c = read_char();
            break;
        case IonTokenConstsX.TOKEN_OPEN_BRACE:
            assert( sp == null ); // you can't save point a scanned struct (right now anyway)
            skip_over_struct();
            c = read_char();
            break;
        case IonTokenConstsX.TOKEN_OPEN_PAREN:
            skip_over_sexp(); // you can't save point a scanned sexp (right now anyway)
            c = read_char();
            break;
        case IonTokenConstsX.TOKEN_OPEN_SQUARE:
            skip_over_list();  // you can't save point a scanned list (right now anyway)
            c = read_char();
            break;
        case IonTokenConstsX.TOKEN_DOT:
        case IonTokenConstsX.TOKEN_COMMA:
        case IonTokenConstsX.TOKEN_COLON:
        case IonTokenConstsX.TOKEN_DOUBLE_COLON:
        case IonTokenConstsX.TOKEN_CLOSE_PAREN:
        case IonTokenConstsX.TOKEN_CLOSE_BRACE:
        case IonTokenConstsX.TOKEN_CLOSE_SQUARE:
        case IonTokenConstsX.TOKEN_CLOSE_DOUBLE_BRACE:
        case IonTokenConstsX.TOKEN_ERROR:
        case IonTokenConstsX.TOKEN_EOF:
        default:
            c = -1; // makes eclipse happy
            error("token "+IonTokenConstsX.getTokenName(_token)+
                  " unexpectedly encounterd as \"unfinished\"");
            break;
        }
        if (IonTokenConstsX.isWhitespace(c)) {
            c = skip_over_whitespace();
        }
        _unfinished_token = false;
        return c;
    }

    public final long getStartingOffset() throws IOException
    {
        int c;
        if (_unfinished_token) {
            c = skip_to_end(null);
        }
        else {
            c = skip_over_whitespace();
        }
        unread_char(c);
        long pos = _stream.getPosition();
        return pos;
    }

    public final int nextToken() throws IOException
    {
        int t = -1;
        int c, c2;

        if (_unfinished_token) {
            c = skip_to_end(null);
        }
        else {
            c = skip_over_whitespace();
        }
        _unfinished_token = true;

        switch (c) {
        case -1:
            return next_token_finish(IonTokenConstsX.TOKEN_EOF, true);
        case '/':
            unread_char(c);
            return next_token_finish(IonTokenConstsX.TOKEN_SYMBOL_OPERATOR, true);
        case ':':
            c2 = read_char();
            if (c2 != ':') {
                unread_char(c2);
                return next_token_finish(IonTokenConstsX.TOKEN_COLON, true);
            }
            return next_token_finish(IonTokenConstsX.TOKEN_DOUBLE_COLON, true);
        case '{':
            c2 = read_char();
            if (c2 != '{') {
                unread_char(c2);
                return next_token_finish(IonTokenConstsX.TOKEN_OPEN_BRACE, true); // CAS: 9 nov 2009
            }
            return next_token_finish(IonTokenConstsX.TOKEN_OPEN_DOUBLE_BRACE, true);
        case '}':
            // detection of double closing braces is done
            // in the parser in the blob and clob handling
            // state - it's otherwise ambiguous with closing
            // two structs together. see tryForDoubleBrace() below
            return next_token_finish(IonTokenConstsX.TOKEN_CLOSE_BRACE, false);
        case '[':
            return next_token_finish(IonTokenConstsX.TOKEN_OPEN_SQUARE, true); // CAS: 9 nov 2009
        case ']':
            return next_token_finish(IonTokenConstsX.TOKEN_CLOSE_SQUARE, false);
        case '(':
            return next_token_finish(IonTokenConstsX.TOKEN_OPEN_PAREN, true); // CAS: 9 nov 2009
        case ')':
            return next_token_finish(IonTokenConstsX.TOKEN_CLOSE_PAREN, false);
        case ',':
            return next_token_finish(IonTokenConstsX.TOKEN_COMMA, false);
        case '.':
            c2 = read_char();
            unread_char(c2);
            if (IonTokenConstsX.isValidExtendedSymbolCharacter(c2)) {
                unread_char('.');
                return next_token_finish(IonTokenConstsX.TOKEN_SYMBOL_OPERATOR, true);
            }
            return next_token_finish(IonTokenConstsX.TOKEN_DOT, false);
        case '\'':
            if (is_2_single_quotes_helper()) {
                return next_token_finish(IonTokenConstsX.TOKEN_STRING_TRIPLE_QUOTE, true);
            }
            // unread_char(c);
            return next_token_finish(IonTokenConstsX.TOKEN_SYMBOL_QUOTED, true);
        case '+':
            if (peek_inf_helper(c)) // this will consume the inf if it succeeds
            {
                return next_token_finish(IonTokenConstsX.TOKEN_FLOAT_INF, false);
            }
            unread_char(c);
            return next_token_finish(IonTokenConstsX.TOKEN_SYMBOL_OPERATOR, true);
        case '#':
        case '<': case '>': case '*': case '=': case '^': case '&': case '|':
        case '~': case ';': case '!': case '?': case '@': case '%': case '`':
            unread_char(c);
            return next_token_finish(IonTokenConstsX.TOKEN_SYMBOL_OPERATOR, true);
        case '"':
            return next_token_finish(IonTokenConstsX.TOKEN_STRING_DOUBLE_QUOTE, true);
        case 'a': case 'b': case 'c': case 'd': case 'e': case 'f':
        case 'g': case 'h': case 'i': case 'j': case 'k': case 'l':
        case 'm': case 'n': case 'o': case 'p': case 'q': case 'r':
        case 's': case 't': case 'u': case 'v': case 'w': case 'x':
        case 'y': case 'z':
        case 'A': case 'B': case 'C': case 'D': case 'E': case 'F':
        case 'G': case 'H': case 'I': case 'J': case 'K': case 'L':
        case 'M': case 'N': case 'O': case 'P': case 'Q': case 'R':
        case 'S': case 'T': case 'U': case 'V': case 'W': case 'X':
        case 'Y': case 'Z':
        case '$': case '_':
            unread_char(c);
            return next_token_finish(IonTokenConstsX.TOKEN_SYMBOL_IDENTIFIER, true);
        case '0': case '1': case '2': case '3': case '4':
        case '5': case '6': case '7': case '8': case '9':
            t = scan_for_numeric_type(c);
            unread_char(c);
            return next_token_finish(t, true);
        case '-':
            // see if we have a number or what might be an extended symbol
            c2 = read_char();
            unread_char(c2);
            if (IonTokenConstsX.isDigit(c2)) {
                t = scan_negative_for_numeric_type(c);
                unread_char(c);
                return next_token_finish(t, true);
            }
            else if (peek_inf_helper(c)) // this will consume the inf if it succeeds
            {
                return next_token_finish(IonTokenConstsX.TOKEN_FLOAT_MINUS_INF, false);
            }
            else {
                unread_char(c);
                return next_token_finish(IonTokenConstsX.TOKEN_SYMBOL_OPERATOR, true);
            }
        default:
            bad_token_start(c); // throws
        }
        throw new IonException("invalid state: next token switch shouldn't exit");
    }
    private final int next_token_finish(int token, boolean content_is_waiting) {
        _token = token;
        _unfinished_token = content_is_waiting;
        return _token;
    }

    /**
     * Defines strategies to apply when comments are encountered.
     */
    private enum CommentStrategy
    {
        /**
         * Skip over all of the comment's text.
         */
        IGNORE
        {

            @Override
            boolean onComment(IonReaderTextRawTokensX tokenizer)
                throws IOException
            {
                int next = tokenizer.read_char();
                switch(next) {
                case '/':
                    tokenizer.skip_single_line_comment();
                    return true; // valid comment
                case '*':
                    tokenizer.skip_block_comment();
                    return true; // valid comment
                default:
                    tokenizer.unread_char(next);
                    return false; // invalid comment
                }
            }

        },
        /**
         * If it's a valid comment, throw an error.
         */
        ERROR
        {

            @Override
            boolean onComment(IonReaderTextRawTokensX tokenizer)
                throws IOException
            {
                int next = tokenizer.read_char();
                if (next == '/' || next == '*')
                {
                    tokenizer.error("Illegal comment");
                }
                else
                {
                    tokenizer.unread_char(next);
                }
                return false; // invalid comment
            }

        },
        /**
         * A '/' character has been found, so break the loop as it may be a valid blob character.
         */
        BREAK
        {

            @Override
            boolean onComment(IonReaderTextRawTokensX tokenizer)
                throws IOException
            {
                return false;
            }

        };

        /**
         * Called when positioned after the first '/'.
         * @return true if a valid comment was found, otherwise false
         * @throws IonReaderTextTokenException when the ERROR strategy encounters a comment
         */
        abstract boolean onComment(IonReaderTextRawTokensX tokenizer) throws IOException;
    }

    /**
     * Skip over any whitespace, ignoring any comments.
     * @return the next character in the stream
     * @throws IOException
     */
    private final int skip_over_whitespace() throws IOException
    {
        return skip_over_whitespace(CommentStrategy.IGNORE);
    }

    /**
     * Skip over any whitespace, applying the given CommentStrategy to
     * any comments found.
     * @param commentStrategy the strategy to use upon encountering comments.
     * @return the next character in the stream
     * @throws IOException
     */
    private final int skip_over_whitespace(CommentStrategy commentStrategy) throws IOException
    {
        skip_whitespace(commentStrategy);
        return read_char();
    }

    /**
     * The type of lob is not yet known. Break the loop on encountering
     * a / character and defer to the blob validation.
     * @return the next character in the stream
     * @throws IOException
     */
    private final int skip_over_lob_whitespace() throws IOException
    {
        return skip_over_blob_whitespace();
    }

    /**
     * Skip over whitespace, but not the / character, as it's a valid
     * Base64 character.
     * @return the next character in the stream
     * @throws IOException
     */
    private final int skip_over_blob_whitespace() throws IOException
    {
        return skip_over_whitespace(CommentStrategy.BREAK);
    }

    /**
     * Skip over the whitespace after the clob string and before the closing
     * braces. Throw if a comment is encountered.
     * @return the next character in the stream
     * @throws IOException
     */
    private final int skip_over_clob_whitespace() throws IOException
    {
        return skip_over_whitespace(CommentStrategy.ERROR);
    }

    /**
     * Skips whitespace and comments and finishes at the starting position
     * of the next token.
     * @return true if whitespace or comments were encountered
     * @throws IOException
     */
    protected final boolean skip_whitespace() throws IOException
    {
        return skip_whitespace(CommentStrategy.IGNORE);
    }

    /**
     * Skips whitespace and applies the given CommentStrategy to any comments
     * found. Finishes at the starting position of the next token.
     * @param commentStrategy
     * @return true if whitespace was skipped and/or comments ignored
     * @throws IOException
     */
    private final boolean skip_whitespace(CommentStrategy commentStrategy) throws IOException
    {
        boolean any_whitespace = false;
        int c;

        loop: for (;;) {
            c = read_char();
            switch (c) {
            case -1:
                break loop;
            case ' ':
            case '\t':
            // new line normalization and counting is handled in read_char
            case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_1:
            case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_2:
            case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_3:
            case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_1:
            case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_2:
            case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_3:
                any_whitespace = true;
                break;
            case '/':
                if (!commentStrategy.onComment(this))
                {
                    break loop;
                }
                any_whitespace = true;
                break;
            default:
                break loop;
            }
        }
        unread_char(c);
        return any_whitespace;
    }

    private final void skip_single_line_comment() throws IOException
    {
        for (;;) {
            int c = read_char();
            switch (c) {
            // new line normalization and counting is handled in read_char
            case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_1:
            case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_2:
            case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_3:
            case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_1:
            case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_2:
            case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_3:
                return;
            case -1:
                return;
            default:
                break; // and read another character
            }
        }
    }

    private final void skip_block_comment() throws IOException
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

    /**
     * this peeks ahead to see if the next two characters
     * are single quotes. this would finish off a triple
     * quote when the first quote has been read.
     * if it succeeds it "consumes" the two quotes
     * it reads.
     * if it fails it unreads
     * @return true if the next two characters are single quotes
     * @throws IOException
     */
    private final boolean is_2_single_quotes_helper() throws IOException
    {
        int c = read_char();
        if (c != '\'') {
            unread_char(c);
            return false;
        }
        c = read_char();
        if (c != '\'') {
            unread_char(c);
            unread_char('\'');
            return false;
        }
        return true;
    }

    private final boolean peek_inf_helper(int c) throws IOException
    {
        if (c != '+' && c != '-') return false;
        c = read_char();
        if (c == 'i') {
            c = read_char();
            if (c == 'n') {
                c = read_char();
                if (c == 'f') {
                    c = read_char();
                    if (is_value_terminating_character(c)) {
                        unread_char(c);
                        return true;
                    }
                    unread_char(c);
                    c = 'f';
                }
                unread_char(c);
                c = 'n';
            }
            unread_char(c);
            c = 'i';
        }
        unread_char(c);
        return false;
    }

    /**
     * we encountered a character that starts a number,
     * a digit or a dash (minus).  Now we'll scan a little
     * ways ahead to spot some of the numeric types.
     *
     * this only looks far enough (2 or 6 chars) to identify
     * hex and timestamps
     * it might encounter a decimal or a 'd' or an 'e' and
     * decide this token is float or decimal (or int if we
     * hit a non-numeric char) but it may return TOKEN_UNKNOWN_NUMERIC;
     *
     * if will unread everything it's read, and the character
     * passed in as the first digit encountered
     *
     * @param c first char of number read by caller
     * @return numeric token type
     * @throws IOException
     */
    private final int scan_for_numeric_type(int c1) throws IOException
    {
        int   t = IonTokenConstsX.TOKEN_UNKNOWN_NUMERIC;
        int[] read_chars = new int[6];
        int   read_char_count = 0;
        int   c;

        if (!IonTokenConstsX.isDigit(c1)) {
            error(String.format("Expected digit, got U+%04X", c1));
        }

        // the caller needs to unread this if they want to: read_chars[read_char_count++] = c1;

        c = read_char();
        read_chars[read_char_count++] = c;

        if (c1 == '0') {
            // check for hex
            switch(c) {
            case 'x':
            case 'X':
                t = IonTokenConstsX.TOKEN_HEX;
                break;
            case 'd':
            case 'D':
                t = IonTokenConstsX.TOKEN_DECIMAL;
                break;
            case 'e':
            case 'E':
                t = IonTokenConstsX.TOKEN_FLOAT;
                break;
            case 'b':
            case 'B':
                t = IonTokenConstsX.TOKEN_BINARY;
                break;
            case '.':
                // the decimal might have an 'e' somewhere down the line so we
                // don't really know the type here
                break;
            default:
                if (is_value_terminating_character(c)) {
                    t = IonTokenConstsX.TOKEN_INT;
                }
                break;
            }
        }
        if (t == IonTokenConstsX.TOKEN_UNKNOWN_NUMERIC) { // oh for goto :(
            if (IonTokenConstsX.isDigit(c)) { // 2nd digit
                // it might be a timestamp if we have 4 digits, a dash,
                // and a digit
                c = read_char();
                read_chars[read_char_count++] = c;
                if (IonTokenConstsX.isDigit(c)) { // digit 3
                    c = read_char();
                    read_chars[read_char_count++] = c;
                    if (IonTokenConstsX.isDigit(c)) {
                        // last digit of possible year
                        c = read_char();
                        read_chars[read_char_count++] = c;
                        if (c == '-' || c =='T') {
                            // we have dddd- or ddddT looks like a timestamp
                            // (or invalid input)
                            t = IonTokenConstsX.TOKEN_TIMESTAMP;
                        }
                    }
                }
            }
        }

        // unread whatever we read, including the passed in char
        do {
            read_char_count--;
            c = read_chars[read_char_count];
            unread_char(c);
        } while (read_char_count > 0);

        return t;
    }

    private final boolean is_value_terminating_character(int c)
        throws IOException
    {
        boolean isTerminator;

        switch (c) {
            case '/':
            // this is terminating only if it starts a comment of some sort
            c = read_char();
            unread_char(c);  // we never "keep" this character
            isTerminator = (c == '/' || c == '*');
            break;
        // new line normalization and counting is handled in read_char
        case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_1:
        case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_2:
        case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_3:
        case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_1:
        case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_2:
        case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_3:
            isTerminator = true;
            break;
        default:
            isTerminator = IonTextUtils.isNumericStop(c);
            break;
        }

        return isTerminator;
    }

    /**
     * variant of scan_numeric_type where the passed in
     * start character was preceded by a minus sign.
     * this will also unread the minus sign.
     *
     * @param c first char of number read by caller
     * @return numeric token type
     * @throws IOException
     */
    private final int scan_negative_for_numeric_type(int c) throws IOException
    {
        assert(c == '-');
        c = read_char();
        int t = scan_for_numeric_type(c);
        if (t == IonTokenConstsX.TOKEN_TIMESTAMP) {
            bad_token(c);
        }
        unread_char(c); // and the caller need to unread the '-'
        return t;
    }

    // TODO: need new test cases since stepping out over values
    //       (or next-ing over them) is quite different from
    //       fully parsing them.  It is generally more lenient
    //       and that may not be best.

    /**
     * this is used to load a previously marked set of bytes
     * into the StringBuilder without escaping.  It expects
     * the caller to have set a save point so that the EOF
     * will stop us at the right time.
     * This does handle UTF8 decoding and surrogate encoding
     * as the bytes are transfered.
     */
    protected void load_raw_characters(StringBuilder sb) throws IOException
    {
        int c = read_char();
        for (;;) {
            c = read_char();
            switch (c) {
            case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_1:
            case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_2:
            case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_3:
            // WAS: case IonTokenConstsX.ESCAPED_NEWLINE_SEQUENCE:
                continue;
            case -1:
                return;
            default:
                if (!IonTokenConstsX.is7bitValue(c)) {
                    c = read_large_char_sequence(c);
                }
            }
            if (IonUTF8.needsSurrogateEncoding(c)) {
                sb.append(IonUTF8.highSurrogate(c));
                c = IonUTF8.lowSurrogate(c);
            }
            sb.append((char)c);
        }
    }

    protected void skip_over_struct() throws IOException
    {
        skip_over_container('}');
    }
    protected void skip_over_list() throws IOException
    {
        skip_over_container(']');
    }
    protected void skip_over_sexp() throws IOException
    {
        skip_over_container(')');
    }
    private void skip_over_container(int terminator) throws IOException
    {
        assert( terminator == '}' || terminator == ']' || terminator == ')' );
        int c;

        for (;;) {
            c = skip_over_whitespace();
            switch (c) {
            case -1:
                unexpected_eof();
            case '}':
            case ']':
            case ')':
                if (c == terminator) { // no point is checking this on every char
                    return;
                }
                break;
            case '"':
                skip_double_quoted_string_helper();
                break;
            case '\'':
                if (is_2_single_quotes_helper()) {
                    skip_triple_quoted_string(null);
                }
                else {
                    c = skip_single_quoted_string(null);
                    unread_char(c);
                }
                break;
            case '(':
                skip_over_container(')');
                break;
            case '[':
                skip_over_container(']');
                break;
            case '{':
                // this consumes lobs as well since the double
                // braces count correctly and the contents
                // of either clobs or blobs will be just content
                c = read_char();
                if (c == '{') {
                    // 2nd '{' - it's a lob of some sort - let's find out what sort
                    c = skip_over_lob_whitespace();

                    int lobType;
                    if (c == '"') {
                        // clob, double quoted
                        lobType = IonTokenConstsX.TOKEN_STRING_DOUBLE_QUOTE;
                    }
                    else if (c == '\'') {
                        // clob, triple quoted - or error
                        if (!is_2_single_quotes_helper()) {
                            error("invalid single quote in lob content");
                        }
                        lobType = IonTokenConstsX.TOKEN_STRING_TRIPLE_QUOTE;
                    }
                    else {
                        // blob
                        unread_char(c);
                        lobType = IonTokenConstsX.TOKEN_OPEN_DOUBLE_BRACE;
                    }

                    skip_over_lob(lobType, null);
                }
                else if (c == '}') {
                    // do nothing, we just opened and closed an empty struct
                    // move on, there's nothing to see here ...
                }
                else {
                    unread_char(c);
                    skip_over_container('}');
                }
                break;
            default:
                break;
            }
        }
    }

    private int skip_over_number(SavePoint sp) throws IOException
    {
        int c = read_char();

        // first consume any leading 0 to get it out of the way
        if (c == '-') {
            c = read_char();
        }
        // could be a long int, a decimal, a float
        // it cannot be a hex or a valid timestamp
        // so scan digits - if decimal can more digits
        // if d or e eat possible sign
        // scan to end of digits
        c = skip_over_digits(c);
        if (c == '.') {
            c = read_char();
            c = skip_over_digits(c);
        }
        if (c == 'd' || c == 'D' || c == 'e' || c == 'E') {
            c = read_char();
            if (c == '-' || c == '+') {
                c = read_char();
            }
            c = skip_over_digits(c);
        }
        if (!is_value_terminating_character(c)) {
            bad_token(c);
        }
        if (sp != null) {
            sp.markEnd(-1);
        }
        return c;
    }
    private int skip_over_int(SavePoint sp) throws IOException
    {
        int c = read_char();
        if (c == '-') {
            c = read_char();
        }
        c = skip_over_digits(c);
        if (!is_value_terminating_character(c)) {
            bad_token(c);
        }
        if (sp != null) {
            sp.markEnd(-1);
        }
        return c;
    }
    private int skip_over_digits(int c) throws IOException
    {
        while (IonTokenConstsX.isDigit(c)) {
            c = read_char();
        }
        return c;
    }

    private int skipOverRadix(SavePoint sp, Radix radix) throws IOException
    {
        int c;

        c = read_char();
        if (c == '-') {
            c = read_char();
        }
        assert(c == '0');
        c = read_char();
        radix.assertPrefix(c);

        c = readNumeric(NULL_APPENDABLE, radix);

        if (!is_value_terminating_character(c)) {
            bad_token(c);
        }
        if (sp != null) {
            sp.markEnd(-1);
        }

        return c;
    }

    private int skip_over_decimal(SavePoint sp) throws IOException
    {
        int c = skip_over_number(sp);
        return c;
    }
    private int skip_over_float(SavePoint sp) throws IOException
    {
        int c = skip_over_number(sp);
        return c;
    }
    private int skip_over_timestamp(SavePoint sp) throws IOException
    {
        // we know we have dddd- or ddddT we don't know what follows
        // is should be dddd-mm
        int c = skip_timestamp_past_digits(4);
        if (c == 'T') {
            // yyyyT
            if (sp != null) {
                sp.markEnd(0);
            }
            return skip_over_whitespace(); // prefetch
        }
        if (c != '-') {
            error("invalid timestamp encountered");
        }
        // yyyy-mmT
        // yyyy-mm-ddT
        // yyyy-mm-ddT+hh:mm
        // yyyy-mm-ddThh:mm+hh:mm
        // yyyy-mm-ddThh:mm:ss+hh:mm
        // yyyy-mm-ddThh:mm:ss.dddd+hh:mm
        // yyyy-mm-ddThh:mmZ
        // yyyy-mm-ddThh:mm:ssZ
        // yyyy-mm-ddThh:mm:ss.ddddZ
        c = skip_timestamp_past_digits(2);
        if (c == 'T') {
            // yyyy-mmT
            if (sp != null) {
                sp.markEnd(0);
            }
            return skip_over_whitespace(); // prefetch
        }
        skip_timestamp_validate(c, '-');
        c = skip_timestamp_past_digits(2);
        if ( c != 'T' ) {
            return skip_timestamp_finish(c, sp);
        }
        c = read_char();
        if (!IonTokenConstsX.isDigit(c)) {
            // yyyy-mm-ddT
            return skip_timestamp_finish(skip_optional_timestamp_offset(c), sp);
        }
        // one hour digit already read above
        c = skip_timestamp_past_digits(1);
        if (c != ':') {
            bad_token(c);
        }
        c = skip_timestamp_past_digits(2);
        if (c != ':') {
            // yyyy-mm-ddThh:mm?
            return skip_timestamp_offset_or_z(c, sp);
        }
        c = skip_timestamp_past_digits(2);
        if (c != '.') {
            // yyyy-mm-ddThh:mm:ss?
            return skip_timestamp_offset_or_z(c, sp);
        }
        c = read_char();
        if (IonTokenConstsX.isDigit(c)) {
            c = skip_over_digits(c);
        }
        // yyyy-mm-ddThh:mm:ss.ddd?

        return skip_timestamp_offset_or_z(c, sp);
    }

    private int skip_timestamp_finish(int c, SavePoint sp) throws IOException {
        if (!is_value_terminating_character(c)) {
            bad_token(c);
        }
        if (sp != null) {
            sp.markEnd(-1);
        }
        return c;
    }
    private int skip_optional_timestamp_offset(int c) throws IOException
    {
        if (c == '-' || c == '+') {
            c = skip_timestamp_past_digits(2);
            if (c != ':') {
                bad_token( c );
            }
            c = skip_timestamp_past_digits(2);
        }
        return c;
    }
    private int skip_timestamp_offset_or_z(int c, SavePoint sp) throws IOException
    {
        if (c == '-' || c == '+') {
            c = skip_timestamp_past_digits(2);
            if (c != ':') {
                bad_token( c );
            }
            c = skip_timestamp_past_digits(2);
        }
        else if (c == 'Z' || c == 'z') {
            c = read_char();
        } else {
            bad_token(c);
        }
        return skip_timestamp_finish(c, sp);
    }
    private final void skip_timestamp_validate(int c, int expected) {
        if (c != expected) {
            error("invalid character '"+(char)c+
                  "' encountered in timestamp (when '"+(char)expected+
                  "' was expected");
        }
    }

    /**
     * Helper method for skipping embedded digits inside a timestamp value.
     * This overload skips exactly the number indicated, and errors if a
     * non-digit is encountered.
     */
    private final int skip_timestamp_past_digits(int len) throws IOException
    {
        // special case of the other overload
        return skip_timestamp_past_digits(len, len);
    }

    /**
     * Helper method for skipping embedded digits inside a timestamp value
     * This overload skips at least min and at most max digits, and errors
     * if a non-digit is encountered in the first min characters read
     */
    private final int skip_timestamp_past_digits(int min, int max)
        throws IOException
    {
        int c;

        // scan the first min characters insuring they're digits
        while (min > 0) {
            c = read_char();
            if (!IonTokenConstsX.isDigit(c)) {
                error("invalid character '"+(char)c+"' encountered in timestamp");
            }
            --min;
            --max;
        }
        // stop at the first non digit between min and max
        while (max > 0) {
            c = read_char();
            if (!IonTokenConstsX.isDigit(c)) {
                return c;
            }
            --max;
        }
        // max characters reached; stop
        return read_char();
    }
    protected IonType load_number(StringBuilder sb) throws IOException
    {
        boolean has_sign = false;
        int     t, c;

        // this reads int, float, decimal and timestamp strings
        // anything staring with a +, a - or a digit
        //case '0': case '1': case '2': case '3': case '4':
        //case '5': case '6': case '7': case '8': case '9':
        //case '-': case '+':

        //start_pos = _stream.getPosition();
        c = read_char();
        has_sign = ((c == '-') || (c == '+'));
        if (has_sign) {
            // if there is a sign character, we just consume it
            // here and get whatever is next in line
            sb.append((char)c);
            c = read_char();
        }

        // first leading digit - to look for hex and
        // to make sure that there is at least 1 digit (or
        // this isn't really a number
        if (!IonTokenConstsX.isDigit(c)) {
            // if it's not a digit, this isn't a number
            // the only non-digit it could have been was a
            // sign character, and we'll have read past that
            // by now
            // TODO this will be a confusing error message,
            // but I can't figure out when it will be reached.
            bad_token(c);
        }

        // the first digit is a special case
        boolean starts_with_zero = (c == '0');
        if (starts_with_zero) {
            // if it's a leading 0 check for a hex value
            int c2 = read_char();
            if (Radix.HEX.isPrefix(c2)) {
                sb.append((char)c);
                c = loadRadixValue(sb, has_sign, c2, Radix.HEX);
                return load_finish_number(sb, c, IonTokenConstsX.TOKEN_HEX);
            } else if (Radix.BINARY.isPrefix(c2)) {
                sb.append((char) c);
                c = loadRadixValue(sb, has_sign, c2, Radix.BINARY);
                return load_finish_number(sb, c, IonTokenConstsX.TOKEN_BINARY);
            }
            // not a next value, back up and try again
            unread_char(c2);
        }

        // remaining (after the first, c is the first) leading digits
        c = load_digits(sb, c);

        if (c == '-' || c == 'T') {
            // this better be a timestamp and it starts with a 4 digit
            // year followed by a dash and no leading sign
            if (has_sign) {
                error("Numeric value followed by invalid character: "
                      + sb + (char)c);
            }
            int len = sb.length();
            if (len != 4) {
                error("Numeric value followed by invalid character: "
                      + sb + (char)c);
            }
            IonType tt = load_timestamp(sb, c);
            return tt;
        }

        if (starts_with_zero) {
            // Ion doesn't allow leading zeros, so make sure our buffer only
            // has one character.
            int len = sb.length();
            if (has_sign) {
                len--; // we don't count the sign
            }
            if (len != 1) {
                error("Invalid leading zero in number: " + sb);
            }
        }

        if (c == '.') {
            // so if it's a float of some sort
            // mark it as at least a DECIMAL
            // and read the "fraction" digits
            sb.append((char)c);
            c = read_char();
            c = load_digits(sb, c);
            t = IonTokenConstsX.TOKEN_DECIMAL;
        }
        else {
            t = IonTokenConstsX.TOKEN_INT;
        }

        // see if we have an exponential as in 2d+3
        if (c == 'e' || c == 'E') {
            t = IonTokenConstsX.TOKEN_FLOAT;
            sb.append((char)c);
            c = load_exponent(sb);  // the unused lookahead char
        }
        else if (c == 'd' || c == 'D') {
            t = IonTokenConstsX.TOKEN_DECIMAL;
            sb.append((char)c);
            c = load_exponent(sb);
        }
        return load_finish_number(sb, c, t);
    }

    private final IonType load_finish_number(CharSequence numericText, int c,
                                             int token)
    throws IOException
    {
        // all forms of numeric need to stop someplace rational
        if (! is_value_terminating_character(c)) {
            error("Numeric value followed by invalid character: "
                  + numericText + (char)c);
        }

        // we read off the end of the number, so put back
        // what we don't want, but what ever we have is an int
        unread_char(c);
        IonType it = IonTokenConstsX.ion_type_of_scalar(token);
        return it;
    }
    // this returns the lookahead character it didn't use so the caller
    // can unread it
    private final int load_exponent(StringBuilder sb) throws IOException
    {
        int c = read_char();
        if (c == '-' || c == '+') {
            sb.append((char)c);
            c = read_char();
        }
        c = load_digits(sb, c);

        if (c == '.') {
            sb.append((char)c);
            c = read_char();
            c = load_digits(sb, c);
        }
        return c;
    }

    /**
     * Accumulates digits into the buffer, starting with the given character.
     *
     * @return the first non-digit character on the input. Could be the given
     *  character if its not a digit.
     *
     * @see IonTokenConstsX#isDigit(int)
     */
    private final int load_digits(StringBuilder sb, int c) throws IOException
    {
        if (!IonTokenConstsX.isDigit(c))
        {
            return c;
        }
        sb.append((char) c);

        return readNumeric(sb, Radix.DECIMAL, NumericState.DIGIT);
    }

    private final void load_fixed_digits(StringBuilder sb, int len)
        throws IOException
    {
        int c;

        switch (len) {
        default:
            while (len > 4) {
                c = read_char();
                if (!IonTokenConstsX.isDigit(c)) bad_token(c);
                sb.append((char)c);
                len--;
            }
            // fall through
        case 4:
            c = read_char();
            if (!IonTokenConstsX.isDigit(c)) bad_token(c);
            sb.append((char)c);
            // fall through
        case 3:
            c = read_char();
            if (!IonTokenConstsX.isDigit(c)) bad_token(c);
            sb.append((char)c);
            // fall through
        case 2:
            c = read_char();
            if (!IonTokenConstsX.isDigit(c)) bad_token(c);
            sb.append((char)c);
            // fall through
        case 1:
            c = read_char();
            if (!IonTokenConstsX.isDigit(c)) bad_token(c);
            sb.append((char)c);
            break;
        }

        return;
    }
    private final IonType load_timestamp(StringBuilder sb, int c)
        throws IOException
    {
        // we read the year in our caller, we should only be
        // here is we read 4 digits and then a dash or a 'T'
        assert (c == '-' || c == 'T');

        sb.append((char)c);

        // if it's 'T' we done: yyyyT
        if (c == 'T') {
            c = read_char(); // because we'll unread it before we return
            return load_finish_number(sb, c, IonTokenConstsX.TOKEN_TIMESTAMP);
        }

        // read month
        load_fixed_digits(sb, 2);

        c = read_char();
        if (c == 'T') {
            sb.append((char)c);
            c = read_char(); // because we'll unread it before we return
            return load_finish_number(sb, c, IonTokenConstsX.TOKEN_TIMESTAMP);
        }
        if (c != '-') bad_token(c);

        // read day
        sb.append((char)c);
        load_fixed_digits(sb, 2);

        // look for the 'T', otherwise we're done (and happy about it)
        c = read_char();
        if (c != 'T') {
            return load_finish_number(sb, c, IonTokenConstsX.TOKEN_TIMESTAMP);
        }

        // so either we're done or we must at least hours and minutes
        // hour
        sb.append((char)c);
        c = read_char();
        if (!IonTokenConstsX.isDigit(c)) {
            return load_finish_number(sb, c, IonTokenConstsX.TOKEN_TIMESTAMP);
        }
        sb.append((char)c);
        load_fixed_digits(sb,1); // we already read the first digit
        c = read_char();
        if (c != ':') bad_token(c);

        // minutes
        sb.append((char)c);
        load_fixed_digits(sb, 2);
        c = read_char();
        if (c == ':') {
            // seconds are optional
            // and first we'll have the whole seconds
            sb.append((char)c);
            load_fixed_digits(sb, 2);
            c = read_char();
            if (c == '.') {
                sb.append((char)c);
                c = read_char();
                // Per spec and W3C Note http://www.w3.org/TR/NOTE-datetime
                // We require at least one digit after the decimal point.
                if (!IonTokenConstsX.isDigit(c)) {
                    expected_but_found("at least one digit after timestamp's decimal point", c);
                }
                c = load_digits(sb,c);
            }
        }

        // since we have a time, we have to have a timezone of some sort
        // the timezone offset starts with a '+' '-' 'Z' or 'z'
        if (c == 'z' || c == 'Z') {
            sb.append((char)c);
            // read ahead since we'll check for a valid ending in a bit
            c = read_char();
        }
        else if (c == '+' || c == '-') {
            // then ... hours of time offset
            sb.append((char)c);
            load_fixed_digits(sb, 2);
            c = read_char();
            if (c != ':') {
                // those hours need their minutes if it wasn't a 'z'
                // (above) then it has to be a +/- hours { : minutes }
                bad_token(c);
            }
            // and finally the *not* optional minutes of time offset
            sb.append((char)c);
            load_fixed_digits(sb, 2);
            c = read_char();
        }
        else {
            // some sort of offset is required with a time value
            // if it wasn't a 'z' (above) then it has to be a +/- hours { : minutes }
            bad_token(c);
        }
        return load_finish_number(sb, c, IonTokenConstsX.TOKEN_TIMESTAMP);
    }

    private final int loadRadixValue(StringBuilder sb, boolean has_sign, int c2, Radix radix)
        throws IOException
    {
        radix.assertPrefix(c2);
        sb.append((char) c2);

        return readNumeric(sb, radix);
    }

    private final int skip_over_symbol_identifier(SavePoint sp) throws IOException
    {
        int c = read_char();

        while(IonTokenConstsX.isValidSymbolCharacter(c)) {
            c = read_char();
        }

        if (sp != null) {
            sp.markEnd(0);
         }
        return c;
    }

    protected void load_symbol_identifier(StringBuilder sb) throws IOException
    {
        int c = read_char();
        while(IonTokenConstsX.isValidSymbolCharacter(c)) {
            sb.append((char)c);
            c = read_char();
        }
        unread_char(c);
    }

    private int skip_over_symbol_operator(SavePoint sp) throws IOException
    {
        int c = read_char();

        // lookahead for +inf and -inf
        if (peek_inf_helper(c)) // this will consume the inf if it succeeds
        {
            // do nothing, peek_inf did all the work for us
            // (such as it is)
            c = read_char();
        }
        else {
            assert(IonTokenConstsX.isValidExtendedSymbolCharacter(c));

            // if it's not +/- inf then we'll just read the characters normally
            while (IonTokenConstsX.isValidExtendedSymbolCharacter(c)) {
                c = read_char();
            }
        }
        if (sp != null) {
            sp.markEnd(0);
        }
        return c;
    }
    protected void load_symbol_operator(StringBuilder sb) throws IOException
    {
        int c = read_char();

        // lookahead for +inf and -inf
        // this will consume the inf if it succeeds
        if ((c == '+' || c == '-') && peek_inf_helper(c)) {
            sb.append((char)c);
            sb.append("inf");
        }
        else {
            assert(IonTokenConstsX.isValidExtendedSymbolCharacter(c));

            // if it's not +/- inf then we'll just read the characters normally
            while (IonTokenConstsX.isValidExtendedSymbolCharacter(c)) {
                sb.append((char)c);
                c = read_char();
            }
            unread_char(c);
        }

        return;
    }
    private final int skip_single_quoted_string(SavePoint sp) throws IOException
    {
        int c;

        // the position should always be correct here
        // since there's no reason to lookahead into a
        // quoted symbol

        for (;;) {
            c = read_string_char(ProhibitedCharacters.NONE);
            switch (c) {
            case -1: unexpected_eof();
            case '\'':
                if (sp != null) {
                   sp.markEnd(-1);
                }
                return read_char(); // Return the next character beyond the token
            case '\\':
                c = read_char();
                break;
            }
        }
    }

    protected int load_single_quoted_string(StringBuilder sb, boolean is_clob)
        throws IOException
    {
        int c;

        for (;;) {
            c = read_string_char(ProhibitedCharacters.NONE);
            switch (c) {
            case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_1:
            case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_2:
            case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_3:
                continue;
            case -1:
            case '\'':
                return c;
            // new line normalization and counting is handled in read_char
            case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_1:
            case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_2:
            case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_3:
                bad_token(c);
            case '\\':
                // TODO why not read_char_escaped() ?
                //  That's how load_double_quoted_string works.
                c = read_char();
                c = read_escaped_char_content_helper(c, is_clob);
                break;
            default:
                if (!is_clob && !IonTokenConstsX.is7bitValue(c)) {
                    c = read_large_char_sequence(c);
                }
            }

            if (!is_clob) {
                if (IonUTF8.needsSurrogateEncoding(c)) {
                    sb.append(IonUTF8.highSurrogate(c));
                    c = IonUTF8.lowSurrogate(c);
                }
            }
            else if (IonTokenConstsX.is8bitValue(c)) {
                bad_token(c);
            }
            sb.append((char)c);
        }
    }

    private void skip_double_quoted_string(SavePoint sp) throws IOException
    {
        skip_double_quoted_string_helper();
        if (sp != null) {
            sp.markEnd(-1);
        }
    }

    private final void skip_double_quoted_string_helper() throws IOException
    {
        int c;
        for (;;) {
            c = read_string_char(ProhibitedCharacters.NONE);
            switch (c) {
            case -1:
                unexpected_eof(); // throws
            // new line normalization and counting is handled in read_char
            case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_1:
            case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_2:
            case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_3:
                bad_token(c); // throws
            case '"':
                return;
            case '\\':
                c = read_char();
                break;
            }
        }
    }

    protected int load_double_quoted_string(StringBuilder sb, boolean is_clob)
        throws IOException
    {
        int c;

        for (;;) {
            c = read_string_char(ProhibitedCharacters.SHORT_CHAR);
            switch (c) {
            case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_1:
            case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_2:
            case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_3:
                continue;
            case -1:
            case '"':
                return c;
            // new line normalization and counting is handled in read_char
            case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_1:
            case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_2:
            case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_3:
                bad_token(c);
            case '\\':
                c = read_char_escaped(c, is_clob);
                break;
            default:
                if (!is_clob && !IonTokenConstsX.is7bitValue(c)) {
                    c = read_large_char_sequence(c);
                }
                break;
            }

            if (!is_clob) {
                if (IonUTF8.needsSurrogateEncoding(c)) {
                    sb.append(IonUTF8.highSurrogate(c));
                    c = IonUTF8.lowSurrogate(c);
                }
            }
            sb.append((char)c);
        }
    }

    protected int read_double_quoted_char(boolean is_clob) throws IOException
    {
        int c = read_char();

        switch (c) {
        case '"':
            unread_char(c);
            c = CharacterSequence.CHAR_SEQ_STRING_TERMINATOR;
            break;
        case -1:
            break;
        case '\\':
            c = read_char_escaped(c, is_clob);
            break;
        default:
            if (!is_clob && !IonTokenConstsX.is7bitValue(c)) {
                c = read_large_char_sequence(c);
            }
            break;
        }

        return c;
    }

    /**
     * Skip to the end of a triple quoted string sequence, ignoring any
     * comments encountered between triple quoted string elements.
     * @param sp
     * @throws IOException
     */
    private void skip_triple_quoted_string(SavePoint sp) throws IOException
    {
        skip_triple_quoted_string(sp, CommentStrategy.IGNORE);
    }

    /**
     * Skip to the end of a triple quoted string sequence within a clob,
     * erroring on any comments encountered between triple quoted string
     * elements.
     * @param sp
     * @throws IOException
     */
    private void skip_triple_quoted_clob_string(SavePoint sp) throws IOException
    {
        skip_triple_quoted_string(sp, CommentStrategy.ERROR);
    }

    private void skip_triple_quoted_string(SavePoint sp, CommentStrategy commentStrategy) throws IOException
    {
        // starts AFTER the 3 quotes have been consumed
        int c;
        for (;;) {
            c = read_char();
            switch (c) {
            case -1:
                unexpected_eof();
            case '\'':
                c = read_char();
                if (c == '\'') { // 2nd quote
                    c = read_char(); // possibly the 3rd
                    if (sp != null) {
                        sp.markEnd(-3);
                    }
                    if (c == '\'') { // it is the 3rd quote - end of this segment
                        c = skip_over_whitespace(commentStrategy);
                        if (c == '\'' && is_2_single_quotes_helper()) {
                            // there's another segment so read the next segment as well
                            break;
                        }
                        // end of last segment
                        unread_char(c);
                        return;
                    }
                }
                break;
            case '\\':
                c = read_char();
               break;
            }
        }
    }

    protected int load_triple_quoted_string(StringBuilder sb, boolean is_clob)
        throws IOException
    {
        int c;

        for (;;) {
            c = read_triple_quoted_char(is_clob);
            switch(c) {
            case CharacterSequence.CHAR_SEQ_STRING_TERMINATOR:
            case CharacterSequence.CHAR_SEQ_EOF: // was EOF
                return c;
            // new line normalization and counting is handled in read_char
            case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_1:
                c = '\n';
                break;
            case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_2:
                // TODO: uncomment if we don't want to normalize end of line: c = '\r';
                c = '\n';
                break;
            case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_3:
                // TODO: uncomment if we don't want to normalize end of line: sb.append('\r');
                c = '\n';
                break;
            case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_1:
            case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_2:
            case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_3:
            case CharacterSequence.CHAR_SEQ_STRING_NON_TERMINATOR:
                continue;
            default:
                break;
            }
            // if this isn't a clob we need to decode UTF8 and
            // handle surrogate encoding (otherwise we don't care)
            if (!is_clob) {
                if (IonUTF8.needsSurrogateEncoding(c)) {
                    sb.append(IonUTF8.highSurrogate(c));
                    c = IonUTF8.lowSurrogate(c);
                }
            }
            sb.append((char)c);
        }
    }

    protected int read_triple_quoted_char(boolean is_clob) throws IOException
    {
        int c = read_string_char(ProhibitedCharacters.LONG_CHAR);
        switch (c) {
        case '\'':
            if (is_2_single_quotes_helper()) {
                // so at this point we are at the end of the closing
                // triple quote - so we need to look ahead to see if
                // there's just whitespace and a new opening triple quote
                c = skip_over_whitespace();
                if (c == '\'' && is_2_single_quotes_helper()) {
                    // there's another segment so read the next segment as well
                    // since we're now just before char 1 of the next segment
                    // loop again, but don't append this char
                    return CharacterSequence.CHAR_SEQ_STRING_NON_TERMINATOR;
                }
                // end of last segment - we're done (although we read a bit too far)
                unread_char(c);
                c = CharacterSequence.CHAR_SEQ_STRING_TERMINATOR;
            }
            break;
        case '\\':
            c = read_char_escaped(c, is_clob);
            break;
        case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_1:
        case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_2:
        case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_3:
        case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_1:
        case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_2:
        case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_3:
            break;
        case -1:
            break;
        default:
            if (!is_clob && !IonTokenConstsX.is7bitValue(c)) {
                c = read_large_char_sequence(c);
            }
            break;
        }

        return c;
    }

    /** Skips over the closing }} too. */
    protected void skip_over_lob(int lobToken, SavePoint sp) throws IOException {
        switch(lobToken) {
        case IonTokenConstsX.TOKEN_STRING_DOUBLE_QUOTE:
            skip_double_quoted_string(sp);
            skip_clob_close_punctuation();
            break;
        case IonTokenConstsX.TOKEN_STRING_TRIPLE_QUOTE:
            skip_triple_quoted_clob_string(sp);
            skip_clob_close_punctuation();
            break;
        case IonTokenConstsX.TOKEN_OPEN_DOUBLE_BRACE:
            skip_over_blob(sp);
            break;
        default:
            error("unexpected token "+IonTokenConstsX.getTokenName(lobToken)+
                  " encountered for lob content");
        }
    }

    protected void load_clob(int lobToken, StringBuilder sb) throws IOException
    {
        switch(lobToken) {
        case IonTokenConstsX.TOKEN_STRING_DOUBLE_QUOTE:
            load_double_quoted_string(sb, true);
            break;
        case IonTokenConstsX.TOKEN_STRING_TRIPLE_QUOTE:
            load_triple_quoted_string(sb, true);
            break;
        case IonTokenConstsX.TOKEN_OPEN_DOUBLE_BRACE:
            load_blob(sb);
            break;
        default:
            error("unexpected token "+IonTokenConstsX.getTokenName(lobToken)+
                  " encountered for lob content");
        }
    }

    private final int read_char_escaped(int c, boolean is_clob)
        throws IOException
    {
        for (;;) {
            switch (c) {
            case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_1:
            case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_2:
            case CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_3:
                // loop again, we don't want empty escape chars
                c = read_string_char(ProhibitedCharacters.NONE);
                continue;
            case '\\':
                c = read_char();
                if (c < 0) {
                    unexpected_eof();
                }
                c = read_escaped_char_content_helper(c, is_clob);
                if (c == CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_1
                 || c == CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_2
                 || c == CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_3
                ) {
                    // loop again, we don't want empty escape chars
                    c = read_string_char(ProhibitedCharacters.NONE);
                    continue;
                }
                if (c == IonTokenConstsX.ESCAPE_NOT_DEFINED) bad_escape_sequence();
                break;
            default:
                if (!is_clob && !IonTokenConstsX.is7bitValue(c)) {
                    c = read_large_char_sequence(c);
                }
                break;
            }
            break; // at this point we have a post-escaped character to return to the caller
        }

        if (c == CharacterSequence.CHAR_SEQ_EOF) return c;
        if (is_clob && !IonTokenConstsX.is8bitValue(c)) {
            error("invalid character ["+ printCodePointAsString(c)+"] in CLOB");
        }
        return c;
    }

    private final int read_large_char_sequence(int c) throws IOException
    {
        if (_stream._is_byte_data) {
            return read_ut8_sequence(c);
        }
        if (PrivateIonConstants.isHighSurrogate(c)) {
            int c2 = read_char();
            if (PrivateIonConstants.isLowSurrogate(c2)) {
                c = PrivateIonConstants.makeUnicodeScalar(c, c2);
            }
            else {
                // we don't always pair up surrogates here
                // our caller does that
                unread_char(c2);
            }
        }
        return c;
    }
    private final int read_ut8_sequence(int c) throws IOException
    {
        // this should have the high order bit set
        assert(!IonTokenConstsX.is7bitValue(c));
        int len = IonUTF8.getUTF8LengthFromFirstByte(c);
        int b2, b3, b4;
        switch (len) {
        case 1:
            break;
        case 2:
            b2 = read_char();
            c = IonUTF8.twoByteScalar(c, b2);
            break;
        case 3:
            b2 = read_char();
            b3 = read_char();
            c = IonUTF8.threeByteScalar(c, b2, b3);
            break;
        case 4:
            b2 = read_char();
            b3 = read_char();
            b4 = read_char();
            c = IonUTF8.fourByteScalar(c, b2, b3, b4);
            break;
        default:
            error("invalid UTF8 starting byte");
        }
        return c;
    }

    private void skip_over_blob(SavePoint sp) throws IOException
    {
        int c = skip_over_blob_whitespace();
        for (;;) {
            if (c == UnifiedInputStreamX.EOF) break;
            if (c == '}') break;
            c = skip_over_blob_whitespace();
        }
        if (sp != null) {
            // we don't care about these last 2 closing curly braces
            // but we may have seen one of them already
            int offset = (c == '}') ? -1 : 0;
            sp.markEnd(offset);
        }
        // did we hit EOF or the first '}' ?
        if (c != '}') unexpected_eof();
        c = read_char();
        if (c < 0) {
            unexpected_eof();
        }
        if (c != '}') {
            String message = "improperly closed BLOB, "
                           + IonTextUtils.printCodePointAsString(c)
                           + " encountered when '}' was expected";
            error(message);
        }
        if (sp != null) {
            sp.markEnd();
        }
        return;
    }
    protected void load_blob(StringBuilder sb) throws IOException {
        int c;

        for (;;) {
            c = read_base64_byte();
            if (c == UnifiedInputStreamX.EOF) {
                break;
            }
            sb.append(c);
        }
        // did we hit EOF or the first '}' ?
        if (_stream.isEOF()) unexpected_eof();

        c = read_char();
        if (c < 0) {
            unexpected_eof();
        }
        if (c != '}') {
            String message = "improperly closed BLOB, "
                           + IonTextUtils.printCodePointAsString(c)
                           + " encountered when '}' was expected";
            error(message);
        }
        return;
    }

    private final int read_escaped_char_content_helper(int c1, boolean is_clob)
        throws IOException
    {
        if (c1 < 0) {
            switch (c1) {
            // new line normalization and counting is handled in read_char
            case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_1:
                return CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_1;
            case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_2:
                return CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_2;
            case CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_3:
                return CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_3;
            default:
                bad_escape_sequence(c1);
            }
        }
        if (!IonTokenConstsX.isValidEscapeStart(c1)) {
            bad_escape_sequence(c1);
        }
        int c2 = IonTokenConstsX.escapeReplacementCharacter(c1);
        switch (c2) {
        case IonTokenConstsX.ESCAPE_NOT_DEFINED:
            assert false
                : "invalid escape start characters (line " + ((char)c1)
                + " should have been removed by isValid";
            break;
        case IonTokenConstsX.ESCAPE_LITTLE_U:
            if (is_clob) {
                bad_escape_sequence(c2);
            }
            c2 = read_hex_escape_sequence_value(4);
            break;
        case IonTokenConstsX.ESCAPE_BIG_U:
            if (is_clob) {
                bad_escape_sequence(c2);
            }
            c2 = read_hex_escape_sequence_value(8);
            break;
        case IonTokenConstsX.ESCAPE_HEX:
            c2 = read_hex_escape_sequence_value(2);
            break;
        }
        return c2;
    }
    private final int read_hex_escape_sequence_value(int len) throws IOException
    {
        int hexchar = 0;
        while (len > 0) {
            len--;
            int c = read_char();
            if (c < 0) {
                unexpected_eof();
            }
            int d = IonTokenConstsX.hexDigitValue(c);
            if (d < 0) return -1;
            hexchar = (hexchar << 4) + d;
        }
        if (len > 0) {
            String message = "invalid hex digit ["
                + IonTextUtils.printCodePointAsString(hexchar)
                + "] in escape sequence";
            error(message);
        }
        return hexchar;
    }

    public final int read_base64_byte() throws IOException
    {
        int b;
        if (_base64_prefetch_count < 1) {
            b = read_base64_byte_helper();
        }
        else {
            b = (_base64_prefetch_stack & 0xff);
            _base64_prefetch_stack >>= 8;
            _base64_prefetch_count--;
        }
        return b;
    }
    private final int read_base64_byte_helper() throws IOException
    {
        // if there's any data left to read (the normal case)
        // we'll read 4 characters off the input source and
        // generate 1-3 bytes to return to the user.  That
        // will be 1 byte returned immediately and 0-2 bytes
        // put on the _binhex_stack to return later

        int c = skip_over_blob_whitespace();
        if (c == UnifiedInputStreamX.EOF || c == '}') {
            // we'll figure how which is which by check the stream for eof
            return UnifiedInputStreamX.EOF;
        }

        int c1 = read_base64_getchar_helper(c);
        int c2 = read_base64_getchar_helper();
        int c3 = read_base64_getchar_helper();
        int c4 = read_base64_getchar_helper();

        int b1, len = decode_base64_length(c1, c2, c3, c4);

        _base64_prefetch_stack = 0;
        _base64_prefetch_count = len - 1;
        switch (len) {
        default:
            String message =
                "invalid binhex sequence encountered at offset"+input_position();
            throw new IonReaderTextTokenException(message);
        case 3:
            int b3  = decode_base64_byte3(c1, c2, c3, c4);
            _base64_prefetch_stack = (b3 << 8) & 0xff00;
            // fall through
        case 2:
            int b2  = decode_base64_byte2(c1, c2, c3, c4);
            _base64_prefetch_stack |= (b2 & 0xff);
            // fall through
        case 1:
            b1 = decode_base64_byte1(c1, c2, c3, c4);
            // fall through
        }
        return b1;
    }
    private final int read_base64_getchar_helper(int c) throws IOException {
        assert( ! (c == UnifiedInputStreamX.EOF || c == '}') );

        if (c == UnifiedInputStreamX.EOF || c == '}') {
            return UnifiedInputStreamX.EOF;
        }
        if (c == BASE64_TERMINATOR_CHAR) {
            error("invalid base64 image - excess terminator characters ['=']");
        }
        return read_base64_getchar_helper2(c);
    }
    private final int read_base64_getchar_helper() throws IOException {
        int c = skip_over_blob_whitespace();
        if (c == UnifiedInputStreamX.EOF || c == '}') {
            error("invalid base64 image - too short");
        }
        return read_base64_getchar_helper2(c);
    }
    private final int read_base64_getchar_helper2(int c) throws IOException {
        assert( ! (c == UnifiedInputStreamX.EOF || c == '}') );

        if (c == BASE64_TERMINATOR_CHAR) {
            // we're using a new EOF here since the '=' is in range
            // of 0-63 (6 bits) and we don't want to confuse it with
            // the normal EOF
            return BASE64_EOF;
        }
        int b = BASE64_CHAR_TO_BIN[c & 0xff];
        if (b == UnifiedInputStreamX.EOF || !IonTokenConstsX.is8bitValue(c)) {
            String message = "invalid character "
                           + Character.toString((char)c)
                           + " encountered in base64 value at "
                           + input_position();
            throw new IonReaderTextTokenException(message);
        }
        return b;
    }
    private final static int decode_base64_length(int c1, int c2, int c3, int c4) {
        int len = 3;
        if (c4 != BASE64_EOF)      len = 3;
        else if (c3 != BASE64_EOF) len = 2;
        else                       len = 1;
        return len;
    }
    private final static int decode_base64_byte1(int c1, int c2, int c3, int c4) {
        //extracted from Base64Encoder.java:
        // convert =  c1 << 18;    [6:1] + 18 => [24:19]
        // convert |= (c2 << 12);  [6:1] + 12 => [18:13]
        // b1 = (char)((convert & 0x00FF0000) >> 16);  [32:1] & 0x00FF0000 => [24:17] - 16 => [8:1]
        // byte1 uses the 6 bits in char1 + 2 highest bits (out of 6) from char2
        if (_debug) assert(decode_base64_length(c1, c2, c3, c4) >= 1);
        int b1 = (((c1 << 2) & 0xfc) | ((c2 >> 4) & 0x03));
        return b1;
    }
    private final static int decode_base64_byte2(int c1, int c2, int c3, int c4) {
        //convert |= (c2 << 12);  [6:1]+12 => [18:13]
        //convert |= (c3 << 6);   [6:1]+6  => [12:7]
        //b2 = (char)((convert & 0x0000FF00) >> 8); [32:1] & 0x0000FF00 => [16:9] - 8 => [8:1]
        // [18:13] - 8 -> [10:5] or [6:5] from c2
        // [12:7] - 8 -> [4:-1] or [6:3] - 2 from c3
        //byte2 uses 4 low bits from c2 and 4 high bits from c3
        if (_debug) assert(decode_base64_length(c1, c2, c3, c4) >= 2);
        int b2 = (((c2 << 4) & 0xf0) | ((c3 >> 2) & 0x0f)) & 0xff;
        return b2;
    }
    private final static int decode_base64_byte3(int c1, int c2, int c3, int c4) {
        // convert |= (c3 << 6); [6:1]+6  => [12:7]
        // convert |= (c4 << 0); [6:1]+9  => [6:1]
        // b3 = (char)((convert & 0x000000FF) >> 0);
        // b3 uses low 2 bits from c3 and all 6 bits of c4
        if (_debug) assert(decode_base64_length(c1, c2, c3, c4) >= 3);
        int b3 = (((c3 & 0x03) << 6) | (c4 & 0x3f)) & 0xff;
        return b3;
    }

    protected void save_point_start(SavePoint sp) throws IOException
    {
        assert(sp != null && sp.isClear());
        long line_number = _line_count;
        long line_start = _line_starting_position;
        sp.start(line_number, line_start);
    }
    protected void save_point_activate(SavePoint sp) throws IOException
    {
        assert(sp != null && sp.isDefined());
        long line_number = _line_count;
        long line_start  = _line_starting_position;
        // this will set the "restore" (aka prev) line and start offset so
        // that when we pop the save point we'll get the correct line & char
        _stream._save_points.savePointPushActive(sp, line_number, line_start);
        _line_count = sp.getStartLineNumber();
        _line_starting_position = sp.getStartLineStart();
    }
    protected void save_point_deactivate(SavePoint sp) throws IOException
    {
        assert(sp != null && sp.isActive());

        _stream._save_points.savePointPopActive(sp);
        _line_count = sp.getPrevLineNumber();
        _line_starting_position = sp.getPrevLineStart();
    }

    protected final void error(String message)
    {
        String message2 = message + input_position();
        throw new IonReaderTextTokenException(message2);
    }
    protected final void unexpected_eof()
    {
        String message = "unexpected EOF encountered "+input_position();
        throw new UnexpectedEofException(message);
    }
    protected final void bad_escape_sequence()
    {
        String message = "bad escape character encountered "+input_position();
        throw new IonReaderTextTokenException(message);
    }
    protected final void bad_escape_sequence(int c)
    {
        String message =
            "bad escape character '"+printCodePointAsString(c)+
            "' encountered "+input_position();
        throw new IonReaderTextTokenException(message);
    }
    protected final void bad_token_start(int c)
    {
        String message =
            "bad character ["+c+", "+printCodePointAsString(c)+
            "] encountered where a token was supposed to start "+
            input_position();
        throw new IonReaderTextTokenException(message);
    }
    protected final void bad_token(int c)
    {
        String charStr = IonTextUtils.printCodePointAsString(c);
        String message =
            "a bad character " + charStr + " was encountered "+input_position();
        throw new IonReaderTextTokenException(message);
    }

    protected final void expected_but_found(String expected, int c)
    {
        String charStr = IonTextUtils.printCodePointAsString(c);
        String message =
            "Expected " + expected + " but found " + charStr + input_position();
        throw new IonReaderTextTokenException(message);
    }

    static public class IonReaderTextTokenException extends IonException {
        private static final long serialVersionUID = 1L;
        IonReaderTextTokenException(String msg) {
            super(msg);
        }
    }

    private enum ProhibitedCharacters {
        SHORT_CHAR
        {
            boolean includes(int c)
            {
                return isControlCharacter(c) && !isWhitespace(c);
            }
        },

        LONG_CHAR
        {
            boolean includes(int c)
            {
                return isControlCharacter(c) && !isWhitespace(c) && !isNewline(c);
            }
        },

        NONE
        {
            boolean includes(int c)
            {
                return false;
            }
        };

        abstract boolean includes(int c);

        private static boolean isControlCharacter(int c)
        {
            return c <= 0x1F && 0x00 <= c;
        }

        private static boolean isNewline(int c)
        {
            return c == 0x0A || c == 0x0D;
        }

        private static boolean isWhitespace(int c)
        {
            return c == 0x09 // tab
                || c == 0x0B // vertical tab
                || c == 0x0C // form feed
                || c == 0x20 // space
            ;
        }
    }

    private enum Radix
    {
        BINARY
        {
            boolean isPrefix(int c)
            {
                return c == 'b' || c == 'B';
            }

            boolean isValidDigit(int c)
            {
                return IonTokenConstsX.isBinaryDigit(c);
            }

            @Override
            char normalizeDigit(char c)
            {
                return c; // no normalization required
            }
        },

        DECIMAL
        {
            boolean isPrefix(int c)
            {
                return false;
            }

            boolean isValidDigit(int c)
            {
                return IonTokenConstsX.isDigit(c);
            }

            @Override
            char normalizeDigit(char c)
            {
                return c; // no normalization required
            }
        },

        HEX
        {
            boolean isPrefix(int c)
            {
                return c == 'x' || c == 'X';
            }

            boolean isValidDigit(int c)
            {
                return IonTokenConstsX.isHexDigit(c);
            }

            @Override
            char normalizeDigit(char c)
            {
                return Character.toLowerCase(c);
            }
        };

        abstract boolean isPrefix(int c);
        abstract boolean isValidDigit(int c);
        abstract char normalizeDigit(char c);

        void assertPrefix(int c)
        {
            assert isPrefix(c);
        }
    }

    private int readNumeric(Appendable buffer, Radix radix) throws IOException
    {
        return readNumeric(buffer, radix, NumericState.START);
    }

    private int readNumeric(Appendable buffer, Radix radix, NumericState startingState) throws IOException
    {
        NumericState state = startingState;

        for (;;)
        {
            int c = read_char();
            switch (state)
            {
                case START:
                    if (radix.isValidDigit(c))
                    {
                        buffer.append(radix.normalizeDigit((char) c));
                        state = NumericState.DIGIT;
                    }
                    else
                    {
                        return c;
                    }
                    break;
                case DIGIT:
                    if (radix.isValidDigit(c))
                    {
                        buffer.append(radix.normalizeDigit((char) c));
                        state = NumericState.DIGIT;
                    }
                    else if (c == '_')
                    {
                        state = NumericState.UNDERSCORE;
                    }
                    else
                    {
                        return c;
                    }
                    break;
                case UNDERSCORE:
                    if (radix.isValidDigit(c))
                    {
                        buffer.append(radix.normalizeDigit((char) c));
                        state = NumericState.DIGIT;
                    }
                    else
                    {
                        unread_char(c);
                        return '_';
                    }
                    break;
            }
        }
    }

    private enum NumericState
    {
        START,
        UNDERSCORE,
        DIGIT,
    }
}
