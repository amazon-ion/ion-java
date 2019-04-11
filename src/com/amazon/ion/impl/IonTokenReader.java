/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion.impl;

import static com.amazon.ion.util.IonTextUtils.isDigit;
import static com.amazon.ion.util.IonTextUtils.isOperatorPart;
import static com.amazon.ion.util.IonTextUtils.isWhitespace;
import static com.amazon.ion.util.IonTextUtils.printCodePointAsString;

import com.amazon.ion.Decimal;
import com.amazon.ion.IonException;
import com.amazon.ion.Timestamp;
import com.amazon.ion.UnexpectedEofException;
import com.amazon.ion.impl._Private_IonConstants.HighNibble;
import com.amazon.ion.util.IonTextUtils;
import java.io.IOException;
import java.io.PushbackReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Stack;

/**
 *  This class is responsible for breaking the input stream into Tokens.
 *  It does both the token recognition (aka the scanner) and the state
 *  management needed to use the value underlying some of these tokens.
 */
final class IonTokenReader
{
    // TODO clean up, many of these are unused.
    public static int isPunctuation = 0x0001;
    public static int isKeyword     = 0x0002;
    public static int isTypeName    = 0x0004;
    public static int isConstant    = 0x0008;
    private static int isPosInt     = 0x0010;
    private static int isNegInt     = 0x0020;
    public static int isFloat       = 0x0040;
    public static int isDecimal     = 0x0080;
    public static int isTag         = 0x0100;

    static public enum Type {

        eof                 (isPunctuation,         "<eof>"    ),
        tOpenParen          (isPunctuation,         "("        ),
        tCloseParen         (isPunctuation,         ")"        ),
        tOpenSquare         (isPunctuation,         "["        ),
        tCloseSquare        (isPunctuation,         "["        ),
        tOpenCurly          (isPunctuation,         "{"        ),
        tCloseCurly         (isPunctuation,         "}"        ),
        tOpenDoubleCurly    (isPunctuation,         "{{"    ),
        //tCloseDoubleCurly (isPunctuation,         "}}"    ), // only valid at end of blob, not recognized in token read
        tSingleQuote        (isPunctuation,         "'"        ),
        tDoubleQuote        (isPunctuation,         "\""    ),
        //tColon            (isPunctuation,         ":"        ), // filled in during identifier scan with lookahead
        //tDoubleColon      (isPunctuation,         "::"    ), // ditto
        tComma              (isPunctuation,         ","        ),

        kwTrue            ((isConstant + isTag + isKeyword),  "true",           HighNibble.hnBoolean),
        kwFalse           ((isConstant + isTag + isKeyword),  "false",          HighNibble.hnBoolean),
        kwNull            ((isConstant + isTag + isKeyword),  "null",           HighNibble.hnNull),

        kwNullNull        ((isConstant + isTag + isKeyword),  "null.null",      HighNibble.hnNull),
        kwNullInt         ((isConstant + isTag + isKeyword),  "null.int",       HighNibble.hnPosInt),
        kwNullList        ((isConstant + isTag + isKeyword),  "null.list",      HighNibble.hnList),
        kwNullSexp        ((isConstant + isTag + isKeyword),  "null.sexp",      HighNibble.hnSexp),
        kwNullFloat       ((isConstant + isTag + isKeyword),  "null.float",     HighNibble.hnFloat),
        kwNullBlob        ((isConstant + isTag + isKeyword),  "null.blob",      HighNibble.hnBlob),
        kwNullClob        ((isConstant + isTag + isKeyword),  "null.clob",      HighNibble.hnClob),
        kwNullString      ((isConstant + isTag + isKeyword),  "null.string",    HighNibble.hnString),
        kwNullStruct      ((isConstant + isTag + isKeyword),  "null.struct",    HighNibble.hnStruct),  // LN
        kwNullSymbol      ((isConstant + isTag + isKeyword),  "null.symbol",    HighNibble.hnSymbol),
        kwNullBoolean     ((isConstant + isTag + isKeyword),  "null.bool",      HighNibble.hnBoolean),
        kwNullDecimal     ((isConstant + isTag + isKeyword),  "null.decimal",   HighNibble.hnDecimal),
        kwNullTimestamp   ((isConstant + isTag + isKeyword),  "null.timestamp", HighNibble.hnTimestamp),

        kwNan             ((isConstant + isKeyword),          "nan",            HighNibble.hnFloat),
        kwPosInf          ((isConstant + isKeyword),          "+inf",           HighNibble.hnFloat),
        kwNegInf          ((isConstant + isKeyword),          "-inf",           HighNibble.hnFloat),

        constNegInt       ((isConstant + isNegInt),           "cNegInt",        HighNibble.hnNegInt),
        constPosInt       ((isConstant + isPosInt),           "cPosInt",        HighNibble.hnPosInt),
        constFloat        ((isConstant + isFloat),            "cFloat",         HighNibble.hnFloat),
        constDecimal      ((isConstant + isDecimal),          "cDec",           HighNibble.hnDecimal),
        constTime         ((isConstant),                      "cTime",          HighNibble.hnTimestamp),
        constString       ((isConstant + isTag),              "cString",        HighNibble.hnString),
        constSymbol       ((isConstant + isTag),              "cSymbol",        HighNibble.hnSymbol),
        constMemberName   ((isConstant + isTag),              "cMemberName",    HighNibble.hnSymbol),
        constUserTypeDecl ((isConstant + isTag),              "cUserTypeDecl",  HighNibble.hnSymbol),

        none(0);

        private int            flags;
        private String        image;
        private HighNibble    highNibble;

        Type() {}
        Type(int v) {
            this.flags = v;
        }
        Type(int v, String name) {
            flags = v;
            image = name;
        }
        Type(int v, String name, HighNibble ln) {
            flags = v;
            image = name;
            highNibble = ln;
        }

        /**
         * TODO why this class exists at all.  We should store
         * this stuff directly in IonTimestampImpl as a BigDecimal (time) and
         * int (offset, -1==unknown) and avoid constructing more objects.
         * <p>
         * Also, we probably don't need the DateFormats.  The parser already
         * matches against a regex, so we should be able to pull the data
         * straight from the string to compute the value.
         */
        public static class timeinfo {  // TODO remove vestigial class timeinfo

            static public Timestamp parse(String s) {
                Timestamp t = null;
                s = s.trim(); // TODO why is this necessary?
                try {
                    t = Timestamp.valueOf(s);  // TODO should Timestamp just throw an IonException?
                }
                catch (IllegalArgumentException e) {
                    throw new IonException(e);
                }
                return t;
            }

        }

        // TODO move out of this class into TR.
        public Type setNumericValue(IonTokenReader tr, String s) {
            switch (this) {
            case kwNan:
                tr.doubleValue = Double.NaN;
                return this;
            case kwPosInf:
                tr.doubleValue = Double.POSITIVE_INFINITY;
                return this;
            case kwNegInf:
                tr.doubleValue = Double.NEGATIVE_INFINITY;
                return this;
            case constNegInt:
            case constPosInt:
                if (NumberType.NT_HEX.equals(tr.numberType)) {
                    tr.intValue = new BigInteger(s, 16);
                    // In hex case we've discarded the prefix [+-]?0x so
                    // reconstruct the sign
                    if (this == constNegInt) tr.intValue = tr.intValue.negate();
                }
                else {
                    tr.intValue = new BigInteger(s, 10);

                    // Make sure that sign aligns with type.
                    // Note that this allows negative zero.
                    assert (BigInteger.ZERO.equals(tr.intValue)
                             ? true
                             : this == (tr.intValue.signum() < 0 ? constNegInt : constPosInt));
                }
                return this;
            case constFloat:
                tr.doubleValue = Double.parseDouble(s);
                return this;
            case constDecimal:
                // BigDecimal parses using e instead of d or D
                String eFormat = s.replace('d', 'e');
                if (eFormat == s) {
                    // No match for 'd' but look for 'D'
                    eFormat = s.replace('D', 'e');
                }
                tr.decimalValue = Decimal.valueOf(eFormat);
                return this;
            case constTime:
                tr.dateValue = timeinfo.parse(s);
                return this;
            default:
                throw new  AssertionError("Unknown op for numeric case: " + this);
            }
        }

        public boolean isKeyword()     { return ((flags & isKeyword) != 0); }
        public boolean isConstant()    { return ((flags & isConstant) != 0); }
        public boolean isNumeric()     { return ((flags & (isPosInt + isNegInt + isFloat + isDecimal)) != 0); }

        public String  getImage()      { return (image == null) ? this.name() : image; }

        public HighNibble getHighNibble() { return highNibble; }

        @Override
        public String toString() {
            if (this.getImage() != null) return this.getImage();
            return super.toString();
        }
    }

    //
    // this is a temp reader we use when converting blob's into
    // blob values
    //
    static class LocalReader extends Reader {

        IonTokenReader _tr;
        int            _sboffset;
        int            _sbavailable;

        LocalReader(IonTokenReader tr) {
            _tr = tr;
        }

        @Override
        public void close() throws IOException {
            _tr = null;
            return;
        }

        @Override
        public void reset() {
            _sboffset = 0;
            _sbavailable = _tr.value.length();
        }

        @Override
        public int read() throws IOException {
            int c = -1;

            if (_sbavailable > 0) {
                c = _tr.value.charAt(_sboffset++);
                _sbavailable--;
            }
            else {
                c = _tr.read();
            }
            return c;
        }

        @Override
        public int read(char[] dst, int dstoffset, int len) throws IOException {
            int needed = len;

            while (needed-- > 0) {
                int c = this.read();
                if (c < 0) break;
                dst[dstoffset++] = (char)c;
            }
            return len - needed;
        }
    }

    // easy to use number types
    static enum NumberType {
        NT_POSINT,
        NT_NEGINT,
        NT_HEX, // pos or neg
        NT_FLOAT,
        NT_DECIMAL,
        NT_DECIMAL_NEGATIVE_ZERO
    }

    /**
     * Magic "character" to represent an escape sequence with an empty expansion.
     * (or, in other words, a non-eof character that should be ignored)
     */
    static final int EMPTY_ESCAPE_SEQUENCE = -2;


    //--------------------------------------------------------------------
    //
    // this is the parsing context to manage the in list, in struct,
    // waiting for identifier tag (usertypedecl or member name) vs value
    //
    //
    static public enum Context {
        NONE,
        STRING,
        BLOB,
        CLOB,
        EXPRESSION,
        DATALIST,
        STRUCT;
    }
    public Stack<Context> contextStack = new Stack<Context>();
    public Context context = Context.NONE;

    public  void pushContext(Context newcontext) {
        contextStack.push(newcontext);
        context = newcontext;
    }
    public Context popContext() {
        context = contextStack.pop();
        return context;
    }

    //--------------------------------------------------------------------
    //
    // state that keeps us on track ...
    //

    private IonCharacterReader  in;
    private LocalReader         localReader;
    private PushbackReader      pushbackReader;

    public boolean          inQuotedContent;
    public boolean          isIncomplete;
    public boolean          isLongString;
    public boolean          quotedIdentifier;
    public int              embeddedSlash;
    public int              endquote;


    public Type             t        = Type.none;
    public Type             keyword  = Type.none;
    public StringBuilder    value    = new StringBuilder();

    public String           stringValue;
    public Double           doubleValue;
    public BigInteger       intValue;

    public Timestamp        dateValue;
    public BigDecimal       decimalValue;
    public boolean          isNegative;
    public NumberType       numberType;

    public IonTokenReader(final Reader r) {
        this.in = new IonCharacterReader( r );
    }

    public long getConsumedAmount() {
        return in.getConsumedAmount();
    }

    public int getLineNumber() {
        return in.getLineNumber();
    }

    public int getColumn() {
        return in.getColumn();
    }

    public String position() {
        return "line " + this.getLineNumber() + " column " + this.getColumn();
    }

    public String getValueString(boolean is_in_expression) throws IOException {
        if (this.isIncomplete) {
            finishScanString(is_in_expression);
            stringValue = value.toString();  // TODO combine with below?
            this.inQuotedContent = false;
        }
        else if (stringValue == null) {
            stringValue = value.toString();
        }
        return stringValue;
    }

    void resetValue() {
        isIncomplete = false;
        stringValue = null;
        doubleValue = null;
        intValue = null;
        dateValue = null;
        decimalValue = null;
        isNegative = false;
        numberType = null;
        t = null;
        value.setLength(0);
    }

    public PushbackReader getPushbackReader() {
        if (localReader == null) {
            localReader = new LocalReader(this);
            pushbackReader =
                new PushbackReader(localReader,
                                   _Private_Utils.MAX_LOOKAHEAD_UTF16);
        }
        localReader.reset();
        return pushbackReader ;
    }

    /**
     * Reads the next character using the underlying stream.
     * @return -1 on end of stream.
     * @throws IOException
     */
    final int read() throws IOException {
        final int ch = in.read();
        assert ch != '\r';
        return ch;
    }


    int readIgnoreWhitespace() throws IOException {
        assert !inQuotedContent;

        int c;

        // read through comments - this detects and rejects double slash
        // and slash star style comments and their contents
        for (;;) {
            c = read();

            if (c == '/') { // possibly a start of a comment
                int c2 = read();

                if (c2 == '/') {
                    // we have a // comment, scan for the terminating new line
                    while (c2 != '\n' && c2 != -1) {
                        c2 = read();
                    }
                    c = c2;
                }
                else if (c2 == '*') {
                    // we have a /* */ comment scan for closing */
                    scancomment:
                    while (c2 != -1) {
                        c2 = read();

                        if (c2 == '*') {
                            c2 = read();

                            if (c2 == '/') {
                                break scancomment;
                            }
                            unread(c2); // in case this was the '*' we're looking for
                        }
                    }
                    c = read();
                }
                else {
                    // it wasn't a comment start, throw it back
                    unread(c2);
                }
            }
            if (/* inContent ||*/ !isWhitespace(c)) {
                break;
            }
        }
        return c;
    }
    void unread(int c) throws IOException {
        this.in.unread(c);
    }

    // TODO clone the body of next(c) here, later , for perf, we
    //      don't really want TWO methods calls per character, or
    //      mostly we don't want an extra one. (and I doubt that
    //      Java handles the tail optimization)
    public Type next(boolean is_in_expression) throws IOException {
        inQuotedContent = false;
        int c = this.readIgnoreWhitespace();
        return next(c, is_in_expression);
    }

    private Type next(int c, boolean is_in_expression) throws IOException {
        int c2;
        t = Type.none;

        isIncomplete = false;
        switch (c) {
        case -1:
            return (t = Type.eof);
        case '{':
            c2 = read();
            if (c2 == '{') {
                return (t = Type.tOpenDoubleCurly);
            }
            unread(c2);
            return (t = Type.tOpenCurly);
        case '}':
            return (t = Type.tCloseCurly);
        case '[':
            return (t = Type.tOpenSquare);
        case ']':
            return (t = Type.tCloseSquare);
        case '(':
            return (t = Type.tOpenParen);
        case ')':
            return (t = Type.tCloseParen);
        case ',':
            return (t = Type.tComma);
        case '0': case '1': case '2': case '3': case '4':
        case '5': case '6': case '7': case '8': case '9':
            return readNumber(c);
        case '\"':
            inQuotedContent = true;
            this.keyword = Type.none; // anything in quotes isn't a keyword
            return scanString(c, _Private_IonConstants.lnIsVarLen - 1);
        case '\'':
            c2 = read();
            if (c2 == '\'') {
                c2 = read();
                if (c2 == '\'') {
                    return scanLongString();
                }
                this.unread(c2);
                c2 = '\''; // restore c2 for the next unread
            }
            this.unread(c2);
            inQuotedContent = true;
            return scanIdentifier(c);
        case '+':
            c2 = read();
            if (c2 == 'i') {
                c2 = read();
                if (c2 == 'n') {
                    c2 = read();
                    if (c2 == 'f') {
                        return (t = Type.kwPosInf);
                    }
                    this.unread(c2);
                    c2 = 'n';
                }
                this.unread(c2);
                c2 = 'i';
            }
            this.unread(c2);
            if (is_in_expression) {
                return scanOperator(c);
            }
            break; // break to error
        case '-':
            c2 = read();
            if (c2 >= '0' && c2 <= '9') {
                this.unread(c2);
                return readNumber(c);
            }
            if (c2 == 'i') {
                c2 = read();
                if (c2 == 'n') {
                    c2 = read();
                    if (c2 == 'f') {
                        return (t = Type.kwNegInf);
                    }
                    this.unread(c2);
                    c2 = 'n';
                }
                this.unread(c2);
                c2 = 'i';
            }
            this.unread(c2);
            if (is_in_expression) {
                return scanOperator(c);
            }
            break; // break to error
        default:
            if (IonTextUtils.isIdentifierStart(c)) {
                return scanIdentifier(c);
            }
            if (is_in_expression && isOperatorPart(c)) {
                return scanOperator(c);
            }
        }

        String message =
            "Unexpected character " + printCodePointAsString(c) +
            " encountered at line " + this.getLineNumber() +
            " column " + this.getColumn();
        throw new IonException(message);
    }


    public Type scanIdentifier(int c) throws IOException
    {
        // reset our local value buffer
        resetValue();

        // we don't have an identifier type any longer, just string
        this.t = Type.constSymbol;

        // some strings are keywords, most are not
        // mostly we'll guess that it's not a keyword
        this.keyword = null;

        // first we read the identifier content into our value buffer
        // if the value is quoted, use the escape char loop, otherwise
        // look for the non-identifier character (and throw it back)

        if (!readIdentifierContents(c)) { // not quoted (true is quoted)
            // anything in quotes (even single quotes) is not a keyword
            // and here we're not in quoted content (that is handled above)
            // so see if it's also a keyword
            this.keyword = IonTokenReader.matchKeyword(value, 0, value.length());
            if (this.keyword != null) {
                if (this.keyword == Type.kwNull) {
                    c = this.read();
                    if (c == '.') {
                        int dot = value.length();
                        value.append((char)c);
                        c = this.read();
                        int added = readIdentifierContents(c, IonTokenConstsX.TN_MAX_NAME_LENGTH + 1); // +1 is "enough roap" so if there are extra letters at the end of the keyword we keep at least 1
                        int kw = IonTokenConstsX.keyword(value, dot+1, dot+added+1);
                        switch (kw) {
                        case IonTokenConstsX.KEYWORD_NULL:
                        case IonTokenConstsX.KEYWORD_BOOL:
                        case IonTokenConstsX.KEYWORD_INT:
                        case IonTokenConstsX.KEYWORD_FLOAT:
                        case IonTokenConstsX.KEYWORD_DECIMAL:
                        case IonTokenConstsX.KEYWORD_TIMESTAMP:
                        case IonTokenConstsX.KEYWORD_SYMBOL:
                        case IonTokenConstsX.KEYWORD_STRING:
                        case IonTokenConstsX.KEYWORD_BLOB:
                        case IonTokenConstsX.KEYWORD_CLOB:
                        case IonTokenConstsX.KEYWORD_LIST:
                        case IonTokenConstsX.KEYWORD_SEXP:
                        case IonTokenConstsX.KEYWORD_STRUCT:
                            this.keyword = setNullType(value, dot + 1, value.length() - dot - 1);
                            break;
                        default:
                            // not a valid type name - so we have to unread back to the dot
                            for (int ii=value.length(); ii>dot; ) {
                                ii--;
                                char uc = value.charAt(ii);
                                unread(uc);
                            }

                            String message =
                                position()
                                + ": Expected Ion type after 'null.' but found: "
                                + value;
                            throw new IonException(message);
                        }
                    }
                    else {
                        unread(c);
                    }
                }
                this.t = this.keyword;
                return this.t;
            }
        }

        // see if we're a user type of a member name
        c = this.readIgnoreWhitespace();
        if (c != ':') {
            unread(c);
        }
        else {
            c = read();
            if (c != ':') {
                unread(c);
                this.t = Type.constMemberName;
            }
            else {
                this.t = Type.constUserTypeDecl;
            }
        }
        return this.t;
    }

    boolean readIdentifierContents(int c) throws IOException {

        int quote = c;
        inQuotedContent = (quote == '\'' || quote == '\"');

        if ((quotedIdentifier = inQuotedContent) == true) {
            for (;;) {
                c = read();
                if (c < 0 || c == quote) break;
                if (c == '\\') {
                    c = readEscapedCharacter(this.in, false /*not clob*/);
                }
                if (c != EMPTY_ESCAPE_SEQUENCE) {
                    value.appendCodePoint(c);
                }
            }
            if (c == -1) { // TODO throw UnexpectedEofException
                throw new IonException("end encountered before closing quote '\\" + (char)endquote+ "'");
            }
            // c, at this point is a single quote, which we don't append
            inQuotedContent = false;
        }
        else {
            value.append((char)c);
            for (;;) {
                c = read();
                if (!IonTextUtils.isIdentifierPart(c)) {
                    break;
                }
                value.append((char)c);
            }
            unread(c);  // we throw back our terminator here
        }
        return quotedIdentifier;
    }
    int readIdentifierContents(int c, int max_length) throws IOException {
        assert(c != '\'' && c != '\"');

        value.append((char)c);
        int count = 1;

        while (count < max_length) {
            c = read();
            if (!IonTextUtils.isIdentifierPart(c)) {
                unread(c);  // we throw back our terminator here
                break;
            }
            value.append((char)c);
            count++;
        }

        return count;
    }

    static Type matchKeyword(StringBuilder sb, int pos, int valuelen) throws IOException
    {
        Type keyword = null;

        switch (sb.charAt(pos++)) { // there has to be at least 1 chacter or we wouldn't be here
        case 'f':
            if (valuelen == 5 //    "f"
             && sb.charAt(pos++) == 'a'
             && sb.charAt(pos++) == 'l'
             && sb.charAt(pos++) == 's'
             && sb.charAt(pos++) == 'e'
            ) {
                keyword = Type.kwFalse;
            }
            break;
        case 'n':
            if (valuelen == 4 //    'n'
             && sb.charAt(pos++) == 'u'
             && sb.charAt(pos++) == 'l'
             && sb.charAt(pos++) == 'l'
            ) {
                keyword = Type.kwNull;
            }
            else if (valuelen == 3 //   'n'
                 && sb.charAt(pos++) == 'a'
                 && sb.charAt(pos++) == 'n'
            ) {
                keyword = Type.kwNan;
            }
            break;
        case 't':
            if (valuelen == 4 //    "t"
             && sb.charAt(pos++) == 'r'
             && sb.charAt(pos++) == 'u'
             && sb.charAt(pos++) == 'e'
            ) {
                keyword =  Type.kwTrue;
            }
            break;
        }

        return keyword;
    }

    public Type setNullType(StringBuilder sb, int pos, int valuelen)
    {
        switch (valuelen) {
        case 3:
            //int
            if (sb.charAt(pos++) == 'i'
             && sb.charAt(pos++) == 'n'
             && sb.charAt(pos++) == 't'
            ) {
                return Type.kwNullInt;
            }
            break;
        case 4:
            //bool, blob, list, null, clob, sexp
            switch (sb.charAt(pos++)) {
            case 'b':
                //bool, blob
                switch (sb.charAt(pos++)) {
                    case 'o':
                        // bool
                        if (sb.charAt(pos++) == 'o'
                         && sb.charAt(pos++) == 'l'
                        ) {
                            return Type.kwNullBoolean;
                        }
                        break;
                    case 'l':
                        //blob
                        if (sb.charAt(pos++) == 'o'
                         && sb.charAt(pos++) == 'b'
                        ) {
                            return Type.kwNullBlob;
                        }
                        break;
                }
                break;
            case 'l':
                if (sb.charAt(pos++) == 'i'
                 && sb.charAt(pos++) == 's'
                 && sb.charAt(pos++) == 't'
                ) {
                    return Type.kwNullList;
                }
                break;
            case 'n':
                if (sb.charAt(pos++) == 'u'
                 && sb.charAt(pos++) == 'l'
                 && sb.charAt(pos++) == 'l'
                ) {
                    return Type.kwNullNull;
                }
                break;
            case 'c':
                //clob
                if (sb.charAt(pos++) == 'l'
                 && sb.charAt(pos++) == 'o'
                 && sb.charAt(pos++) == 'b'
                ) {
                    return Type.kwNullClob;
                }
                break;
            case 's':
                //sexp
                if (sb.charAt(pos++) == 'e'
                 && sb.charAt(pos++) == 'x'
                 && sb.charAt(pos++) == 'p'
                ) {
                    return Type.kwNullSexp;
                }
                break;
            }
            break;
        case 5:
            //float
            if (sb.charAt(pos++) == 'f'
             && sb.charAt(pos++) == 'l'
             && sb.charAt(pos++) == 'o'
             && sb.charAt(pos++) == 'a'
             && sb.charAt(pos++) == 't'
            ) {
                return Type.kwNullFloat;
            }
            break;
        case 6:
            switch (sb.charAt(pos++)) {
            case 's':
                //string
                //struct
                //symbol
                switch(sb.charAt(pos++)) {
                case 't':
                    if (sb.charAt(pos++) == 'r') {
                        switch (sb.charAt(pos++)) {
                        case 'i':
                            if (sb.charAt(pos++) == 'n' && sb.charAt(pos++) == 'g') {
                                return Type.kwNullString;
                            }
                            break;
                        case 'u':
                            if (sb.charAt(pos++) == 'c' && sb.charAt(pos++) == 't') {
                                return Type.kwNullStruct;
                            }
                            break;
                        }
                    }
                    break;
                case 'y':
                    if (sb.charAt(pos++) == 'm'
                     && sb.charAt(pos++) == 'b'
                     && sb.charAt(pos++) == 'o'
                     && sb.charAt(pos++) == 'l'
                    ) {
                        return Type.kwNullSymbol;
                    }
                    break;
                }
            }
            break;
        case 7:
            //decimal
            if (sb.charAt(pos++) == 'd'
             && sb.charAt(pos++) == 'e'
             && sb.charAt(pos++) == 'c'
             && sb.charAt(pos++) == 'i'
             && sb.charAt(pos++) == 'm'
             && sb.charAt(pos++) == 'a'
             && sb.charAt(pos++) == 'l'
            ) {
                return Type.kwNullDecimal;
            }
            break;
        case 9:
            //timestamp
            if (sb.charAt(pos++) == 't'
             && sb.charAt(pos++) == 'i'
             && sb.charAt(pos++) == 'm'
             && sb.charAt(pos++) == 'e'
             && sb.charAt(pos++) == 's'
             && sb.charAt(pos++) == 't'
             && sb.charAt(pos++) == 'a'
             && sb.charAt(pos++) == 'm'
             && sb.charAt(pos++) == 'p'
            ) {
                return Type.kwNullTimestamp;
            }
            break;
        }
        String nullimage = sb.toString();
        throw new IonException("invalid null value '"+nullimage+"' at " + this.position());
    }

    public Type scanOperator(int c) throws IOException
    {
        // reset our local value buffer
        resetValue();

        // we don't have an identifier type any longer, just string
        this.t = Type.constSymbol;

        // some strings are keywords, most are not
        // mostly we'll guess that it's not a keyword
        this.keyword = null;

        // first we read the identifier content into our value buffer
        // if the value is quoted, use the escape char loop, otherwise
        // look for the non-identifier character (and throw it back)

        value.append((char)c);
        for (;;) {
            c = read();
            if (!IonTextUtils.isOperatorPart(c)) {
                break;
            }
            value.append((char)c);
        }
        unread(c);  // we throw back our terminator here

        return this.t;
    }


    public Type scanString(int c, int maxlookahead) throws IOException
    {
        // reset out local value buffer
        resetValue();

        if (c != '\"') {
            throw new IonException("Programmer error! Only a quote should get you here.");
        }
        endquote = '\"';

        sizedloop:
        while (maxlookahead-- > 0) {
            switch ((c = this.read())) {
            case -1:   break sizedloop; // TODO deoptimize, throw exception
            case '\"': break sizedloop;
            case '\n':
                throw new IonException("unexpected line terminator encountered in quoted string");
            case '\\':
                c = readEscapedCharacter(this.in, false /*not clob*/);
                //    throws UnexpectedEofException on EOF
                if (c != EMPTY_ESCAPE_SEQUENCE) {
                    value.appendCodePoint(c);
                }
                break;
            default:
                // c could be part of a surrogate pair so we can't use
                // appendCodePoint()
                value.append((char)c);
                break;
            }
        }

        if (maxlookahead != -1 && c == '\"') {
            // this is the normal, non-longline case so we're just done
            closeString();
        }
        else {
            // we're not at the closing quote, so this is an incomplete
            // string which will have to be read to the end ... later
            leaveOpenString(c, false);
        }
        return Type.constString;
    }
    void leaveOpenString(int c, boolean longstring) {
        if (c == -1) {
            throw new UnexpectedEofException();
        }

        this.isIncomplete = true;
        this.inQuotedContent = true;
        this.isLongString = longstring;
    }

    /**
     * Scans the rest of the string through {@link #endquote}.
     * <p>
       XXX  WARNING  XXX
     * Almost identical logic is found in
     * {@link IonBinary#appendToLongValue(int, boolean, boolean, PushbackReader)}
     *
     * @param is_in_expression
     * @throws UnexpectedEofException if we hit EOF before the endquote.
     */
    void finishScanString(boolean is_in_expression) throws IOException
    {
        assert isIncomplete;
        assert inQuotedContent;

        for (;;)
        {
            int c = this.read();
            if (c == -1) {
                throw new UnexpectedEofException();
            }
            if (c == endquote) break;  // endquote == -1 during long strings
            if (c == '\\') {
                c = readEscapedCharacter(this.in, false /*not clob*/);
                if (c != EMPTY_ESCAPE_SEQUENCE) {
                    value.appendCodePoint(c);
                }
            }
            else if (c == '\'' && isLongString) {
                // This happens while handling triple-quote field names
                if (twoMoreSingleQuotes()) {
                    // At the end of a long string.  Look for another one.
                    inQuotedContent = false;
                    c = readIgnoreWhitespace();

                    if (c == '\'' && twoMoreSingleQuotes()) {
                        // Yep, another triple-quote.  We've consumed all three
                        // so just keep reading.
                        inQuotedContent = true;
                    }
                    else {
                        // Something else, we are done.
                        unread(c);
                        break;
                    }
                }
                else {
                    // Still inside the long string
                    value.append((char)c);
                }
            }
            else if (!isLongString && (c == '\n')) {
                throw new IonException("unexpected line terminator encountered in quoted string");
            }
            else {
                value.append((char)c);
            }
        }
        return;
    }

    /**
     * If two single quotes are next on the input, consume them and return
     * true.  Otherwise, leave them on the input and return false.
     * @return true when there are two pending single quotes.
     * @throws IOException
     */
    private boolean twoMoreSingleQuotes() throws IOException
    {
        int c = read();
        if (c == '\'') {
            int c2 = read();
            if (c2 == '\'') {
                return true;
            }
            unread(c2);
        }
        unread(c);
        return false;
    }


    void closeString() {
        this.isIncomplete = false;
        this.inQuotedContent = false;
        this.isLongString = false;
    }

    public Type scanLongString() throws IOException
    {
        // reset out local value buffer
        resetValue();

        endquote = -1;

        // we're not at the closing quote, so this is an incomplete
        // string which will have to be read to the end ... later
        leaveOpenString(EMPTY_ESCAPE_SEQUENCE, true);

        return Type.constString;
    }
    /**
     * Closes parsing of a triple-quote string, remembering that there may be
     * more to come.
     */
    void closeLongString() {
        this.isIncomplete = true;
        this.inQuotedContent = false;
        this.isLongString = true;
    }


    /**
     * @return the translated escape sequence.  Will not return -1 since EOF is
     * not legal in this context. May return {@link #EMPTY_ESCAPE_SEQUENCE} to
     * indicate a zero-length sequence such as BS-NL.
     * @throws UnexpectedEofException if the EOF is encountered in the middle
     * of the escape sequence.
     * */
    public static int readEscapedCharacter(PushbackReader r, boolean inClob)
        throws IOException, UnexpectedEofException
    {
        //    \\ The backslash character
        //    \0 The character 0 (nul terminator char, for some)
        //    \xhh The character with hexadecimal value 0xhh
        //    \ u hhhh The character with hexadecimal value 0xhhhh
        //    \t The tab character ('\u0009')
        //    \n The newline (line feed) character ('\ u 000A')
        //    \v The vertical tab character ('\u000B')
        //    \r The carriage-return character ('\ u 000D')
        //    \f The form-feed character ('\ u 000C')
        //    \a The alert (bell) character ('\ u 0007')
        //    \" The double quote character
        //    \' The single quote character
        //    \? The question mark character

        int c2 = 0, c = r.read();
        switch (c) {
            case -1:
                throw new UnexpectedEofException();
            case 't':  return '\t';
            case 'n':  return '\n';
            case 'v':  return '\u000B';
            case 'r':  return '\r';
            case 'f':  return '\f';
            case 'b':  return '\u0008';
            case 'a':  return '\u0007';
            case '\\': return '\\';
            case '\"': return '\"';
            case '\'': return '\'';
            case '/':  return '/';
            case '?':  return '?';

            case 'U':
                // Expecting 8 hex digits
                if (inClob) {
                    throw new IonException("Unicode escapes \\U not allowed in clob");
                }
                c = readDigit(r, 16, true);
                if (c < 0) break;  // TODO throw UnexpectedEofException
                c2 = c << 28;
                c = readDigit(r, 16, true);
                if (c < 0) break;
                c2 += c << 24;
                c = readDigit(r, 16, true);
                if (c < 0) break;
                c2 += c << 20;
                c = readDigit(r, 16, true);
                if (c < 0) break;
                c2 += c << 16;
                // ... fall through...
            case 'u':
                // Expecting 4 hex digits
                if (inClob) {
                    throw new IonException("Unicode escapes \\u not allowed in clob");
                }
                c = readDigit(r, 16, true);
                if (c < 0) break;
                c2 += c << 12;
                c = readDigit(r, 16, true);
                if (c < 0) break;
                c2 += c << 8;
                // ... fall through...
            case 'x':
                // Expecting 2 hex digits
                c = readDigit(r, 16, true);
                if (c < 0) break;
                c2 += c << 4;
                c = readDigit(r, 16, true);
                if (c < 0) break;
                return c2 + c;

            case '0':
                return 0;
            case '\n':
                return EMPTY_ESCAPE_SEQUENCE;
            default:
                break;
        }
        throw new IonException("invalid escape sequence \"\\"
                               + (char) c + "\" [" + c + "]");
    }

    public static int readDigit(/*EscapedCharacterReader*/PushbackReader r, int radix, boolean isRequired) throws IOException {
        int c = r.read();
        if (c < 0) {
            r.unread(c);
            return -1;
        }

        int c2 =  Character.digit(c, radix);
        if (c2 < 0) {
            if (isRequired) {
                throw new IonException("bad digit in escaped character '"+((char)c)+"'");
            }
            // if it's not a required digit, we just throw it back
            r.unread(c);
            return -1;
        }
        return c2;
    }


    private void checkAndUnreadNumericStopper(int c) throws IOException
    {
        // Ignore EOF, so we don't "unread" it - EOF is a terminator
        if (c != -1) {
            if ( ! this.isValueTerminatingCharacter(c) ) {
                final String message =
                    position() + ": Numeric value followed by invalid character "
                    + printCodePointAsString(c);
                throw new IonException(message);
            }
            this.unread(c);
        }
    }

    private final boolean isValueTerminatingCharacter(int c) throws IOException
    {
        boolean isTerminator;

        if (c == '/') {
            // this is terminating only if it starts a comment of some sort
            c = this.read();
            this.unread(c);  // we never "keep" this character
            isTerminator = (c == '/' || c == '*');
        }
        else {
            isTerminator = IonTextUtils.isNumericStop(c);
        }

        return isTerminator;
    }

    public Type readNumber(int c) throws IOException {
        // clear out our string buffer
        resetValue();

        // process the initial sign character, if its there.
        boolean explicitPlusSign = false;
        switch (c) {
        case '-':
            value.append((char)c);
            c = this.read();
            t = Type.constNegInt;
            this.numberType = NumberType.NT_NEGINT;
            this.isNegative = true;
            break;
        case '+':
            // we eat the plus sign, but remember we saw one
            explicitPlusSign = true;
            c = this.read();
            t = Type.constPosInt;
            this.isNegative = false;
            this.numberType = NumberType.NT_POSINT;
            break;
        default:
            t = Type.constPosInt;
            this.numberType = NumberType.NT_POSINT;
            this.isNegative = false;
            break;
        }

        // process the initial digit.  Caller has checked that it's a digit.
        assert isDigit(c, 10);
        value.append((char)c);
        boolean isZero = (c == '0');
        boolean leadingZero = isZero;

        // process the next character after the initial digit.
        switch((c = this.read())) {
        case 'x':
        case 'X':
            if (!isZero) {
                throw new IonException("badly formed number encountered at " + position());
            }
            this.numberType = NumberType.NT_HEX;
            // We don't need to append the char because we're going to wipe
            // the value anyway to accumulate just the hex digits.
            return scanHexNumber();
        case '.':
        case 'd':
        case 'D':
            t = Type.constDecimal;
            this.numberType = NumberType.NT_DECIMAL;
            value.append((char)c);
            break;
        case 'e':
        case 'E':
            t = Type.constFloat;
            this.numberType = NumberType.NT_FLOAT;
            value.append((char)c);
            break;
        default:
            if (!isDigit(c, 10)) {
                checkAndUnreadNumericStopper(c);
                if (isZero && NumberType.NT_NEGINT.equals(this.numberType)) {
                    t = Type.constPosInt;
                    this.numberType = NumberType.NT_POSINT;
                }
                return t;
            }
            value.append((char)c);
            isZero &= (c == '0');
            break;
        }

        // see if see can continue find leading digits
        if (NumberType.NT_NEGINT.equals(numberType)
         || NumberType.NT_POSINT.equals(numberType)
        ) {
            // We've now scanned at least two digits.
            // read in the remaining whole number digits
            for (;;) {
                c = this.read();
                if (!isDigit(c, 10)) break;
                value.append((char)c);
                isZero &= (c == '0');
            }

            // and see what we ran aground on ..
            switch (c) {
            case '.':
            case 'd':
            case 'D':
                if (leadingZero) {
                    throw new IonException(position() + ": Invalid leading zero on numeric");
                }
                t = Type.constDecimal;
                this.numberType = NumberType.NT_DECIMAL;
                value.append((char)c);
                break;
            case 'e':
            case 'E':
                if (leadingZero) {
                    throw new IonException(position() + ": Invalid leading zero on numeric");
                }
                t = Type.constFloat;
                this.numberType = NumberType.NT_FLOAT;
                value.append((char)c);
                break;
            case 'T':  // same as '-' it's a timestamp
            case '-':
                if (NumberType.NT_POSINT.equals(this.numberType) && !explicitPlusSign) {
                    return scanTimestamp(c);
                }
                // otherwise fall through to "all other" processing
            default:
                // Hit the end of our integer.  Make sure its a valid stopper.
                checkAndUnreadNumericStopper(c);

                if (leadingZero && !isZero) {
                    throw new IonException(position() + ": Invalid leading zero on numeric");
                }
                if (isZero && NumberType.NT_NEGINT.equals(this.numberType)) {
                    t = Type.constPosInt;
                    this.numberType = NumberType.NT_POSINT;
                }
                return t;
            }
        }

        // now we must have just decimal or float
        // if it's decimal we ended on a DOT, so get the trailing
        // (fractional) digits
        if (NumberType.NT_DECIMAL.equals(this.numberType)) {
            for (;;) {
                c = this.read();
                if (!isDigit(c, 10)) break;
                value.append((char)c);
            }
            // and see what we ran aground on this time..
            switch (c) {
            case '-':
            case '+':
                // We'll handle exponent below.
                this.unread(c);
                break;
            case 'e':
            case 'E':
                // this is the only viable option now
                t = Type.constFloat;
                this.numberType = NumberType.NT_FLOAT;
                value.append((char)c);
                break;
            case 'd':
            case 'D':
                // this is the only viable option now
                assert t == Type.constDecimal;
                assert NumberType.NT_DECIMAL.equals(this.numberType);
                value.append((char)c);
                break;
            default:
                checkAndUnreadNumericStopper(c);
                return t;
            }
        }

        // now we have the exponent part to read
        // reading the first character of the exponent, it can be
        // a sign character or a digit (this is the only place
        // the sign is valid) and we're for sure a float at this point
        switch ((c = this.read())) {
        case '-':
            value.append((char)c);
            break;
        case '+':
            // we eat the plus sign
            break;
        default:
            if (!isDigit(c, 10)) {
                // this they said 'e' then they better put something valid after it!
                throw new IonException("badly formed number encountered at " + position());
            }
            this.unread(c);
            // we'll re-read this in just a bit, but we had to check
            break;
        }

        // read in the remaining whole number digits and then quit
        for (;;) {
            c = this.read();
            if (!isDigit(c, 10)) break;
            value.append((char)c);
        }

        checkAndUnreadNumericStopper(c);

        return t;
    }

    Type scanHexNumber() throws IOException
    {
        boolean anydigits = false;
        int c;

        boolean isZero = true;

        // Leave the 0x in the buffer since we'll need it to parse the content.
        // first get clear out our string buffer, we're starting over
        value.setLength(0);  // resetValue() wipes out too much

        // read in the remaining whole number digits and then quit
        for (;;) {
            c = this.read();
            if (!isDigit(c, 16)) break;
            anydigits = true;
            isZero &= (c == '0');
            value.append((char)c);
        }
        if (!anydigits) {
            throw new IonException("badly formed hexadecimal number encountered at " + position());
        }

        checkAndUnreadNumericStopper(c);
        if (isZero && t == Type.constNegInt) {
            t = Type.constPosInt;
        }

        // as long as we saw any digit, we're good
        return t;
    }

    /**
     * Scans a timestamp after reading <code>yyyy-</code>.
     *
     * We can be a little lenient here since the result will be reparsed and
     * validated more thoroughly by {@link Timestamp#valueOf(CharSequence)}.
     *
     * @param c the last character scanned; must be <code>'-'</code>.
     * @return {@link Type#constTime}
     */
    Type scanTimestamp(int c) throws IOException {

endofdate:
        for (;;) {  // fake for loop to create a label we can jump out of,
                    // because 4 or 5 levels of nested if's is just ugly

            // at this point we will have read leading digits and exactly 1 dash
            // in other words, we'll have read the year
            if (c == 'T') {
                // yearT is a valid timestamp value
                value.append((char)c);
                c = this.read(); // because we'll unread it before we return
                break endofdate;
            }
            if (c != '-') {
                // not a dash or a T after the year - so this is a bad value
                throw new IllegalStateException("invalid timestamp, expecting a dash here at " + this.position());
            }

            // append the dash and then read the month field
            value.append((char)c); // so append it, because we haven't already
            c = readDigits(2, "month");
            if (c == 'T') {
                // year-monthT is a valid timestamp value
                value.append((char)c);
                c = this.read(); // because we'll unread it before we return
                break endofdate;
            }
            if (c != '-') {
                // if the month isn't followed by a dash or a T it's an invalid month
                throw new IonException("invalid timestamp, expecting month at " + this.position());
            }

            // append the dash and read the day (or day-of-month) field
            value.append((char)c);
            c = readDigits(2, "day of month");
            if (c == 'T') {

check4timezone:
                for (;;) { // another fake label/ for=goto

                    // attach the 'T' to the value we're collecting
                    value.append((char)c);

                    // we're going to "watch" how many digits we read in the hours
                    // field.  It's 0 that's actually ok, since we can end at the
                    // 'T' we just read
                    int length_before_reading_hours = value.length();
                    // so read the hours
                    c = readDigits(2, "hours");
                    if (length_before_reading_hours == value.length()) {
                        // FIXME I don't think there should be a timezone here
                        break check4timezone;
                    }
                    if (c != ':') {
                        throw new IonException("invalid timestamp, expecting hours at " + this.position());
                    }
                    value.append((char)c);
                    // so read the minutes
                    c = readDigits(2, "minutes");
                    if (c != ':') {
                        if (c == '-' || c == '+' || c == 'Z') {
                            break check4timezone;
                        }
                        break endofdate;
                    }
                    value.append((char)c);
                    // so read the seconds
                    c = readDigits(2, "seconds");
                    if (c != '.') {
                        if (c == '-' || c == '+' || c == 'Z') {
                            break check4timezone;
                        }
                        break endofdate;
                    }
                    value.append((char)c);
                    // so read the fractional seconds
                    c = readDigits(32, "fractional seconds");
                    break check4timezone;
                }//check4timezone

                // now check to see if it's a timezone offset we're looking at
                if (c == '-' || c == '+') {
                    value.append((char)c);
                    // so read the timezone offset
                    c = readDigits(2, "timezone offset");
                    if (c != ':') break endofdate;
                    value.append((char)c);
                    c = readDigits(2, "timezone offset");
                }
                else if (c == 'Z') {
                    value.append((char)c);
                    c = this.read(); // because we'll unread it before we return
                }
            }
            break endofdate;
        }//endofdate

        checkAndUnreadNumericStopper(c);

        return Type.constTime;
    }

    int readDigits(int limit, String field) throws IOException {
        int c, len = 0;
        // read in the remaining whole number digits and then quit
        for (;;) {
            c = this.read();
            if (!isDigit(c, 10)) break;
            len++;
            if (len > limit) {
                throw new IonException("invalid format " + field + " too long at " + this.position());
            }
            value.append((char)c);
        }
        return c;
    }

    public boolean makeValidNumeric(Type castto) throws IOException {
        String  s = getValueString(false);

        try
        {
            this.t = castto.setNumericValue(this, s);
        }
        catch (NumberFormatException e)
        {
            throw new IonException(this.position() + ": invalid numeric value " + s, e);
        }

        return this.t.isNumeric();
    }

}
