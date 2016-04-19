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

import software.amazon.ion.IonException;
import software.amazon.ion.IonType;
import software.amazon.ion.impl.PrivateScalarConversions.CantConvertException;


/**
 * this is a collection of constants and some static helper functions
 * to support tokenizing Ion text
 */
final class IonTokenConstsX
{
    public static class CharacterSequence {
        public static final int CHAR_SEQ_EOF                         = -1; // matches -1 (stream eof)
        public static final int CHAR_SEQ_STRING_TERMINATOR           = -2; // can't be >=0, ==-1 (eof), nor -2 (empty esc)
        public static final int CHAR_SEQ_STRING_NON_TERMINATOR       = -3; // used for a pair of triple quotes treated a nothing

        public static final int CHAR_SEQ_NEWLINE_SEQUENCE_1          = -4;  // single new line
        public static final int CHAR_SEQ_NEWLINE_SEQUENCE_2          = -5;  // single carriage return
        public static final int CHAR_SEQ_NEWLINE_SEQUENCE_3          = -6;  // new line - carriage return pair
        public static final int CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_1  = -7;  // escape followed by new line
        public static final int CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_2  = -8;  // escape followed by carriage return
        public static final int CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_3  = -9;  // escape followed by new line - carriage return pair
    }

    public static final int TOKEN_ERROR                 = -1;
    public static final int TOKEN_EOF                   =  0;

    public static final int TOKEN_UNKNOWN_NUMERIC       =  1;
    public static final int TOKEN_INT                   =  2;
    public static final int TOKEN_HEX                   =  3;
    public static final int TOKEN_DECIMAL               =  4;
    public static final int TOKEN_FLOAT                 =  5;
    public static final int TOKEN_FLOAT_INF             =  6;
    public static final int TOKEN_FLOAT_MINUS_INF       =  7;
    public static final int TOKEN_TIMESTAMP             =  8;

    /**
     * Unquoted identifier symbol, including keywords like {@code true} and
     * {@code nan} as well as SIDs like {@code $123}
     */
    public static final int TOKEN_SYMBOL_IDENTIFIER     =  9;
    /** Single-quoted symbol */
    public static final int TOKEN_SYMBOL_QUOTED         = 10;
    /** Unquoted operator sequence for sexp */
    public static final int TOKEN_SYMBOL_OPERATOR       = 11;

    public static final int TOKEN_STRING_DOUBLE_QUOTE   = 12;
    public static final int TOKEN_STRING_TRIPLE_QUOTE   = 13;

    public static final int TOKEN_DOT                   = 14;
    public static final int TOKEN_COMMA                 = 15;
    public static final int TOKEN_COLON                 = 16;
    public static final int TOKEN_DOUBLE_COLON          = 17;

    public static final int TOKEN_OPEN_PAREN            = 18;
    public static final int TOKEN_CLOSE_PAREN           = 19;
    public static final int TOKEN_OPEN_BRACE            = 20;
    public static final int TOKEN_CLOSE_BRACE           = 21;
    public static final int TOKEN_OPEN_SQUARE           = 22;
    public static final int TOKEN_CLOSE_SQUARE          = 23;

    public static final int TOKEN_OPEN_DOUBLE_BRACE     = 24;
    public static final int TOKEN_CLOSE_DOUBLE_BRACE    = 25;

    public static final int TOKEN_BINARY                = 26;

    public static final int TOKEN_MAX                   = 26;
    public static final int TOKEN_count                 = 27;

    public static final int KEYWORD_unrecognized = -1;
    public static final int KEYWORD_none         =  0;
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
    public static final int KEYWORD_NAN       = 16;
    public static final int KEYWORD_sid       = 17;

    public final static String getTokenName(int t) {
        switch (t) {
        case TOKEN_ERROR:              return "TOKEN_ERROR";
        case TOKEN_EOF:                return "TOKEN_EOF";

        case TOKEN_UNKNOWN_NUMERIC:    return "TOKEN_UNKNOWN_NUMERIC";
        case TOKEN_INT:                return "TOKEN_INT";
        case TOKEN_HEX:                return "TOKEN_HEX";
        case TOKEN_DECIMAL:            return "TOKEN_DECIMAL";
        case TOKEN_FLOAT:              return "TOKEN_FLOAT";
        case TOKEN_FLOAT_INF:          return "TOKEN_FLOAT_INF";
        case TOKEN_FLOAT_MINUS_INF:    return "TOKEN_FLOAT_MINUS_INF";
        case TOKEN_TIMESTAMP:          return "TOKEN_TIMESTAMP";

        case TOKEN_SYMBOL_IDENTIFIER:  return "TOKEN_SYMBOL_IDENTIFIER";
        case TOKEN_SYMBOL_QUOTED:      return "TOKEN_SYMBOL_QUOTED";
        case TOKEN_SYMBOL_OPERATOR:    return "TOKEN_SYMBOL_OPERATOR";
        case TOKEN_STRING_DOUBLE_QUOTE:return "TOKEN_STRING_DOUBLE_QUOTE";
        case TOKEN_STRING_TRIPLE_QUOTE:return "TOKEN_STRING_TRIPLE_QUOTE";

        case TOKEN_DOT:                return "TOKEN_DOT";
        case TOKEN_COMMA:              return "TOKEN_COMMA";
        case TOKEN_COLON:              return "TOKEN_COLON";
        case TOKEN_DOUBLE_COLON:       return "TOKEN_DOUBLE_COLON";

        case TOKEN_OPEN_PAREN:         return "TOKEN_OPEN_PAREN";
        case TOKEN_CLOSE_PAREN:        return "TOKEN_CLOSE_PAREN";
        case TOKEN_OPEN_BRACE:         return "TOKEN_OPEN_BRACE";
        case TOKEN_CLOSE_BRACE:        return "TOKEN_CLOSE_BRACE";
        case TOKEN_OPEN_SQUARE:        return "TOKEN_OPEN_SQUARE";
        case TOKEN_CLOSE_SQUARE:       return "TOKEN_CLOSE_SQUARE";

        case TOKEN_OPEN_DOUBLE_BRACE:  return "TOKEN_OPEN_DOUBLE_BRACE";
        case TOKEN_CLOSE_DOUBLE_BRACE: return "TOKEN_CLOSE_DOUBLE_BRACE";

        default: return "<invalid token "+t+">";
        }
    }
    public final static String describeToken(int t) {
        switch (t) {
        case TOKEN_OPEN_PAREN:         return "(";
        case TOKEN_CLOSE_PAREN:        return ")";
        case TOKEN_OPEN_BRACE:         return "{";
        case TOKEN_CLOSE_BRACE:        return "}";
        case TOKEN_OPEN_SQUARE:        return "[";
        case TOKEN_CLOSE_SQUARE:       return "]";

        case TOKEN_OPEN_DOUBLE_BRACE:  return "{{";
        case TOKEN_CLOSE_DOUBLE_BRACE: return "}}";

        default: return getTokenName(t);
        }
    }
    public static final IonType ion_type_of_scalar(int token) {
        switch(token) {
        case TOKEN_INT:                 return IonType.INT;
        case TOKEN_BINARY:              return IonType.INT;
        case TOKEN_HEX:                 return IonType.INT;
        case TOKEN_DECIMAL:             return IonType.DECIMAL;
        case TOKEN_FLOAT:               return IonType.FLOAT;
        case TOKEN_TIMESTAMP:           return IonType.TIMESTAMP;
        case TOKEN_SYMBOL_IDENTIFIER:   return IonType.SYMBOL;
        case TOKEN_SYMBOL_QUOTED:       return IonType.SYMBOL;
        case TOKEN_SYMBOL_OPERATOR:     return IonType.SYMBOL;
        case TOKEN_STRING_DOUBLE_QUOTE: return IonType.STRING;
        case TOKEN_STRING_TRIPLE_QUOTE: return IonType.STRING;
        default:                        return null;
        }
    }

    public static final char[] BLOB_TERMINATOR               = new char[]  { '}', '}' };
    public static final char[] CLOB_DOUBLE_QUOTED_TERMINATOR = new char[]  { '\'', '\'', '\'' };
    public static final char[] CLOB_TRIPLE_QUOTED_TERMINATOR = new char[]  { '"' };

    public static final boolean is8bitValue(int v) {
        return (v & ~0xff) == 0;
    }
    public static final boolean is7bitValue(int v) {
        return (v & ~0x7f) == 0;
    }
    public static final boolean isWhitespace(int c) {
        return (c == ' ' || c == '\t' || c == '\n' || c == '\r');
    }

    public final static boolean[] isBase64Character = makeBase64Array();
    public final static int       base64FillerCharacter = '=';
    private static boolean[] makeBase64Array()
    {
        boolean[] base64 = new boolean[256];

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

    public final static int[] hexValue = makeHexValueArray();
    public final static boolean[] isHexDigit = makeHexDigitTestArray(hexValue);
    private final static int[] makeHexValueArray() {
        int[] hex = new int[256];
        for (int ii=0; ii<256; ii++) {
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
    private final static boolean[] makeHexDigitTestArray(int [] hex_characters) {
        boolean[] is_hex = new boolean[hex_characters.length];
        for (int ii=0; ii<hex_characters.length; ii++) {
            is_hex[ii] = (hex_characters[ii] >= 0);
        }
        return is_hex;
    }

    public static final boolean isBinaryDigit(int c) {
        return c =='0' || c == '1';
    }

    public final static boolean isHexDigit(int c) {
        return isHexDigit[c & 0xff] && is8bitValue(c);
    }

    public final static int hexDigitValue(int c) {
        if (!isHexDigit(c)) {
            IllegalArgumentException e = new IllegalArgumentException("character '"+((char)c)+"' is not a hex digit");
            throw new IonException(e);
        }
        return hexValue[c];
    }
/*
    public final static int[] decimalValue = makeDecimalValueArray();
    public final static boolean[] isDecimalDigit = makeDecimalDigitTestArray(decimalValue);
    private final static int[] makeDecimalValueArray() {
        int[] dec = new int[256];
        for (int ii=0; ii<256; ii++) {
            dec[ii] = -1;
        }
        for (int ii='0'; ii<='9'; ii++) {
            dec[ii] = ii - '0';
        }
        return dec;
    }
    private final static boolean[] makeDecimalDigitTestArray(int [] dec_characters) {
        boolean[] is_hex = new boolean[dec_characters.length];
        for (int ii=0; ii<dec_characters.length; ii++) {
            is_hex[ii] = (dec_characters[ii] >= 0);
        }
        return is_hex;
    }
*/
    public final static boolean isDigit(int c) {
        return (c >= '0' && c <= '9');
    }
    public final static int decimalDigitValue(int c) {
        if (!isDigit(c)) {
            throw new IllegalArgumentException("character '"+((char)c)+"' is not a hex digit");
        }
        return c - '0'; // decimalValue[c];
    }

    public static final int CLOB_CHARACTER_LIMIT = 0xFF;
    public static final int ESCAPE_LITTLE_U_MINIMUM = 0x100;
    public static final int ESCAPE_BIG_U_MINIMUM = 0x10000;

    public static final int ESCAPE_HEX = -16;
    public static final int ESCAPE_BIG_U = -15;
    public static final int ESCAPE_LITTLE_U = -14;
    public static final int ESCAPE_REMOVES_NEWLINE2 = -13;
    public static final int ESCAPE_REMOVES_NEWLINE = -12;
    public static final int ESCAPE_NOT_DEFINED = -11;

    private static final int escapeCharactersValues[] = makeEscapeCharacterValuesArray();
    private static final int[] makeEscapeCharacterValuesArray() {
        int[] values = new int[256];
        for (int ii=0; ii<256; ii++) {
            values[ii] = ESCAPE_NOT_DEFINED;
        }
        values['0'] = 0;        //    \u0000  \0  alert NUL
        values['a'] = 7;        //    \u0007  \a  alert BEL
        values['b'] = 8;        //    \u0008  \b  backspace BS
        values['t'] = 9;        //    \u0009  \t  horizontal tab HT
        values['n'] = '\n';     //    \ u000A  \ n  linefeed LF
        values['f'] = 0x0c;     //    \u000C  \f  form feed FF
        values['r'] = '\r';     //    \ u000D  \ r  carriage return CR
        values['v'] = 0x0b;     //    \u000B  \v  vertical tab VT
        values['"'] = '"';      //    \u0022  \"  double quote
        values['\''] = '\'';    //    \u0027  \'  single quote
        values['?'] = '?';      //    \u003F  \?  question mark
        values['\\'] = '\\';    //    \u005C  \\  backslash
        values['/'] = '/';      //    \u002F  \/  forward slash nothing  \NL  escaped NL expands to nothing
        values['\n'] = ESCAPE_REMOVES_NEWLINE;  // slash-new line the new line eater
        values['\r'] = ESCAPE_REMOVES_NEWLINE2;  // slash-new line the new line eater
        values['x'] = ESCAPE_HEX; //    any  \xHH  2-digit hexadecimal unicode character equivalent to \ u00HH
        values['u'] = ESCAPE_LITTLE_U; //    any  \ uHHHH  4-digit hexadecimal unicode character
        values['U'] = ESCAPE_BIG_U;
        return values;
    }
    private static final String escapeCharacterImage[] = makeEscapeCharacterImageArray();
    public final static String [] makeEscapeCharacterImageArray() {
        String [] values = new String[256];

        for (int ii=0; ii<256; ii++) {
            values[ii] = null;
        }

        values['0'] = "\\0";        //    \u0000  \0  alert NUL
        values['a'] = "\\a";        //    \u0007  \a  alert BEL
        values['b'] = "\\b";        //    \u0008  \b  backspace BS
        values['t'] = "\\t";        //    \u0009  \t  horizontal tab HT
        values['n'] = "\\n";        //    \ u000A \n line feed LF
        values['f'] = "\\f";        //    \u000C  \f  form feed FF
        values['r'] = "\\r";        //    \ u000D \r  carriage return CR
        values['v'] = "\\v";        //    \u000B  \v  vertical tab VT
        values['"'] = "\\\"";       //    \u0022  \"  double quote
        values['\''] = "\\"+"'";    //    \u0027  \'  single quote
        values['?'] = "\\?";        //    \u003F  \?  question mark
        values['\\'] = "\\"+"\\";   //    \u005C  \\  backslash
        values['/'] = "\\/";        //    \u002F  \/  forward slash nothing  \NL  escaped NL expands to nothing

        return values;
    }
    public final static String getEscapeCharacterImage(int c) {
        if (c < 0 || c > 255) {
            throw new IllegalArgumentException("character is outside escapable range (0-255 inclusive)");
        }
        return escapeCharacterImage[c];
    }

    public final static boolean isValidEscapeStart(int c) {
        return (escapeCharactersValues[c & 0xff] != ESCAPE_NOT_DEFINED)
         && is8bitValue(c);
    }
    public final static int escapeReplacementCharacter(int c) {
        if (!isValidEscapeStart(c)) {
            throw new IllegalArgumentException("not a valid escape sequence character: "+c);
        }
        return escapeCharactersValues[c];
    }

    // escapeType
    public enum EscapeType {
                    ESCAPE_DESTINATION_NONE,
                    ESCAPE_DESTINATION_STRING,
                    ESCAPE_DESTINATION_SYMBOL,
                    ESCAPE_DESTINATION_CLOB
    }
    public final static boolean needsIonEscape(EscapeType escapeType, int c) {
        switch(escapeType) {
        case ESCAPE_DESTINATION_NONE:   return false;
        case ESCAPE_DESTINATION_STRING: return needsStringEscape(c);
        case ESCAPE_DESTINATION_SYMBOL: return needsSymbolEscape(c);
        case ESCAPE_DESTINATION_CLOB:   return needsClobEscape(c);
        default:                        throw new IllegalArgumentException("escapeType "+escapeType+" is unrecognized");
        }
    }
    public final static boolean needsSymbolEscape(int c) {
        return (c < 32 || c == '\'' || c == '\\');
    }
    public final static boolean needsStringEscape(int c) {
        return (c < 32 || c == '"' || c == '\\');
    }
    public final static boolean needsClobEscape(int c) {
        return (c < 32 || c == '"' || c == '\\' || c > 127);
    }
    public static String escapeSequence(int c) {
        if (c >= 0 || c <= 0x10FFFF) {
            if (c < 128) {
                return escapeCharacterImage[c];
            }
            if (c < 0xFFFF) {
                String short_hex = Integer.toHexString(c);
                int    short_len = short_hex.length();
                if (short_len < 4) {
                    short_hex = "0000".substring(short_len);
                }
                return "\\u"+short_hex;
            }
            if (c < 0xFFFF) {
                String long_hex = Integer.toHexString(c);
                int    long_len = long_hex.length();
                if (long_len < 4) {
                    long_hex = "00000000".substring(long_len);
                }
                return "\\U"+long_hex;
            }
        }
        throw new IllegalArgumentException("the value "+c+" isn't a valid character");
    }

    private static final boolean invalidTerminatingCharsForInf[] = makeInvalidTerminatingCharsForInfArray();
    private static final boolean [] makeInvalidTerminatingCharsForInfArray() {
        boolean [] values = new boolean [256];

        for (int ii='a'; ii<='z'; ii++) {
            values[ii] = true;
        }
        for (int ii='A'; ii<='Z'; ii++) {
            values[ii] = true;
        }
        for (int ii='0'; ii<='9'; ii++) {
            values[ii] = true;
        }
        values['$'] = true;
        values['_'] = true;

        return values;
    }
    public final static boolean isValidTerminatingCharForInf(int c) {
        return !is8bitValue(c) || !invalidTerminatingCharsForInf[c & 0xff];
    }

    private static final boolean isValidExtendedSymbolCharacter[] = makeIsValidExtendedSymbolCharacterArray();
    private static final boolean [] makeIsValidExtendedSymbolCharacterArray() {
        boolean [] values = new boolean [256];

        values['!'] = true;
        values['#'] = true;
        values['%'] = true;
        values['&'] = true;
        values['*'] = true;
        values['+'] = true;
        values['-'] = true;
        values['.'] = true;
        values['/'] = true;
        values[';'] = true;
        values['<'] = true;
        values['='] = true;
        values['>'] = true;
        values['?'] = true;
        values['@'] = true;
        values['^'] = true;
        values['`'] = true;
        values['|'] = true;
        values['~'] = true;

        return values;
    }
    public final static boolean isValidExtendedSymbolCharacter(int c)
    {
        return (isValidExtendedSymbolCharacter[c & 0xff] && is8bitValue(c));
    }

    private static final boolean isValidSymbolCharacter[] = makeIsValidSymbolCharacterArray();
    private static final boolean [] makeIsValidSymbolCharacterArray() {
        boolean [] values = new boolean [256];

        for (int ii='a'; ii<='z'; ii++) {
            values[ii] = true;
        }
        for (int ii='A'; ii<='Z'; ii++) {
            values[ii] = true;
        }
        for (int ii='0'; ii<='9'; ii++) {
            values[ii] = true;
        }
        values['$'] = true;
        values['_'] = true;

        return values;
    }
    public final static boolean isValidSymbolCharacter(int c)
    {
        return (isValidSymbolCharacter[c & 0xff] && is8bitValue(c));
    }

    private static final boolean isValidStartSymbolCharacter[] = makeIsValidStartSymbolCharacterArray();
    private static final boolean [] makeIsValidStartSymbolCharacterArray() {
        boolean [] values = new boolean [256];

        for (int ii='a'; ii<='z'; ii++) {
            values[ii] = true;
        }
        for (int ii='A'; ii<='Z'; ii++) {
            values[ii] = true;
        }
        values['$'] = true;
        values['_'] = true;

        return values;
    }
    public final static boolean isValidStartSymbolCharacter(int c)
    {
        return (isValidStartSymbolCharacter[c & 0xff] && is8bitValue(c));
    }

    public static int decodeSid(CharSequence sidToken)
    {
        assert sidToken.charAt(0) == '$';

        int length = sidToken.length();
        assert length > 1;

        String digits = sidToken.subSequence(1, length).toString();
        return Integer.parseInt(digits);
    }

    static public int keyword(CharSequence word, int start_word, int end_word)
    {
        int c = word.charAt(start_word);
        int len = end_word - start_word; // +1 but we build that into the constants below
        switch (c) {
        case '$':
            if (len > 1) {
                for (int i = start_word + 1; i < end_word; i++) {
                    if (! isDigit(word.charAt(i))) return -1;
                }
                return KEYWORD_sid;
            }
            return -1;
        case 'b':
            if (len == 4) {
                if (word.charAt(start_word+1) == 'o'
                 && word.charAt(start_word+2) == 'o'
                 && word.charAt(start_word+3) == 'l'
                ) {
                    return KEYWORD_BOOL;
                }
                if (word.charAt(start_word+1) == 'l'
                 && word.charAt(start_word+2) == 'o'
                 && word.charAt(start_word+3) == 'b'
                ) {
                    return KEYWORD_BLOB;
                }
            }
            return -1;
        case 'c':
            if (len == 4) {
                if (word.charAt(start_word+1) == 'l'
                 && word.charAt(start_word+2) == 'o'
                 && word.charAt(start_word+3) == 'b'
                ) {
                    return KEYWORD_CLOB;
                }
            }
            return -1;
        case 'd':
            if (len == 7) {
                if (word.charAt(start_word+1) == 'e'
                 && word.charAt(start_word+2) == 'c'
                 && word.charAt(start_word+3) == 'i'
                 && word.charAt(start_word+4) == 'm'
                 && word.charAt(start_word+5) == 'a'
                 && word.charAt(start_word+6) == 'l'
                ) {
                    return KEYWORD_DECIMAL;
                }
            }
            return -1;
        case 'f':
            if (len == 5) {
                if (word.charAt(start_word+1) == 'a'
                 && word.charAt(start_word+2) == 'l'
                 && word.charAt(start_word+3) == 's'
                 && word.charAt(start_word+4) == 'e'
                ) {
                    return KEYWORD_FALSE;
                }
                if (word.charAt(start_word+1) == 'l'
                 && word.charAt(start_word+2) == 'o'
                 && word.charAt(start_word+3) == 'a'
                 && word.charAt(start_word+4) == 't'
                ) {
                    return KEYWORD_FLOAT;
                }
            }
            return -1;
        case 'i':
            if (len == 3) {
                if (word.charAt(start_word+1) == 'n') {
                    if (word.charAt(start_word+2) == 't') {
                        return KEYWORD_INT;
                    }
                }
            }
            return -1;
        case 'l':
            if (len == 4) {
                if (word.charAt(start_word+1) == 'i'
                 && word.charAt(start_word+2) == 's'
                 && word.charAt(start_word+3) == 't'
                ) {
                    return KEYWORD_LIST;
                }
            }
            return -1;
        case 'n':
            if (len == 4) {
                if (word.charAt(start_word+1) == 'u'
                 && word.charAt(start_word+2) == 'l'
                 && word.charAt(start_word+3) == 'l'
                ) {
                    return KEYWORD_NULL;
                }
            }
            else if (len == 3) {
                if (word.charAt(start_word+1) == 'a'
                    && word.charAt(start_word+2) == 'n'
                   ) {
                       return KEYWORD_NAN;
                   }
            }
            return -1;
        case 's':
            if (len == 4) {
                if (word.charAt(start_word+1) == 'e'
                 && word.charAt(start_word+2) == 'x'
                 && word.charAt(start_word+3) == 'p'
                ) {
                    return KEYWORD_SEXP;
                }
            }
            else if (len == 6) {
                if (word.charAt(start_word+1) == 't'
                 && word.charAt(start_word+2) == 'r'
                ) {
                    if (word.charAt(start_word+3) == 'i'
                     && word.charAt(start_word+4) == 'n'
                     && word.charAt(start_word+5) == 'g'
                    ) {
                        return KEYWORD_STRING;
                    }
                    if (word.charAt(start_word+3) == 'u'
                     && word.charAt(start_word+4) == 'c'
                     && word.charAt(start_word+5) == 't'
                    ) {
                        return KEYWORD_STRUCT;
                    }
                    return -1;
                }
                if (word.charAt(start_word+1) == 'y'
                 && word.charAt(start_word+2) == 'm'
                 && word.charAt(start_word+3) == 'b'
                 && word.charAt(start_word+4) == 'o'
                 && word.charAt(start_word+5) == 'l'
                ) {
                    return KEYWORD_SYMBOL;
                }
            }
            return -1;
        case 't':
            if (len == 4) {
                if (word.charAt(start_word+1) == 'r'
                 && word.charAt(start_word+2) == 'u'
                 && word.charAt(start_word+3) == 'e'
                ) {
                    return KEYWORD_TRUE;
                }
            }
            else if (len == 9) {
                if (word.charAt(start_word+1) == 'i'
                 && word.charAt(start_word+2) == 'm'
                 && word.charAt(start_word+3) == 'e'
                 && word.charAt(start_word+4) == 's'
                 && word.charAt(start_word+5) == 't'
                 && word.charAt(start_word+6) == 'a'
                 && word.charAt(start_word+7) == 'm'
                 && word.charAt(start_word+8) == 'p'
                ) {
                    return KEYWORD_TIMESTAMP;
                }
            }
            return -1;
        default:
            return -1;
        }
    }

    /*
    KW_BIT_BLOB       = 0x0001;
    KW_BIT_BOOL       = 0x0002;
    KW_BIT_CLOB       = 0x0004;
    KW_BIT_DECIMAL    = 0x0008;
    KW_BIT_FLOAT      = 0x0010;
    KW_BIT_INT        = 0x0020;
    KW_BIT_LIST       = 0x0030;
    KW_BIT_NULL       = 0x0080;
    KW_BIT_SEXP       = 0x0100;
    KW_BIT_STRING     = 0x0200;
    KW_BIT_STRUCT     = 0x0400;
    KW_BIT_SYMBOL     = 0x0800;
    KW_BIT_TIMESTAMP  = 0x1000;
    datagram is not included (since these are types that can appear as a null type)

    A  1    F  6    N 11    T 16
    B  2    G  7    O 12    U 17
    C  3    I  8    P 13    X 18
    D  4    L  9    R 14    Y 19
    E  5    M 10    S 15

    */
    // this array is [pos][letter_idx] - if this letter is
    // valid in this position then all the type names that
    // contain the letter at that position are on.  Each
    // keyword is assigned a bit value.
    // however due to the double index expense the index
    // is represented by a 1 dim int array and fn's used
    // to access it (since inline-ing final static small methods
    // is easy for the compiler)
    static final int  TN_MAX_NAME_LENGTH = "TIMESTAMP".length() + 1; // so anything too long will be 0
    static final int  TN_LETTER_MAX_IDX = 19;

    static final int  KW_BIT_BLOB       = 0x0001;
    static final int  KW_BIT_BOOL       = 0x0002;
    static final int  KW_BIT_CLOB       = 0x0004;
    static final int  KW_BIT_DECIMAL    = 0x0008;
    static final int  KW_BIT_FLOAT      = 0x0010;
    static final int  KW_BIT_INT        = 0x0020;
    static final int  KW_BIT_LIST       = 0x0030;
    static final int  KW_BIT_NULL       = 0x0080;
    static final int  KW_BIT_SEXP       = 0x0100;
    static final int  KW_BIT_STRING     = 0x0200;
    static final int  KW_BIT_STRUCT     = 0x0400;
    static final int  KW_BIT_SYMBOL     = 0x0800;
    static final int  KW_BIT_TIMESTAMP  = 0x1000;
    static final int  KW_ALL_BITS       = 0x1fff;

    static final int[] typeNameBits = new int[] {
        KW_BIT_BLOB,      KW_BIT_BOOL,    KW_BIT_CLOB,
        KW_BIT_DECIMAL,   KW_BIT_FLOAT,   KW_BIT_INT,
        KW_BIT_LIST,      KW_BIT_NULL,    KW_BIT_SEXP,
        KW_BIT_STRING,    KW_BIT_STRUCT,  KW_BIT_SYMBOL,
        KW_BIT_TIMESTAMP
    };

    static final String[] typeNameNames = new String[] {
        "blob",           "bool",         "clob",
        "decimal",        "float",        "int",
        "list",           "null",         "sexp",
        "string",         "struct",       "symbol",
        "timestamp"
    };
    static final int[] typeNameKeyWordIds = new int[] {
        KEYWORD_BLOB,     KEYWORD_BOOL,   KEYWORD_CLOB,
        KEYWORD_DECIMAL,  KEYWORD_FLOAT,  KEYWORD_INT,
        KEYWORD_LIST,     KEYWORD_NULL,   KEYWORD_SEXP,
        KEYWORD_STRING,   KEYWORD_STRUCT, KEYWORD_SYMBOL,
        KEYWORD_TIMESTAMP
    };

    static final int[] TypeNameBitIndex = makeTypeNameBitIndex();
    static final int[] makeTypeNameBitIndex() {
        int[] bits = new int[TN_MAX_NAME_LENGTH*TN_LETTER_MAX_IDX];
        int   typename_bit;
        for (int tt=0; tt<typeNameNames.length; tt++) {
            String typename = typeNameNames[tt];
            typename_bit = typeNameBits[tt];
            for (int ii=0; ii<typename.length(); ii++) {
                int c = typename.charAt(ii);
                int l = typeNameLetterIdx(c);
                assert(l > 0);
                typename_set_bit(bits, ii, l, typename_bit);
            }
        }
        return bits;
    }
    private final static void typename_set_bit(int[] bits, int ii, int l, int typename_bit)
    {
        // bits[ii][l]
        int idx = (ii * TN_LETTER_MAX_IDX) + l - 1;
        bits[idx] |= typename_bit;
    }
    public final static int typeNameLetterIdx(int c) {
        switch(c) {
        case 'a': return  1;
        case 'b': return  2;
        case 'c': return  3;
        case 'd': return  4;
        case 'e': return  5;
        case 'f': return  6;
        case 'g': return  7;
        case 'i': return  8;
        case 'l': return  9;
        case 'm': return 10;
        case 'n': return 11;
        case 'o': return 12;
        case 'p': return 13;
        case 'r': return 14;
        case 's': return 15;
        case 't': return 16;
        case 'u': return 17;
        case 'x': return 18;
        case 'y': return 19;
        default:  return -1;
        }
    }
    public final static int typeNamePossibilityMask(int pos, int letter_idx) {
        int idx = (pos * TN_LETTER_MAX_IDX) + letter_idx - 1;
        int mask = TypeNameBitIndex[idx];
        return mask;
    }
    // this can be faster but it's pretty unusual to be called.
    public final static int typeNameKeyWordFromMask(int possible_names, int length) {
        int kw = KEYWORD_unrecognized;
        if (possible_names != IonTokenConstsX.KW_ALL_BITS) {
            for (int ii=0; ii<typeNameBits.length; ii++) {
                int tb = typeNameBits[ii];
                if (tb == possible_names) {
                    if (typeNameNames[ii].length() == length) {
                        kw = typeNameKeyWordIds[ii];
                    }
                    break;
                }
            }
        }
        return kw;
    }
    public static final String getNullImage(IonType type)
    {
        String nullimage = null;

        switch (type) {
        case NULL:      nullimage = "null";           break;
        case BOOL:      nullimage = "null.bool";      break;
        case INT:       nullimage = "null.int";       break;
        case FLOAT:     nullimage = "null.float";     break;
        case DECIMAL:   nullimage = "null.decimal";   break;
        case TIMESTAMP: nullimage = "null.timestamp"; break;
        case SYMBOL:    nullimage = "null.symbol";    break;
        case STRING:    nullimage = "null.string";    break;
        case BLOB:      nullimage = "null.blob";      break;
        case CLOB:      nullimage = "null.clob";      break;
        case SEXP:      nullimage = "null.sexp";      break;
        case LIST:      nullimage = "null.list";      break;
        case STRUCT:    nullimage = "null.struct";    break;

        default: throw new IllegalStateException("unexpected type " + type);
        }

        return nullimage;
    }
    public static final IonType getNullType(CharSequence s)
    {
        IonType type = null;
        int     c, ii = 0;
        boolean stop = false;
        while (!stop && ii<s.length()) {
            c = s.charAt(ii++);
            switch (c) {
            case ' ': case '\t': case '\r': case '\n':
                break;
            case 'n':
                stop = true;
                break;
            default:
                invalid_null_image(s);
            }
        }
        if (ii>=s.length() || s.charAt(ii++) != 'u') invalid_null_image(s);
        if (ii>=s.length() || s.charAt(ii++) != 'l') invalid_null_image(s);
        if (ii>=s.length() || s.charAt(ii++) != 'l') invalid_null_image(s);
        boolean dot = false;
        while (!dot && ii<s.length()) {
            c = s.charAt(ii++);
            switch (c) {
            case ' ': case '\t': case '\r': case '\n':
                break;
            case '.':
                dot = true;
                break;
            default:
                invalid_null_image(s);
            }
        }
        if (dot) {
            int kw = IonTokenConstsX.keyword(s, ii, s.length());
            switch (kw) {
                case IonTokenConstsX.KEYWORD_NULL:
                    type = IonType.NULL;
                    break;
                case IonTokenConstsX.KEYWORD_BOOL:
                    type = IonType.BOOL;
                    break;
                case IonTokenConstsX.KEYWORD_INT:
                    type = IonType.INT;
                    break;
                case IonTokenConstsX.KEYWORD_FLOAT:
                    type = IonType.FLOAT;
                    break;
                case IonTokenConstsX.KEYWORD_DECIMAL:
                    type = IonType.DECIMAL;
                    break;
                case IonTokenConstsX.KEYWORD_TIMESTAMP:
                    type = IonType.TIMESTAMP;
                    break;
                case IonTokenConstsX.KEYWORD_SYMBOL:
                    type = IonType.SYMBOL;
                    break;
                case IonTokenConstsX.KEYWORD_STRING:
                    type = IonType.STRING;
                    break;
                case IonTokenConstsX.KEYWORD_CLOB:
                    type = IonType.CLOB;
                    break;
                case IonTokenConstsX.KEYWORD_BLOB:
                    type = IonType.BLOB;
                    break;
                case IonTokenConstsX.KEYWORD_STRUCT:
                    type = IonType.STRUCT;
                    break;
                case IonTokenConstsX.KEYWORD_LIST:
                    type = IonType.LIST;
                    break;
                case IonTokenConstsX.KEYWORD_SEXP:
                    type = IonType.SEXP;
                    break;
                default:
                    invalid_null_image(s);
            }
        }

        while (ii<s.length()) {
            c = s.charAt(ii++);
            switch (c) {
            case ' ': case '\t': case '\r': case '\n':
                break;
            default:
                invalid_null_image(s);
            }
        }

        return type;
    }
    private static void invalid_null_image(CharSequence s) {
        throw new CantConvertException("invalid image "+s.toString());
    }

}
