// Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.util;

import com.amazon.ion.EmptySymbolException;
import com.amazon.ion.impl._Private_IonConstants;
import java.io.IOException;


/**
 * Utility methods for working with Ion's text-oriented data types.
 */
public class IonTextUtils
{

    public enum SymbolVariant { IDENTIFIER, OPERATOR, QUOTED }

    private enum EscapeMode { JSON, ION_SYMBOL, ION_STRING, ION_LONG_STRING }


    private static final boolean[] IDENTIFIER_START_CHAR_FLAGS;
    private static final boolean[] IDENTIFIER_FOLLOW_CHAR_FLAGS;
    static
    {
        IDENTIFIER_START_CHAR_FLAGS = new boolean[256];
        IDENTIFIER_FOLLOW_CHAR_FLAGS = new boolean[256];

        for (int ii='a'; ii<='z'; ii++) {
            IDENTIFIER_START_CHAR_FLAGS[ii]  = true;
            IDENTIFIER_FOLLOW_CHAR_FLAGS[ii] = true;
        }
        for (int ii='A'; ii<='Z'; ii++) {
            IDENTIFIER_START_CHAR_FLAGS[ii]  = true;
            IDENTIFIER_FOLLOW_CHAR_FLAGS[ii] = true;
        }
        IDENTIFIER_START_CHAR_FLAGS ['_'] = true;
        IDENTIFIER_FOLLOW_CHAR_FLAGS['_'] = true;

        IDENTIFIER_START_CHAR_FLAGS ['$'] = true;
        IDENTIFIER_FOLLOW_CHAR_FLAGS['$'] = true;

        for (int ii='0'; ii<='9'; ii++) {
            IDENTIFIER_FOLLOW_CHAR_FLAGS[ii] = true;
        }
    }


    private static final boolean[] OPERATOR_CHAR_FLAGS;
    static
    {
        final char[] operatorChars = {
            '<', '>', '=', '+', '-', '*', '&', '^', '%',
            '~', '/', '?', '.', ';', '!', '|', '@', '`', '#'
           };

        OPERATOR_CHAR_FLAGS = new boolean[256];

        for (int ii=0; ii<operatorChars.length; ii++) {
            char operator = operatorChars[ii];
            OPERATOR_CHAR_FLAGS[operator] = true;
        }
    }

    private final static String[] ZERO_PADDING =
    {
        "",
        "0",
        "00",
        "000",
        "0000",
        "00000",
        "000000",
        "0000000",
    };

    // escape sequences for character below ascii 32 (space)
    private static final String[] ESCAPE_CODES;
    static
    {
        ESCAPE_CODES = new String[256];
        ESCAPE_CODES[0x00] = "\\0";
        ESCAPE_CODES[0x07] = "\\a";
        ESCAPE_CODES[0x08] = "\\b";
        ESCAPE_CODES['\t'] = "\\t";
        ESCAPE_CODES['\n'] = "\\n";
        ESCAPE_CODES[0x0B] = "\\v";
        ESCAPE_CODES['\f'] = "\\f";
        ESCAPE_CODES['\r'] = "\\r";
        ESCAPE_CODES['\\'] = "\\\\";
        ESCAPE_CODES['\"'] = "\\\"";
        for (int i = 1; i < 0x20; ++i) {
            if (ESCAPE_CODES[i] == null) {
                String s = Integer.toHexString(i);
                ESCAPE_CODES[i] = "\\x" + ZERO_PADDING[2 - s.length()] + s;
            }
        }
        for (int i = 0x7F; i < 0x100; ++i) {
            String s = Integer.toHexString(i);
            ESCAPE_CODES[i] = "\\x" + s;
        }
    }

    private static final String[] LONG_ESCAPE_CODES;
    static
    {
        LONG_ESCAPE_CODES = new String[256];
        for (int i = 0; i < 256; ++i) {
            LONG_ESCAPE_CODES[i] = ESCAPE_CODES[i];
        }
        LONG_ESCAPE_CODES['\n'] = null;
        LONG_ESCAPE_CODES['\''] = "\\\'";
        LONG_ESCAPE_CODES['\"'] = null; // Treat as normal code point for long string
    }

    private static final String[] SYMBOL_ESCAPE_CODES;
    static
    {
        SYMBOL_ESCAPE_CODES = new String[256];
        for (int i = 0; i < 256; ++i) {
            SYMBOL_ESCAPE_CODES[i] = ESCAPE_CODES[i];
        }
        SYMBOL_ESCAPE_CODES['\''] = "\\\'";
        SYMBOL_ESCAPE_CODES['\"'] = null; // Treat as normal code point for symbol.
    }

    private static final String[] JSON_ESCAPE_CODES;
    static
    {
        JSON_ESCAPE_CODES = new String[256];
        JSON_ESCAPE_CODES[0x00] = "\\0";
        JSON_ESCAPE_CODES[0x07] = "\\u0007";
        JSON_ESCAPE_CODES[0x0B] = "\\u000b";
        JSON_ESCAPE_CODES['\t'] = "\\t";
        JSON_ESCAPE_CODES['\n'] = "\\n";
        JSON_ESCAPE_CODES[0x0B] = "\\v";
        JSON_ESCAPE_CODES['\f'] = "\\f";
        JSON_ESCAPE_CODES['\r'] = "\\r";
        JSON_ESCAPE_CODES['\\'] = "\\\\";
        JSON_ESCAPE_CODES['\"'] = "\\\"";
        for (int i = 1; i < 0x20; ++i) {
            if (JSON_ESCAPE_CODES[i] == null) {
                String s = Integer.toHexString(i);
                JSON_ESCAPE_CODES[i] = "\\u" + ZERO_PADDING[4 - s.length()] + s;
            }
        }
        for (int i = 0x7F; i < 0x100; ++i) {
            String s = Integer.toHexString(i);
            JSON_ESCAPE_CODES[i] = "\\u00" + s;
        }
    }

    private static final String utf16Prefix = "\\u";
    private static final String utf32Prefix = "\\U";
    private static final String tripleQuotes = "'''";


//
//          "\\0",   "\\x01", "\\x02", "\\x03",
//          "\\x04", "\\x05", "\\x06", "\\a",
//          "\\b",   "\\t",   "\\n",   "\\v",
//          "\\f",   "\\r",   "\\x0e", "\\x0f",
//          "\\x10", "\\x11", "\\x12", "\\x13",
//          "\\x14", "\\x15", "\\x16", "\\x17",
//          "\\x18", "\\x19", "\\x1a", "\\x1b",
//          "\\x1c", "\\x1d", "\\x1e", "\\x1f",
//    };


    //=========================================================================


    public static final class IonCodePointWriter {

        private final Appendable _out;
        private String[] _escapes;

        public IonCodePointWriter(Appendable out) {
            this._out = out;
        }

        public final void printString(CharSequence text)
            throws IOException
        {
            if (text == null)
            {
                _out.append("null.string");
            }
            else
            {
                _out.append('"');
                _escapes = ESCAPE_CODES;
                printCodePoints(text);
                _out.append('"');
            }
        }

        public final void printLongString(CharSequence text)
            throws IOException
        {
            if (text == null)
            {
                _out.append("null.string");
            }
            else
            {
                _out.append(tripleQuotes);
                _escapes = LONG_ESCAPE_CODES;
                printCodePoints(text);
                _out.append(tripleQuotes);
            }
        }

        public final void printJsonString(CharSequence text)
            throws IOException
        {
            if (text == null)
            {
                _out.append("null");
            }
            else
            {
                _out.append('"');
                _escapes = JSON_ESCAPE_CODES;
                printCodePoints(text);
                _out.append('"');
            }
        }

        public final void printClob(byte[] value, int start, int end)
            throws IOException
        {
            if (value == null)
            {
                _out.append("null.clob");
            }
            else
            {
                _out.append('"');
                _escapes = ESCAPE_CODES;
                printBytes(value, start, end);
                _out.append('"');
            }
        }

        public final void printLongClob(byte[] value, int start, int end)
            throws IOException
        {
            if (value == null) {
                _out.append("null.clob");
            }
            else
            {
                _out.append(tripleQuotes);
                _escapes = LONG_ESCAPE_CODES;
                printBytes(value, start, end);
                _out.append(tripleQuotes);
            }
        }

        public final void printJsonClob(byte[] value, int start, int end)
            throws IOException
        {
            if (value == null) {
                _out.append("null");
            } else {
                _out.append('"');
                _escapes = JSON_ESCAPE_CODES;
                printBytes(value, start, end);
                _out.append('"');
            }
        }

        public final void printSymbol(CharSequence text)
            throws IOException
        {
            if (text == null)
            {
                _out.append("null.symbol");
            }
            else if (text.length() == 0)
            {
                throw new EmptySymbolException();
            }
            else if (symbolNeedsQuoting(text, true)) {
                _out.append('\'');
                _escapes = SYMBOL_ESCAPE_CODES;
                printCodePoints(text);
                _out.append('\'');
            }
            else
            {
                _out.append(text);
            }
        }
        public final void printQuotedSymbol(CharSequence text)
            throws IOException
        {
            if (text == null)
            {
                _out.append("null.symbol");
            }
            else if (text.length() == 0)
            {
                throw new EmptySymbolException();
            }
            else
            {
                _out.append('\'');
                _escapes = SYMBOL_ESCAPE_CODES;
                printCodePoints(text);
                _out.append('\'');
            }
        }

        private final void printBytes(byte[] value, int start, int end)
            throws IOException
        {
            for (int i = start; i < end; i++) {
                char c = (char)(value[i] & 0xff);
                if (_escapes[c] != null) {
                    _out.append(_escapes[c]);
                } else {
                    _out.append(c);
                }
            }
        }

        private final void printCodePoints(CharSequence text)
            throws IOException
        {
            int len = text.length();
            for (int i = 0; i < len; ++i)
            {
                // write as many bytes in a sequence as possible
                int c = -1;
                int j;
                for (j = i; j < len; ++j) {
                    c = text.charAt(j);
                    if (c >= 0x100 || _escapes[c] != null) {
                        // append sequence then continue the normal loop
                        if (j > i) {
                            _out.append(text, i, j);
                            i = j;
                        }
                        break;
                    }
                }
                if (j == len) {
                    // we've reached the end of sequence; append it and break
                    _out.append(text, i, j);
                    break;
                }
                // write the non Latin-1 character
                if (c < 0x100) {
                    assert _escapes[c] != null;
                    _out.append(_escapes[c]);
                } else if (c < 0xD800 || c >= 0xE000) {
                    String s = Integer.toHexString(c);
                    _out.append(utf16Prefix);
                    _out.append(ZERO_PADDING[4 - s.length()]);
                    _out.append(s);
                } else if (_Private_IonConstants.isHighSurrogate(c)) {
                    // high surrogate
                    char c2;
                    if (++i == len || !_Private_IonConstants.isLowSurrogate(c2 = text.charAt(i))) {
                        String message =
                            "text is invalid UTF-16. It contains an unmatched " +
                            "high surrogate 0x" + Integer.toHexString(c) +
                            " at index " + (i-1);
                        throw new IllegalArgumentException(message);
                    }
                    c = _Private_IonConstants.makeUnicodeScalar(c, c2);
                    String s = Integer.toHexString(c);
                    _out.append(utf32Prefix);
                    _out.append(ZERO_PADDING[8 - s.length()]);
                    _out.append(s);
                } else {
                    // unmatched low surrogate
                    String message =
                        "text is invalid UTF-16. It contains an unmatched " +
                        "low surrogate 0x" + Integer.toHexString(c) +
                        " at index " + i;
                    throw new IllegalArgumentException(message);
                }
            }
        }
    }

    /**
     * Ion whitespace is defined as one of the characters space, tab, newline,
     * and carriage-return.  This matches the definition of whitespace used by
     * JSON.
     *
     * @param codePoint the character to test.
     * @return <code>true</code> if <code>c</code> is one of the four legal
     * Ion whitespace characters.
     *
     * @see <a href="http://tools.ietf.org/html/rfc4627">RFC 4627</a>
     */
    public static boolean isWhitespace(int codePoint)
    {
        switch (codePoint)
        {
            // FIXME look for BOM
            case ' ':  case '\t':  case '\n':  case '\r':
            {
                return true;
            }
            default:
            {
                return false;
            }
        }
    }

    /**
     * Determines whether a given code point is one of the valid Ion numeric
     * terminators.
     * <p>
     * The slash character {@code '/'} is not itself a valid terminator, but
     * if the next character is {@code '/'} or {@code '*'} then the number is
     * followed by a comment.  Since this method cannot perform the look-ahead
     * necessary to make that determination, it returns {@code false} for the
     * slash.
     *
     * @param codePoint the Unicode scalar to test.
     *
     * @return true when the scalar can legally follow an Ion number.
     * Returns false for the slash character {@code '/'}.
     */
    public static boolean isNumericStop(int codePoint)
    {
        switch (codePoint) {
        case -1:
        case '{':  case '}':
        case '[':  case ']':
        case '(':  case ')':
        case ',':
        case '\"': case '\'':
        case ' ':  case '\t':  case '\n':  case '\r':  // Whitespace
        // case '/': // we check start of comment in the caller where we
        //              can peek ahead for the following slash or asterisk
            return true;

        default:
            return false;
        }
    }

    private static boolean isDecimalDigit(int codePoint)
    {
        return (codePoint >= '0' && codePoint <= '9');
    }

    public static boolean isDigit(int codePoint, int radix)
    {
        switch (codePoint) {
        case '0': case '1': case '2': case '3': case '4':
        case '5': case '6': case '7':
            return (radix == 8 || radix == 10 || radix == 16);
        case '8': case '9':
            return (radix == 10 || radix == 16);
        case 'A': case 'B': case 'C': case 'D': case 'E': case 'F':
        case 'a': case 'b': case 'c': case 'd': case 'e': case 'f':
            return (radix == 16);
        }
        return false;
    }

    private static boolean is8bitValue(int v) {
        return (v & ~0xff) == 0;
    }
    public static boolean isIdentifierStart(int codePoint) {
        return IDENTIFIER_START_CHAR_FLAGS[codePoint & 0xff] && is8bitValue(codePoint);
    }

    public static boolean isIdentifierPart(int codePoint) {
        return IDENTIFIER_FOLLOW_CHAR_FLAGS[codePoint & 0xff] && is8bitValue(codePoint);
    }

    public static boolean isOperatorPart(int codePoint) {
        return OPERATOR_CHAR_FLAGS[codePoint & 0xff] && is8bitValue(codePoint);
    }

    /**
     * Determines whether the given text matches one of the Ion identifier
     * keywords <code>null</code>, <code>true</code>, or <code>false</code>.
     * <p>
     * This does <em>not</em> check for non-identifier keywords such as
     * <code>null.int</code>.
     *
     * @param text the symbol to check.
     * @return <code>true</code> if the text is an identifier keyword.
     */
    static boolean isIdentifierKeyword(CharSequence text)
    {
        int pos = 0;
        int valuelen = text.length();
        boolean keyword = false;

        // there has to be at least 1 character or we wouldn't be here
        switch (text.charAt(pos++)) {
        case '$':
            if (valuelen == 1) return false;
            while (pos < valuelen) {
                char c = text.charAt(pos++);
                if (! isDecimalDigit(c)) return false;
            }
            return true;
        case 'f':
            if (valuelen == 5 //      'f'
             && text.charAt(pos++) == 'a'
             && text.charAt(pos++) == 'l'
             && text.charAt(pos++) == 's'
             && text.charAt(pos++) == 'e'
            ) {
                keyword = true;
            }
            break;
        case 'n':
            if (valuelen == 4 //      'n'
             && text.charAt(pos++) == 'u'
             && text.charAt(pos++) == 'l'
             && text.charAt(pos++) == 'l'
            ) {
                keyword = true;
            }
            else if (valuelen == 3 // 'n'
             && text.charAt(pos++) == 'a'
             && text.charAt(pos++) == 'n'
            ) {
                keyword = true;
            }
            break;
        case 't':
            if (valuelen == 4 //      't'
             && text.charAt(pos++) == 'r'
             && text.charAt(pos++) == 'u'
             && text.charAt(pos++) == 'e'
            ) {
                keyword = true;
            }
            break;
        }

        return keyword;
    }


    /**
     * Determines whether the text of a symbol requires (single) quotes.
     *
     * @param symbol must be a non-empty string.
     * @param quoteOperators indicates whether the caller wants operators to be
     * quoted; if <code>true</code> then operator symbols like <code>!=</code>
     * will return <code>true</code>.
     * has looser quoting requirements than other containers.
     * @return <code>true</code> if the given symbol requires quoting.
     *
     * @throws NullPointerException
     *         if <code>symbol</code> is <code>null</code>.
     * @throws EmptySymbolException if <code>symbol</code> is empty.
     */
    static boolean symbolNeedsQuoting(CharSequence symbol,
                                      boolean      quoteOperators)
    {
        int length = symbol.length();
        if (length == 0) {
            throw new EmptySymbolException();
        }

        // If the symbol's text matches an Ion keyword, we must quote it.
        // Eg, the symbol 'false' must be rendered quoted.
        if (! isIdentifierKeyword(symbol))
        {
            char c = symbol.charAt(0);
            // Surrogates are neither identifierStart nor operatorPart, so the
            // first one we hit will fall through and return true.
            // TODO test that

            if (!quoteOperators && isOperatorPart(c))
            {
                for (int ii = 0; ii < length; ii++) {
                    c = symbol.charAt(ii);
                    // We don't need to look for escapes since all
                    // operator characters are ASCII.
                    if (!isOperatorPart(c)) {
                        return true;
                    }
                }
                return false;
            }
            else if (isIdentifierStart(c))
            {
                for (int ii = 0; ii < length; ii++) {
                    c = symbol.charAt(ii);
                    if ((c == '\'' || c < 32 || c > 126)
                        || !isIdentifierPart(c))
                    {
                        return true;
                    }
                }
                return false;
            }
        }

        return true;
    }


    /**
     * Determines whether the text of a symbol represents an identifier, an
     * operator, or a symbol that always requires (single) quotes.
     *
     * @param symbol must be a non-empty string.
     *
     * @return the variant of the symbol.
     *
     * @throws NullPointerException
     *         if <code>symbol</code> is <code>null</code>.
     * @throws EmptySymbolException if <code>symbol</code> is empty.
     */
    public static SymbolVariant symbolVariant(CharSequence symbol)
    {
        int length = symbol.length();
        if (length == 0) {
            throw new EmptySymbolException();
        }

        // If the symbol's text matches an Ion keyword, we must quote it.
        // Eg, the symbol 'false' must be rendered quoted.
        if (isIdentifierKeyword(symbol))
        {
            return SymbolVariant.QUOTED;
        }

        char c = symbol.charAt(0);
        // Surrogates are neither identifierStart nor operatorPart, so the
        // first one we hit will fall through and return QUOTED.
        // TODO test that

        if (isIdentifierStart(c))
        {
            for (int ii = 0; ii < length; ii++) {
                c = symbol.charAt(ii);
                if ((c == '\'' || c < 32 || c > 126)
                    || !isIdentifierPart(c))
                {
                    return SymbolVariant.QUOTED;
                }
            }
            return SymbolVariant.IDENTIFIER;
        }

        if (isOperatorPart(c))
        {
            for (int ii = 0; ii < length; ii++) {
                c = symbol.charAt(ii);
                // We don't need to look for escapes since all
                // operator characters are ASCII.
                if (!isOperatorPart(c)) {
                    return SymbolVariant.QUOTED;
                }
            }
            return SymbolVariant.OPERATOR;
        }

        return SymbolVariant.QUOTED;
    }


    //=========================================================================


    /**
     * Prints a single Unicode code point for use in an ASCII-safe Ion string.
     *
     * @param out the stream to receive the data.
     * @param codePoint a Unicode code point.
     */
    public static void printStringCodePoint(Appendable out, int codePoint)
        throws IOException
    {
        printCodePoint(out, codePoint, EscapeMode.ION_STRING);
    }

    /**
     * Prints a single Unicode code point for use in an ASCII-safe Ion symbol.
     *
     * @param out the stream to receive the data.
     * @param codePoint a Unicode code point.
     */
    public static void printSymbolCodePoint(Appendable out, int codePoint)
        throws IOException
    {
        printCodePoint(out, codePoint, EscapeMode.ION_SYMBOL);
    }

    /**
     * Prints a single Unicode code point for use in an ASCII-safe JSON string.
     *
     * @param out the stream to receive the data.
     * @param codePoint a Unicode code point.
     */
    public static void printJsonCodePoint(Appendable out, int codePoint)
        throws IOException
    {
        // JSON only allows double-quote strings.
        printCodePoint(out, codePoint, EscapeMode.JSON);
    }


    /**
     * Prints a single code point, ASCII safe.
     */
    private static void printCodePoint(Appendable out, int c, EscapeMode mode)
        throws IOException
    {
        // JSON only allows uHHHH numeric escapes.
        switch (c) {
            case 0:
                out.append(mode == EscapeMode.JSON ? "\\u0000" : "\\0");
                return;
            case '\t':
                out.append("\\t");
                return;
            case '\n':
                if (mode == EscapeMode.ION_LONG_STRING) {
                    out.append('\n');
                }
                else {
                    out.append("\\n");
                }
                return;
            case '\r':
                out.append("\\r");
                return;
            case '\f':
                out.append("\\f");
                return;
            case '\u0008':
                out.append("\\b");
                return;
            case '\u0007':
                out.append(mode == EscapeMode.JSON ? "\\u0007" : "\\a");
                return;
            case '\u000B':
                out.append(mode == EscapeMode.JSON ? "\\u000b" : "\\v");
                return;
            case '\"':
                if (mode == EscapeMode.JSON || mode == EscapeMode.ION_STRING) {
                    out.append("\\\"");
                    return;
                }
                break; // Treat as normal code point for long string or symbol.
            case '\'':
                if (mode == EscapeMode.ION_SYMBOL ||
                    mode == EscapeMode.ION_LONG_STRING)
                {
                    out.append("\\\'");
                    return;
                }
                break;
            case '\\':
                out.append("\\\\");
                return;
            default:
                break;
        }

        if (c < 32) {
            if (mode == EscapeMode.JSON) {
                printCodePointAsFourHexDigits(out, c);
            }
            else {
                printCodePointAsTwoHexDigits(out, c);
            }
        }
        else if (c < 0x7F) {  // Printable ASCII
            out.append((char)c);
        }
        else if (c <= 0xFF) {
            if (mode == EscapeMode.JSON) {
                printCodePointAsFourHexDigits(out, c);
            }
            else {
                printCodePointAsTwoHexDigits(out, c);
            }
        }
        else if (c <= 0xFFFF) {
            printCodePointAsFourHexDigits(out, c);
        }
        else {
            if (mode == EscapeMode.JSON) {
                // JSON does not have a \Uxxxxyyyy escape, surrogates
                // must be used as per RFC-4627
                //
                // http://www.ietf.org/rfc/rfc4627.txt
                // [...]
                //   To escape an extended character that is not in the Basic Multilingual
                //   Plane, the character is represented as a twelve-character sequence,
                //   encoding the UTF-16 surrogate pair.  So, for example, a string
                //   containing only the G clef character (U+1D11E) may be represented as
                //   "\uD834\uDD1E".
                // [...]
                printCodePointAsSurrogatePairHexDigits(out, c);
            }
            else {
                printCodePointAsEightHexDigits(out, c);
            }
        }
    }


    /**
     * Generates a two-digit hex escape sequence,
     * {@code "\x}<i>{@code HH}</i>{@code "},
     * using lower-case for alphabetics.
     */
    private static void printCodePointAsTwoHexDigits(Appendable out, int c)
        throws IOException
    {
        String s = Integer.toHexString(c);
        out.append("\\x");
        if (s.length() < 2) {
            out.append(ZERO_PADDING[2-s.length()]);
        }
        out.append(s);
    }

    /**
     * Generates a four-digit hex escape sequence,
     * {@code "\}{@code u}<i>{@code HHHH}</i>{@code "},
     * using lower-case for alphabetics.
     */
    private static void printCodePointAsFourHexDigits(Appendable out, int c)
        throws IOException
    {
        String s = Integer.toHexString(c);
        out.append("\\u");
        out.append(ZERO_PADDING[4-s.length()]);
        out.append(s);
    }

    /**
     * Prints an eight-digit hex escape sequence,
     * {@code "\}{@code U}<i>{@code HHHHHHHH}</i>{@code "},
     * using lower-case for alphabetics.
     */
    private static void printCodePointAsEightHexDigits(Appendable out, int c)
        throws IOException
    {
        String s = Integer.toHexString(c);
        out.append("\\U");
        out.append(ZERO_PADDING[8-s.length()]);
        out.append(s);
    }

    /**
     * Generates a surrogate pair as two four-digit hex escape sequences,
     * {@code "\}{@code u}<i>{@code HHHH}</i>{@code \}{@code u}<i>{@code HHHH}</i>{@code "},
     * using lower-case for alphabetics.
     * This for necessary for JSON when the code point is outside the BMP.
     */
    private static void printCodePointAsSurrogatePairHexDigits(Appendable out, int c)
        throws IOException
    {
        for (final char unit : Character.toChars(c)) {
            printCodePointAsFourHexDigits(out, unit);
        }
    }


    /**
     * Prints characters as an ASCII-encoded Ion string, including surrounding
     * double-quotes.
     * If the {@code text} is null, this prints {@code null.string}.
     *
     * @param out the stream to receive the data.
     * @param text the text to print; may be {@code null}.
     *
     * @throws IOException if the {@link Appendable} throws an exception.
     * @throws IllegalArgumentException
     *     if the text contains invalid UTF-16 surrogates.
     */
    public static void printString(Appendable out, CharSequence text)
        throws IOException
    {
        if (text == null)
        {
            out.append("null.string");
        }
        else
        {
            out.append('"');
            printCodePoints(out, text, EscapeMode.ION_STRING);
            out.append('"');
        }
    }

    /**
     * Prints characters as an ASCII-encoded JSON string, including surrounding
     * double-quotes.
     * If the {@code text} is null, this prints {@code null}.
     *
     * @param out the stream to receive the JSON data.
     * @param text the text to print; may be {@code null}.
     *
     * @throws IOException if the {@link Appendable} throws an exception.
     * @throws IllegalArgumentException
     *     if the text contains invalid UTF-16 surrogates.
     */
    public static void printJsonString(Appendable out, CharSequence text)
        throws IOException
    {
        if (text == null)
        {
            out.append("null");
        }
        else
        {
            out.append('"');
            printCodePoints(out, text, EscapeMode.JSON);
            out.append('"');
        }
    }


    /**
     * Builds a String denoting an ASCII-encoded Ion string,
     * including surrounding double-quotes.
     * If the {@code text} is null, this returns {@code "null.string"}.
     *
     * @param text the text to print; may be {@code null}.
     *
     * @throws IllegalArgumentException
     *     if the text contains invalid UTF-16 surrogates.
     */
    public static String printString(CharSequence text)
    {
        if (text == null)
        {
            return "null.string";
        }
        if (text.length() == 0)
        {
            return "\"\"";
        }

        StringBuilder builder = new StringBuilder(text.length() + 2);
        try
        {
            printString(builder, text);
        }
        catch (IOException e)
        {
            // Shouldn't happen
            throw new Error(e);
        }
        return builder.toString();
    }


    /**
     * Builds a String denoting an ASCII-encoded Ion "long string",
     * including surrounding triple-quotes.
     * If the {@code text} is null, this returns {@code "null.string"}.
     *
     * @param text the text to print; may be {@code null}.
     *
     * @throws IllegalArgumentException
     *     if the text contains invalid UTF-16 surrogates.
     */
    public static String printLongString(CharSequence text)
    {
        if (text == null) return "null.string";

        if (text.length() == 0) return "''''''";

        StringBuilder builder = new StringBuilder(text.length() + 6);
        try
        {
            printLongString(builder, text);
        }
        catch (IOException e)
        {
            // Shouldn't happen
            throw new Error(e);
        }
        return builder.toString();
    }


    /**
     * Prints characters as an ASCII-encoded Ion "long string",
     * including surrounding triple-quotes.
     * If the {@code text} is null, this prints {@code null.string}.
     *
     * @param out the stream to receive the data.
     * @param text the text to print; may be {@code null}.
     *
     * @throws IOException if the {@link Appendable} throws an exception.
     * @throws IllegalArgumentException
     *     if the text contains invalid UTF-16 surrogates.
     */
    public static void printLongString(Appendable out, CharSequence text)
        throws IOException
    {
        if (text == null)
        {
            out.append("null.string");
        }
        else
        {
            out.append("'''");
            printCodePoints(out, text, EscapeMode.ION_LONG_STRING);
            out.append("'''");
        }
    }


    /**
     * Builds a String denoting an ASCII-encoded Ion string,
     * with double-quotes surrounding a single Unicode code point.
     *
     * @param codePoint a Unicode code point.
     */
    public static String printCodePointAsString(int codePoint)
    {
        StringBuilder builder = new StringBuilder(12);
        builder.append('"');
        try
        {
            printStringCodePoint(builder, codePoint);
        }
        catch (IOException e)
        {
            // Shouldn't happen
            throw new Error(e);
        }
        builder.append('"');
        return builder.toString();
    }


    /**
     * Prints the text as an Ion symbol, including surrounding single-quotes if
     * they are necessary.  Operator symbols such as {@code '+'} are quoted.
     * If the {@code text} is null, this prints {@code null.symbol}.
     *
     * @param out the stream to receive the Ion data.
     * @param text the symbol text; may be {@code null}.
     *
     * @throws EmptySymbolException if {@code text} is empty.
     * @throws IOException if the {@link Appendable} throws an exception.
     *
     * @see #printSymbol(CharSequence)
     */
    public static void printSymbol(Appendable out, CharSequence text)
        throws IOException
    {
        if (text == null)
        {
            out.append("null.symbol");
        }
        else if (symbolNeedsQuoting(text, true))
        {
            printQuotedSymbol(out, text);
        }
        else
        {
            out.append(text);
        }
    }


    /**
     * Prints the text as an Ion symbol, including surrounding single-quotes if
     * they are necessary.  Operator symbols such as {@code '+'} are quoted.
     * If the {@code text} is null, this returns {@code "null.symbol"}.
     *
     * @param text the symbol text; may be {@code null}.
     *
     * @return a string containing the resulting Ion data.
     *
     * @see #printSymbol(Appendable, CharSequence)
     */
    public static String printSymbol(CharSequence text)
    {
        if (text == null)
        {
            return "null.symbol";
        }

        StringBuilder builder = new StringBuilder(text.length() + 2);
        try
        {
            printSymbol(builder, text);
        }
        catch (IOException e)
        {
            // Shouldn't happen
            throw new Error(e);
        }
        return builder.toString();
    }


    /**
     * Prints text as a single-quoted Ion symbol.
     * If the {@code text} is null, this prints {@code null.symbol}.
     *
     * @param out the stream to receive the data.
     * @param text the symbol text; may be {@code null}.
     *
     * @throws IOException if the {@link Appendable} throws an exception.
     * @throws EmptySymbolException if {@code text} is empty.
     * @throws IllegalArgumentException
     *     if the text contains invalid UTF-16 surrogates.
     */
    public static void printQuotedSymbol(Appendable out, CharSequence text)
        throws IOException
    {
        if (text == null)
        {
            out.append("null.symbol");
        }
        else if (text.length() == 0)
        {
            throw new EmptySymbolException();
        }
        else
        {
            out.append('\'');
            printCodePoints(out, text, EscapeMode.ION_SYMBOL);
            out.append('\'');
        }
    }


    /**
     * Builds a String containing a single-quoted Ion symbol.
     * If the {@code text} is null, this returns {@code "null.symbol"}.
     *
     * @param text the symbol text; may be {@code null}.
     *
     * @throws IllegalArgumentException
     *     if the text contains invalid UTF-16 surrogates.
     */
    public static String printQuotedSymbol(CharSequence text)
    {
        if (text == null)
        {
            return "null.symbol";
        }

        StringBuilder builder = new StringBuilder(text.length() + 2);
        try
        {
            printQuotedSymbol(builder, text);
        }
        catch (IOException e)
        {
            // Shouldn't happen
            throw new Error(e);
        }
        return builder.toString();
    }


    /**
     * @throws IllegalArgumentException
     *     if the text contains invalid UTF-16 surrogates.
     */
    private static void printCodePoints(Appendable out, CharSequence text,
                                        EscapeMode mode)
        throws IOException
    {
        int len = text.length();
        for (int i = 0; i < len; i++)
        {
            int c = text.charAt(i);

            if (_Private_IonConstants.isHighSurrogate(c))
            {
                i++;
                char c2;
                // I apologize for the embedded assignment, but the alternative
                // was worse.
                if (i >= len || !_Private_IonConstants.isLowSurrogate(c2 = text.charAt(i)))
                {
                    String message =
                        "text is invalid UTF-16. It contains an unmatched " +
                        "high surrogate 0x" + Integer.toHexString(c) +
                        " at index " + i;
                    throw new IllegalArgumentException(message);
                }
                c = _Private_IonConstants.makeUnicodeScalar(c, c2);
            }
            else if (_Private_IonConstants.isLowSurrogate(c))
            {
                String message =
                    "text is invalid UTF-16. It contains an unmatched " +
                    "low surrogate 0x" + Integer.toHexString(c) +
                    " at index " + i;
                throw new IllegalArgumentException(message);
            }

            printCodePoint(out, c, mode);
        }
    }


    ////////////////////////////////////

    // from the Java 1.5 spec at (with extra spacing to keep the parser happy):
    // http://java.sun.com/docs/books/jls/second_edition/html/lexical.doc.html#101089
    //
    //    EscapeSequence:
    //        \ b            = \ u 0008: backspace BS
    //        \ t            = \ u 0009: horizontal tab HT
    //        \ n            = \ u 000a: linefeed LF
    //        \ f            = \ u 000c: form feed FF
    //        \ r            = \ u 000d: carriage return CR
    //        \ "            = \ u 0022: double quote "
    //        \ '            = \ u 0027: single quote '
    //        \ \            = \ u 005c: backslash \
    //        \ 0            = \ u 0000: NUL

    // from the JSON RFC4627 at:
    // http://www.faqs.org/rfcs/rfc4627.html
    //    escape (
    //        %x22 /          ; "    quotation mark  U+0022
    //        %x5C /          ; \    reverse solidus U+005C
    //        %x2F /          ; /    solidus         U+002F
    //        %x62 /          ; b    backspace       U+0008
    //        %x66 /          ; f    form feed       U+000C
    //        %x6E /          ; n    line feed       U+000A
    //        %x72 /          ; r    carriage return U+000D
    //        %x74 /          ; t    tab             U+0009
    //        %x75 4HEXDIG )  ; uXXXX                U+XXXX
    //
    // escape = %x5C              ;

    // c++ character escapes from:
    //    http://www.cplusplus.com/doc/tutorial/constants.html
    //    \n newline
    //    \r carriage return
    //    \t tab
    //    \v vertical tab
    //    \b backspace
    //    \f form feed (page feed)
    //    \a alert (beep)
    //    \' single quote (')
    //    \" double quote (")
    //    \? question mark (?)
    //    \\ backslash (\)

    // from http://msdn2.microsoft.com/en-us/library/ms860944.aspx
    //    Table 1.4   Escape Sequences
    //    Escape Sequence Represents
    //    \a Bell (alert)
    //    \b Backspace
    //    \f Formfeed
    //    \n New line
    //    \r Carriage return
    //    \t Horizontal tab
    //    \v Vertical tab
    //    \' Single quotation mark
    //    \"  Double quotation mark
    //    \\ Backslash
    //    \? Literal question mark
    //    \ooo ASCII character in octal notation
    //    \xhhh ASCII character in hexadecimal notation

    //    Ion escapes, actual set:
    //     \a Bell (alert)
    //     \b Backspace
    //     \f Formfeed
    //     \n New line
    //     \r Carriage return
    //     \t Horizontal tab
    //     \v Vertical tab
    //     \' Single quotation mark
    //     xx not included: \` Accent grave
    //     \"  Double quotation mark
    //     \\ Backslash (solidus)
    //     \/ Slash (reverse solidus) U+005C
    //     \? Literal question mark
    //     \0 nul character
    //
    //     \xhh ASCII character in hexadecimal notation (1-2 digits)
    //    \\uhhhh Unicode character in hexadecimal
    //     \Uhhhhhhhh Unicode character in hexadecimal
}
