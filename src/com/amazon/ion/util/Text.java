/*
 * Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.util;

import com.amazon.ion.impl.IonConstants;
import java.io.IOException;


/**
 * Utility methods for working with Ion text.
 */
public class Text
{
    private enum EscapeMode { ION, JSON }

    public static final int ANY_SURROUNDING_QUOTES = -2;

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
            '~', '/', '?', '.', ';', '!', '|', '@', '`'
           };

        OPERATOR_CHAR_FLAGS = new boolean[256];

        for (int ii=0; ii<operatorChars.length; ii++) {
            char operator = operatorChars[ii];
            OPERATOR_CHAR_FLAGS[operator] = true;
        }
    }

    //=========================================================================

    /**
     * Ion whitespace is defined as one of the characters space, tab, newline,
     * and carriage-return.  This matches the definition of whitespace used by
     * JSON.
     *
     * @param c the character to test.
     * @return <code>true</code> if <code>c</code> is one of the four legal
     * Ion whitespace characters.
     *
     * @see <a href="http://tools.ietf.org/html/rfc4627">RFC 4627</a>
     */
    public static boolean isWhitespace(int c)
    {
        switch (c)
        {
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

    public final static boolean isNumericStopChar(int c)
    {
        switch (c) {
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

    public static boolean isDigit(int c, int radix) {
        switch (c) {
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


    public static boolean isIdentifierStartChar(int c) {
        if (c < ' ' || c > '~') {
            return false;
        }
        return IDENTIFIER_START_CHAR_FLAGS[c];
    }

    public static boolean isIdentifierFollowChar(int c) {
        if (c < ' ' || c > '~') {
            return false;
        }
        return IDENTIFIER_FOLLOW_CHAR_FLAGS[c];
    }

    public static boolean isOperatorChar(int c) {
        if (c < ' ' || c > '~') {
            return false;
        }
        return OPERATOR_CHAR_FLAGS[c];
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
    private static boolean isIdentifierKeyword(CharSequence text)
    {
        int pos = 0;
        int valuelen = text.length();
        boolean keyword = false;

        // there has to be at least 1 character or we wouldn't be here
        switch (text.charAt(pos++)) {
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
     * @throws IllegalArgumentException if <code>symbol</code> is empty.
     */
    public static boolean symbolNeedsQuoting(CharSequence symbol,
                                             boolean      quoteOperators)
    {
        int length = symbol.length();
        if (length == 0) {
            throw new IllegalArgumentException("symbol must be non-empty");
        }

        // If the symbol's text matches an Ion keyword, we must quote it.
        // Eg, the symbol 'false' must be rendered quoted.
        if (! isIdentifierKeyword(symbol))
        {
            char c = symbol.charAt(0);
            if (!quoteOperators && isOperatorChar(c))
            {
                for (int ii = 0; ii < length; ii++) {
                    c = symbol.charAt(ii);
                    // We don't need to look for escapes since all
                    // operator characters are ASCII.
                    if (!isOperatorChar(c)) {
                        return true;
                    }
                }
                return false;
            }
            else if (isIdentifierStartChar(c))
            {
                for (int ii = 0; ii < length; ii++) {
                    c = symbol.charAt(ii);
                    if (needsEscapeForAsciiPrinting(c, '\'')
                        || !isIdentifierFollowChar(c))
                    {
                        return true;
                    }
                }
                return false;
            }
        }

        return true;
    }


    //=========================================================================
    @Deprecated
    public static boolean needsEscapeForAsciiRendering(int c) {
        switch (c) {
        case '\"':
            return true;
        case '\'':
            return true;
        case '\\':
            return true;
        default:
            return (c < 32 || c > 126);
        }
    }

    /**
     * Determines whether a single character (Unicode code point) needs an
     * escape sequence for printing within an Ion text value.
     *
     * @param codePoint a Unicode code point.
     * @param surroundingQuoteChar must be either {@code '\''}, {@code '\"'},
     * or {@link #ANY_SURROUNDING_QUOTES}.
     *
     * @return {@code true} if the character needs to be escaped.
     */
    public static boolean needsEscapeForAsciiPrinting(int codePoint,
                                                      int surroundingQuoteChar)
    {
        switch (codePoint) {
        case '\"':
            return (surroundingQuoteChar == '\"');
        case '\'':
            return (surroundingQuoteChar == '\'');
        case '\\':
            return true;
        default:
            return (codePoint < 32 || codePoint > 126);
        }
    }

    @Deprecated
    public static Appendable renderAsAscii(int c, Appendable out)
        throws IOException
    {
        printAsIon(out, c, ANY_SURROUNDING_QUOTES);
        return out;
    }

    /**
     * Prints a single character (Unicode code point) in Ion ASCII text format,
     * using escapes suitable for a specified quoting context.
     *
     * @param out the stream to receive the data.
     * @param codePoint a Unicode code point.
     * @param surroundingQuoteChar must be either {@code '\''}, {@code '\"'},
     * or {@link #ANY_SURROUNDING_QUOTES}.
     *
     * @deprecated Use {@link #printStringCodePoint(Appendable, int)} or
     * {@link #printSymbolCodePoint(Appendable, int)}.
     */
    @Deprecated
    public static void printAsIon(Appendable out, int codePoint,
                                  int surroundingQuoteChar)
        throws IOException
    {
        printCodePoint(out, codePoint, surroundingQuoteChar, EscapeMode.ION);
    }

    /**
     * Prints a single Unicode code point for use in an ASCII-safe Ion string.
     *
     * @param out the stream to receive the data.
     * @param codePoint a Unicode code point.
     */
    public static void printStringCodePoint(Appendable out, int codePoint)
        throws IOException
    {
        printCodePoint(out, codePoint, '\"', EscapeMode.ION);
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
        printCodePoint(out, codePoint, '\'', EscapeMode.ION);
    }

    /**
     * Prints a single character (Unicode code point) in JSON string format,
     * escaping as necessary.
     *
     * @param out the stream to receive the data.
     * @param codePoint a Unicode code point.
     */
    public static void printJsonCodePoint(Appendable out, int codePoint)
        throws IOException
    {
        // JSON only allows double-quote strings.
        printCodePoint(out, codePoint, '"', EscapeMode.JSON);
    }

    /**
     * Prints a single character (Unicode code point) in JSON string format,
     * escaping as necessary.
     *
     * @param out the stream to receive the data.
     * @param codePoint a Unicode code point.
     *
     * @deprecated Renamed to {@link #printJsonCodePoint(Appendable, int)}.
     */
    @Deprecated
    public static void printAsJson(Appendable out, int codePoint)
        throws IOException
    {
        // JSON only allows double-quote strings.
        printCodePoint(out, codePoint, '"', EscapeMode.JSON);
    }

    private static void printCodePoint(Appendable out, int c, int quote,
                                       EscapeMode mode)
        throws IOException
    {
        // JSON only allows uHHHH numeric escapes.
        switch (c) {
            case 0:
                out.append(mode == EscapeMode.ION ? "\\0" : "\\u0000");
                return;
            case '\t':
                out.append("\\t");
                return;
            case '\n':
                out.append("\\n");
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
                out.append(mode == EscapeMode.ION ? "\\a" : "\\u0007");
                return;
            case '\u000B':
                out.append("\\v");
                return;
            case '\"':
                if (quote == c || quote == ANY_SURROUNDING_QUOTES) {
                    out.append("\\\"");
                    return;
                }
                break;
            case '\'':
                if (quote == c || quote == ANY_SURROUNDING_QUOTES) {
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
            // FIXME JSON doesn't support eight-digit \U syntax!
            // https://issue-tracking.amazon.com/browse/ION-33
            printCodePointAsEightHexDigits(out, c);
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

    private static void printCodePointAsTwoHexDigits(Appendable out, int c)
        throws IOException
    {
        String s = Integer.toHexString(c);
        out.append("\\x");
        out.append(ZERO_PADDING[2-s.length()]);
        out.append(s);
    }

    private static void printCodePointAsFourHexDigits(Appendable out, int c)
        throws IOException
    {
        String s = Integer.toHexString(c);
        out.append("\\u");
        out.append(ZERO_PADDING[4-s.length()]);
        out.append(s);
    }

    private static void printCodePointAsEightHexDigits(Appendable out, int c)
        throws IOException
    {
        String s = Integer.toHexString(c);
        out.append("\\U");
        out.append(ZERO_PADDING[8-s.length()]);
        out.append(s);
    }


    /**
     * Renders text content as ASCII using escapes suitable for Ion strings and
     * symbols.  No surrounding quotation marks are rendered.
     *
     * @param text the text to render.
     * @param out the stream to receive the data.
     *
     * @return the same instance supplied by the parameter {@code out}.
     *
     * @throws IOException if the {@link Appendable} throws an exception.
     */
    @Deprecated
    public static Appendable printAsAscii(CharSequence text, Appendable out)
        throws IOException
    {
        printAsIon(out, text, ANY_SURROUNDING_QUOTES);
        return out;
    }

    /**
     * Prints characters as Ion ASCII text content, using escapes suitable for
     * a specified quoting context. No surrounding quotation marks are printed.
     *
     * @param out the stream to receive the data.
     * @param text the text to print.
     * @param surroundingQuoteChar must be either {@code '\''}, {@code '\"'},
     * or {@link #ANY_SURROUNDING_QUOTES}.
     *
     * @throws IOException if the {@link Appendable} throws an exception.
     * @throws IllegalArgumentException
     *     if the text contains invalid UTF-16 surrogates.
     *
     * @deprecated Use {@link #printString(Appendable, CharSequence)} or
     * {@link #printSymbol(Appendable, CharSequence)}.
     */
    @Deprecated
    public static void printAsIon(Appendable out, CharSequence text,
                                  int surroundingQuoteChar)
        throws IOException
    {
        printChars(out, text, surroundingQuoteChar, EscapeMode.ION);
    }



    /**
     * Prints characters as an ASCII-encoded Ion string, including surrounding
     * double-quotes.
     * If the {@code text} is null, this prints {@code null.string}.
     *
     * @param out the stream to receive the data.
     * @param text the text to print, may be {@code null}.
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
            printChars(out, text, '"', EscapeMode.ION);
            out.append('"');
        }
    }

    /**
     * Prints characters as an ASCII-encoded JSON string, including surrounding
     * double-quotes.
     * If the {@code text} is null, this prints {@code null}.
     *
     * @param out the stream to receive the JSON data.
     * @param text the text to print, may be {@code null}.
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
            printChars(out, text, '"', EscapeMode.JSON);
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
     * @throws IOException if the {@link Appendable} throws an exception.
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
        else
        {
            out.append('\'');
            printChars(out, text, '\'', EscapeMode.ION);
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
     * Prints characters as JSON ASCII text content.
     * No surrounding quotation marks are printed.
     *
     * @param out the stream to receive the data.
     * @param text the text to print.
     *
     * @throws IOException if the {@link Appendable} throws an exception.
     * @throws IllegalArgumentException
     *     if the text contains invalid UTF-16 surrogates.
     *
     * @deprecated Use {@link #printJsonString(Appendable, CharSequence)},
     * which always prints surrounding double-quotes.
     */
    @Deprecated
    public static void printAsJson(Appendable out, CharSequence text)
        throws IOException
    {
        printChars(out, text, '"', EscapeMode.JSON);
    }

    /**
     * @throws IllegalArgumentException
     *     if the text contains invalid UTF-16 surrogates.
     */
    private static void printChars(Appendable out, CharSequence text,
                                   int surroundingQuoteChar, EscapeMode mode)
        throws IOException
    {
        int len = text.length();
        for (int i = 0; i < len; i++)
        {
            int c = text.charAt(i);

            if (IonConstants.isHighSurrogate(c))
            {
                i++;
                char c2 = text.charAt(i);
                if (i >= len || !IonConstants.isLowSurrogate(c2))
                {
                    String message =
                        "text is invalid UTF-16. It contains an unmatched " +
                        "high surrogate 0x" + Integer.toHexString(c) +
                        " at index " + i;
                    throw new IllegalArgumentException(message);
                }
                c = IonConstants.makeUnicodeScalar(c, c2);
            }
            else if (IonConstants.isLowSurrogate(c))
            {
                String message =
                    "text is invalid UTF-16. It contains an unmatched " +
                    "low surrogate 0x" + Integer.toHexString(c) +
                    " at index " + i;
                throw new IllegalArgumentException(message);
            }

            printCodePoint(out, c, surroundingQuoteChar, mode);
        }
    }


    ////////////////////////////////////

    static boolean isLetterOrDigit(int c) {
        switch (c) {
        case '0': case '1': case '2': case '3': case '4':
        case '5': case '6': case '7': case '8': case '9':
        case '_': case '$':
        case 'A': case 'B': case 'C': case 'D': case 'E': case 'F':
        case 'G': case 'H': case 'I': case 'J': case 'K': case 'L':
        case 'M': case 'N': case 'O': case 'P': case 'Q': case 'R':
        case 'S': case 'T': case 'U': case 'V': case 'W': case 'X':
        case 'Y': case 'Z':
        case 'a': case 'b': case 'c': case 'd': case 'e': case 'f':
        case 'g': case 'h': case 'i': case 'j': case 'k': case 'l':
        case 'm': case 'n': case 'o': case 'p': case 'q': case 'r':
        case 's': case 't': case 'u': case 'v': case 'w': case 'x':
        case 'y': case 'z':
            return true;
        default:
            return false;
        }
    }


    static boolean[] isEscapeOutChar = getEscapeOutChars();
    static boolean isEscapeOutChar(int c) {
        if (c < ' ' || c > '~') return true;
        return isEscapeOutChar[c];
    }
    static boolean[] getEscapeOutChars() {
        final char[] escoutchars = { '\\' ,'\"', '\'' };

        boolean[] flags = new boolean[256];

        for (int ii=0; ii<escoutchars.length; ii++) {
            int jj = escoutchars[ii];
            flags[jj] = true;
        }
        for (int ii=0; ii<32; ii++) {
            flags[ii] = true;
        }
        flags[255] = true;
        return flags;
    }

    static boolean[] isEscapeInChar = getEscapeInChars();
    static boolean isEscapeInChar(int c) {
        if (c < ' ' || c > '~') return false;
        return isEscapeInChar[c];
    }
    static boolean[] getEscapeInChars() {
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

        final char[] escinchars = {
                'a', 'b', 'f', 'n', 'r', 't', 'v',
                '\'', '\"', '\\', '/', '?',
                'u', 'U', 'x',
                '0',
             };

        boolean[] flags = new boolean[256];

        for (int ii=0; ii<escinchars.length; ii++) {
            int jj = escinchars[ii];
            flags[jj] = true;
        }

        return flags;
    }

    /**
     * @deprecated  Use {@line #printAsIon(Appendable, int, int)}.
     */
    @Deprecated
    public static String getEscapeString(int c, int surroundingQuoteChar) {
        switch (c) {
        case '\u0007':     return "\\a";
        case '\u0008':     return "\\b";
        case '\f':         return "\\f";
        case '\n':         return "\\n";
        case '\r':         return "\\r";
        case '\t':         return "\\t";
        case '\u000b':     return "\\v";
        case '\\':         return "\\\\";
        case '\"':         if (surroundingQuoteChar == c
                            || surroundingQuoteChar == ANY_SURROUNDING_QUOTES
                        ) {
                            return "\\\"";
                        }
                        break;
        case '\'':           if (surroundingQuoteChar == c
                            || surroundingQuoteChar == ANY_SURROUNDING_QUOTES
                            ) {
                                return "\\\'";
                            }
                            break;
        case '/':          return "\\/";
        case '?':          return "\\?";
        default:
            if (c == 0) {
                return "\\0";
            }
            else if (c < 32) {
                if (c < 0) {
                    throw new IllegalArgumentException("fatal - unescapable int character to escape");
                }
                return "\\x"+Integer.toHexString(c & 0xFF);
            }
            else if (c > '~') {
                String s = Integer.toHexString(c);
                if (s.length() > 4) {
                    return "\\U00000000".substring(0, 10-s.length()) + s;
                }
                return "\\u0000".substring(0, 6-s.length()) + s;
            }
            break;
        }
        return ""+(char)c;
    }
}
