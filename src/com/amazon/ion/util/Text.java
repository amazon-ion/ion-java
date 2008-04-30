/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.util;

import java.io.IOException;


/**
 * Utility methods for working with Ion text.
 */
public class Text
{
    public static final int EscapeAllQuoteCharactersCharacter = -2;

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

    public static boolean isNumericStopChar(int c)
    {
        switch (c)
        {
            case '{':  case '}':
            case '[':  case ']':
            case '(':  case ')':
            case ',':
            case '\"': case '\'':
            case ' ':  case '\t':  case '\n':  case '\r':  // Whitespace
            {
                return true;
            }
            default:
            {
                return false;
            }
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
                    if (needsEscapeForAsciiRendering(c, '\'')
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

    public static boolean needsEscapeForAsciiRendering(int c, int quote) {
        switch (c) {
        case '\"':
            return (quote == '\"');
        case '\'':
            return (quote == '\'');
        case '\\':
            return true;
        default:
            return (c < 32 || c > 126);
        }
    }

    @Deprecated
    public static Appendable renderAsAscii(int c, Appendable out)
        throws IOException
    {
        return renderAsAscii(c, EscapeAllQuoteCharactersCharacter, out);
    }

    public static Appendable renderAsAscii(int c, int quote, Appendable out)
        throws IOException
    {
        switch (c) {
            case 0:        return out.append("\\0");
            case '\t':     return out.append("\\t");
            case '\n':     return out.append("\\n");
            case '\r':     return out.append("\\r");
            case '\f':     return out.append("\\f");
            case '\u0008': return out.append("\\b");
            case '\u0007': return out.append("\\a");
            case '\u000B': return out.append("\\v");
            case '\"':	   if (quote == c
                            || quote == EscapeAllQuoteCharactersCharacter
                           ) {
                               return out.append("\\\"");
                           }
                           break;
            case '\'':     if (quote == c
                            || quote == EscapeAllQuoteCharactersCharacter
                           ) {
                               return out.append("\\\'");
                              }
                           break;
            case '\\':     return out.append("\\\\");
            default:
                break;
        }

        if (c < 32) {
            out.append("\\x");
            out.append(Integer.toHexString(c & 0xFF));
        }
        else if (c > 126) {
            String s = Integer.toHexString(c);
            out.append("\\u0000".substring(0, 6-s.length()));
            out.append(s);
        }
        else {
            out.append((char) c);
        }
        return out;
    }

    /**
     * Renders text content as ASCII using escapes suitable for Ion strings and
     * symbols.  No surrounding quotation marks are rendered.
     *
     * @param text the text to render.
     * @param out the stream to recieve the data.
     *
     * @return the same instance supplied by the parameter {@code out}.
     *
     * @throws IOException if the {@link Appendable} throws an exception.
     */
    @Deprecated
    public static Appendable printAsAscii(CharSequence text, Appendable out)
        throws IOException
    {
        return printAsAscii(text, EscapeAllQuoteCharactersCharacter, out);
    }

    /**
     * Renders text content as ASCII using escapes suitable for Ion strings and
     * symbols.  No surrounding quotation marks are rendered.
     *
     * @param text the text to render.
     * @param out the stream to recieve the data.
     *
     * @return the same instance supplied by the parameter {@code out}.
     *
     * @throws IOException if the {@link Appendable} throws an exception.
     */
    public static Appendable printAsAscii(CharSequence text, int quote, Appendable out)
        throws IOException
    {
        int len = text.length();
        for (int i = 0; i < len; i++)
        {
            int c = text.charAt(i);
            renderAsAscii(c, quote, out);
        }
        return out;
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

    @Deprecated
    public static String getEscapeString(int c) {
        return getEscapeString(c, EscapeAllQuoteCharactersCharacter);
    }

    public static String getEscapeString(int c, int quote) {
        switch (c) {
        case '\u0007':     return "\\a";
        case '\u0008':     return "\\b";
        case '\f':         return "\\f";
        case '\n':         return "\\n";
        case '\r':         return "\\r";
        case '\t':         return "\\t";
        case '\u000b':     return "\\v";
        case '\\':         return "\\\\";
        case '\"':         if (quote == c
                            || quote == EscapeAllQuoteCharactersCharacter
                           ) {
                               return "\\\"";
                           }
                           break;
        case '\'':		   if (quote == c
                            || quote == EscapeAllQuoteCharactersCharacter
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
