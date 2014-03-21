// Copyright (c) 2007-2014 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl._Private_IonConstants.isHighSurrogate;
import static com.amazon.ion.impl._Private_IonConstants.isLowSurrogate;
import static com.amazon.ion.impl._Private_IonConstants.makeUnicodeScalar;

import com.amazon.ion.EmptySymbolException;
import com.amazon.ion.FastAppendable;
import com.amazon.ion.impl.Base64Encoder.TextStream;
import com.amazon.ion.system.IonTextWriterBuilder;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.CharBuffer;
import java.nio.charset.Charset;


/**
 * NOT FOR APPLICATION USE!
 * <p>
 * Generic text sink that enables optimized output of both ASCII and UTF-16.
 */
public final class _Private_IonTextAppender
    implements Closeable, Flushable
{
    private static boolean is8bitValue(int v)
    {
        return (v & ~0xff) == 0;
    }

    private static boolean isDecimalDigit(int codePoint)
    {
        return (codePoint >= '0' && codePoint <= '9');
    }

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

    public static boolean isIdentifierStart(int codePoint) {
        return IDENTIFIER_START_CHAR_FLAGS[codePoint & 0xff] && is8bitValue(codePoint);
    }

    public static boolean isIdentifierPart(int codePoint) {
        return IDENTIFIER_FOLLOW_CHAR_FLAGS[codePoint & 0xff] && is8bitValue(codePoint);
    }

    public static final boolean[] OPERATOR_CHAR_FLAGS;
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

    public static boolean isOperatorPart(int codePoint) {
        return OPERATOR_CHAR_FLAGS[codePoint & 0xff] && is8bitValue(codePoint);
    }


    public static final String[] ZERO_PADDING =
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
    private static final String[] STRING_ESCAPE_CODES;
    static
    {
        STRING_ESCAPE_CODES = new String[256];
        STRING_ESCAPE_CODES[0x00] = "\\0";
        STRING_ESCAPE_CODES[0x07] = "\\a";
        STRING_ESCAPE_CODES[0x08] = "\\b";
        STRING_ESCAPE_CODES['\t'] = "\\t";
        STRING_ESCAPE_CODES['\n'] = "\\n";
        STRING_ESCAPE_CODES[0x0B] = "\\v";
        STRING_ESCAPE_CODES['\f'] = "\\f";
        STRING_ESCAPE_CODES['\r'] = "\\r";
        STRING_ESCAPE_CODES['\\'] = "\\\\";
        STRING_ESCAPE_CODES['\"'] = "\\\"";
        for (int i = 1; i < 0x20; ++i) {
            if (STRING_ESCAPE_CODES[i] == null) {
                String s = Integer.toHexString(i);
                STRING_ESCAPE_CODES[i] = "\\x" + ZERO_PADDING[2 - s.length()] + s;
            }
        }
        for (int i = 0x7F; i < 0x100; ++i) {
            String s = Integer.toHexString(i);
            STRING_ESCAPE_CODES[i] = "\\x" + s;
        }
    }

    static final String[] LONG_STRING_ESCAPE_CODES;
    static
    {
        LONG_STRING_ESCAPE_CODES = new String[256];
        for (int i = 0; i < 256; ++i) {
            LONG_STRING_ESCAPE_CODES[i] = STRING_ESCAPE_CODES[i];
        }
        LONG_STRING_ESCAPE_CODES['\n'] = null;
        LONG_STRING_ESCAPE_CODES['\''] = "\\\'";
        LONG_STRING_ESCAPE_CODES['\"'] = null; // Treat as normal code point for long string
    }

    static final String[] SYMBOL_ESCAPE_CODES;
    static
    {
        SYMBOL_ESCAPE_CODES = new String[256];
        for (int i = 0; i < 256; ++i) {
            SYMBOL_ESCAPE_CODES[i] = STRING_ESCAPE_CODES[i];
        }
        SYMBOL_ESCAPE_CODES['\''] = "\\\'";
        SYMBOL_ESCAPE_CODES['\"'] = null; // Treat as normal code point for symbol.
    }

    static final String[] JSON_ESCAPE_CODES;
    static
    {
        JSON_ESCAPE_CODES = new String[256];
        JSON_ESCAPE_CODES[0x08] = "\\b";
        JSON_ESCAPE_CODES['\t'] = "\\t";
        JSON_ESCAPE_CODES['\n'] = "\\n";
        JSON_ESCAPE_CODES['\f'] = "\\f";
        JSON_ESCAPE_CODES['\r'] = "\\r";
        JSON_ESCAPE_CODES['\\'] = "\\\\";
        JSON_ESCAPE_CODES['\"'] = "\\\"";

        // JSON requires all of these characters to be escaped.
        for (int i = 0; i < 0x20; ++i) {
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

    private static final String HEX_4_PREFIX = "\\u";
    private static final String HEX_8_PREFIX = "\\U";
    private static final String TRIPLE_QUOTES = "'''";


    //=========================================================================


    private final FastAppendable myAppendable;
    private final boolean escapeNonAscii;


    _Private_IonTextAppender(FastAppendable out, boolean escapeNonAscii)
    {
        this.myAppendable   = out;
        this.escapeNonAscii = escapeNonAscii;
    }


    public static _Private_IonTextAppender
    forFastAppendable(FastAppendable out, Charset charset)
    {
        boolean escapeNonAscii = charset.equals(_Private_Utils.ASCII_CHARSET);
        return new _Private_IonTextAppender(out, escapeNonAscii);
    }


    /**
     * @param charset must be either {@link IonTextWriterBuilder#ASCII} or
     * {@link IonTextWriterBuilder#UTF8}. When ASCII is used, all non-ASCII
     * characters will be escaped. Otherwise, only select code points will be
     * escaped.
     */
    public static _Private_IonTextAppender forAppendable(Appendable out,
                                                         Charset charset)
    {
        FastAppendable fast = new AppendableIonTextAppender(out);
        return forFastAppendable(fast, charset);
    }


    /**
     * Doesn't escape non-ASCII characters.
     */
    public static _Private_IonTextAppender forAppendable(Appendable out)
    {
        FastAppendable fast = new AppendableIonTextAppender(out);
        boolean escapeNonAscii = false;
        return new _Private_IonTextAppender(fast, escapeNonAscii);
    }


    /**
     * @param charset must be either {@link IonTextWriterBuilder#ASCII} or
     * {@link IonTextWriterBuilder#UTF8}. When ASCII is used, all non-ASCII
     * characters will be escaped. Otherwise, only select code points will be
     * escaped.
     */
    public static _Private_IonTextAppender forOutputStream(OutputStream out,
                                                           Charset charset)
    {
        FastAppendable fast = new OutputStreamIonTextAppender(out);
        return forFastAppendable(fast, charset);
    }


    //=========================================================================


    public void flush()
        throws IOException
    {
        if (myAppendable instanceof Flushable)
        {
            ((Flushable) myAppendable).flush();
        }
    }

    public void close()
        throws IOException
    {
        if (myAppendable instanceof Closeable)
        {
            ((Closeable) myAppendable).close();
        }
    }


    public void appendAscii(char c)
        throws IOException
    {
        myAppendable.appendAscii(c);
    }

    public void appendAscii(CharSequence csq)
        throws IOException
    {
        myAppendable.appendAscii(csq);
    }

    public void appendAscii(CharSequence csq, int start, int end)
        throws IOException
    {
        myAppendable.appendAscii(csq, start, end);
    }

    public void appendUtf16(char c)
        throws IOException
    {
        myAppendable.appendUtf16(c);
    }

    public void appendUtf16Surrogate(char leadSurrogate, char trailSurrogate)
        throws IOException
    {
        myAppendable.appendUtf16Surrogate(leadSurrogate, trailSurrogate);
    }


    //=========================================================================


    /**
     * Print an Ion String type
     * @param text
     * @throws IOException
     */
    public final void printString(CharSequence text)
        throws IOException
    {
        if (text == null)
        {
            appendAscii("null.string");
        }
        else
        {
            appendAscii('"');
            printCodePoints(text, STRING_ESCAPE_CODES);
            appendAscii('"');
        }
    }

    /**
     * Print an Ion triple-quoted string
     * @param text
     * @throws IOException
     */
    public final void printLongString(CharSequence text)
        throws IOException
    {
        if (text == null)
        {
            appendAscii("null.string");
        }
        else
        {
            appendAscii(TRIPLE_QUOTES);
            printCodePoints(text, LONG_STRING_ESCAPE_CODES);
            appendAscii(TRIPLE_QUOTES);
        }
    }

    /**
     * Print a JSON string
     * @param text
     * @throws IOException
     */
    public final void printJsonString(CharSequence text)
        throws IOException
    {
        if (text == null)
        {
            appendAscii("null");
        }
        else
        {
            appendAscii('"');
            printCodePoints(text, JSON_ESCAPE_CODES);
            appendAscii('"');
        }
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
    public static boolean isIdentifierKeyword(CharSequence text)
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
    public static boolean symbolNeedsQuoting(CharSequence symbol,
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
     * Print an Ion Symbol type. This method will check if symbol needs quoting
     * @param text
     * @throws IOException
     */
    public final void printSymbol(CharSequence text)
        throws IOException
    {
        if (text == null)
        {
            appendAscii("null.symbol");
        }
        else if (text.length() == 0)
        {
            throw new EmptySymbolException();
        }
        else if (symbolNeedsQuoting(text, true)) {
            appendAscii('\'');
            printCodePoints(text, SYMBOL_ESCAPE_CODES);
            appendAscii('\'');
        }
        else
        {
            appendAscii(text);
        }
    }

    /**
     * Print single-quoted Ion Symbol type
     * @param text
     * @throws IOException
     */
    public final void printQuotedSymbol(CharSequence text)
        throws IOException
    {
        if (text == null)
        {
            appendAscii("null.symbol");
        }
        else if (text.length() == 0)
        {
            throw new EmptySymbolException();
        }
        else
        {
            appendAscii('\'');
            printCodePoints(text, SYMBOL_ESCAPE_CODES);
            appendAscii('\'');
        }
    }

    private final void printCodePoints(CharSequence text, String[] escapes)
        throws IOException
    {
        int len = text.length();
        for (int i = 0; i < len; ++i)
        {
            // write as many bytes in a sequence as possible
            char c = 0;
            int j;
            for (j = i; j < len; ++j) {
                c = text.charAt(j);
                if (c >= 0x100 || escapes[c] != null) {
                    // append sequence then continue the normal loop
                    if (j > i) {
                        appendAscii(text, i, j);
                        i = j;
                    }
                    break;
                }
            }
            if (j == len) {
                // we've reached the end of sequence; append it and break
                appendAscii(text, i, j);
                break;
            }
            // write the non Latin-1 character
            if (c < 0x80) {
                assert escapes[c] != null;
                appendAscii(escapes[c]);
            } else if (c < 0x100) {
                assert escapes[c] != null;
                if (escapeNonAscii) {
                    appendAscii(escapes[c]);
                } else {
                    appendUtf16(c);
                }
            } else if (c < 0xD800 || c >= 0xE000) {
                String s = Integer.toHexString(c);
                if (escapeNonAscii) {
                    appendAscii(HEX_4_PREFIX);
                    appendAscii(ZERO_PADDING[4 - s.length()]);
                    appendAscii(s);
                } else {
                    appendUtf16(c);
                }
            } else if (isHighSurrogate(c)) {
                // high surrogate
                char c2;
                if (++i == len || !isLowSurrogate(c2 = text.charAt(i))) {
                    String message =
                        "text is invalid UTF-16. It contains an unmatched " +
                        "high surrogate 0x" + Integer.toHexString(c) +
                        " at index " + (i-1);
                    throw new IllegalArgumentException(message);
                }
                if (escapeNonAscii) {
                    int cp = makeUnicodeScalar(c, c2);
                    String s = Integer.toHexString(cp);
                    appendAscii(HEX_8_PREFIX);
                    appendAscii(ZERO_PADDING[8 - s.length()]);
                    appendAscii(s);
                } else {
                    appendUtf16Surrogate(c, c2);
                }
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


    //=========================================================================
    // LOBs


    public void printBlob(_Private_IonTextWriterBuilder _options,
                          byte[] value, int start, int len)
        throws IOException
    {
        if (value == null)
        {
            appendAscii("null.blob");
            return;
        }

        @SuppressWarnings("resource")
        TextStream ts =
            new TextStream(new ByteArrayInputStream(value, start, len));

        // base64 encoding is 6 bits per char so
        // it evens out at 3 bytes in 4 characters
        char[] buf = new char[_options.isPrettyPrintOn() ? 80 : 400];
        CharBuffer cb = CharBuffer.wrap(buf);

        if (_options._blob_as_string)
        {
            appendAscii('"');
        }
        else
        {
            appendAscii("{{");
            if (_options.isPrettyPrintOn())
            {
                appendAscii(' ');
            }
        }

        for (;;)
        {
            // TODO is it better to fill up the CharBuffer before outputting?
            int clen = ts.read(buf, 0, buf.length);
            if (clen < 1) break;
            appendAscii(cb, 0, clen);
        }

        if (_options._blob_as_string)
        {
            appendAscii('"');
        }
        else
        {
            if (_options.isPrettyPrintOn())
            {
                appendAscii(' ');
            }
            appendAscii("}}");
        }
    }


    private void printClobBytes(byte[] value, int start, int end,
                                String[] escapes)
        throws IOException
    {
        for (int i = start; i < end; i++) {
            char c = (char)(value[i] & 0xff);
            String escapedByte = escapes[c];
            if (escapedByte != null) {
                appendAscii(escapedByte);
            } else {
                appendAscii(c);
            }
        }
    }


    public void printClob(_Private_IonTextWriterBuilder _options,
                          byte[] value, int start, int len)
        throws IOException
    {
        if (value == null)
        {
            appendAscii("null.clob");
            return;
        }


        final boolean json =
            _options._clob_as_string && _options._string_as_json;

        final int threshold = _options.getLongStringThreshold();
        final boolean longString = (0 < threshold && threshold < value.length);

        if (!_options._clob_as_string)
        {
            appendAscii("{{");
            if (_options.isPrettyPrintOn())
            {
                appendAscii(' ');
            }
        }

        if (json)
        {
            appendAscii('"');
            printClobBytes(value, start, start + len, JSON_ESCAPE_CODES);
            appendAscii('"');
        }
        else if (longString)
        {
            // This may escape more often than is necessary, but doing it
            // minimally is very tricky. Must be sure to account for
            // quotes at the end of the content.

            // TODO Account for NL versus CR+NL streams
            appendAscii(TRIPLE_QUOTES);
            printClobBytes(value, start, start + len, LONG_STRING_ESCAPE_CODES);
            appendAscii(TRIPLE_QUOTES);
        }
        else
        {
            appendAscii('"');
            printClobBytes(value, start, start + len, STRING_ESCAPE_CODES);
            appendAscii('"');
        }

        if (! _options._clob_as_string)
        {
            if (_options.isPrettyPrintOn())
            {
                appendAscii(' ');
            }
            appendAscii("}}");
        }
    }
}
