/*
 * Copyright 2007-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static software.amazon.ion.impl.PrivateIonConstants.MAX_LONG_TEXT_SIZE;
import static software.amazon.ion.impl.PrivateIonConstants.isHighSurrogate;
import static software.amazon.ion.impl.PrivateIonConstants.isLowSurrogate;
import static software.amazon.ion.impl.PrivateIonConstants.makeUnicodeScalar;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import software.amazon.ion.Decimal;
import software.amazon.ion.EmptySymbolException;
import software.amazon.ion.impl.Base64Encoder.TextStream;
import software.amazon.ion.system.IonTextWriterBuilder;
import software.amazon.ion.util.PrivateFastAppendable;


/**
 * Generic text sink that enables optimized output of both ASCII and UTF-16.
 *
 * @deprecated This is an internal API that is subject to change without notice.
 */
@Deprecated
public final class PrivateIonTextAppender
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


    /**
     * Escapes for U+00 through U+FF, for use in double-quoted Ion strings.
     * This includes escapes for all LATIN-1 code points U+80 through U+FF.
     */
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

    /**
     * Escapes for U+00 through U+FF, for use in triple-quoted Ion strings.
     * This includes escapes for all LATIN-1 code points U+80 through U+FF.
     */
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

    /**
     * Escapes for U+00 through U+FF, for use in single-quoted Ion symbols.
     * This includes escapes for all LATIN-1 code points U+80 through U+FF.
     */
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

    /**
     * Escapes for U+00 through U+FF, for use in double-quoted JSON strings.
     * This includes escapes for all LATIN-1 code points U+80 through U+FF.
     */
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


    private final PrivateFastAppendable myAppendable;
    private final boolean escapeNonAscii;


    PrivateIonTextAppender(PrivateFastAppendable out, boolean escapeNonAscii)
    {
        this.myAppendable   = out;
        this.escapeNonAscii = escapeNonAscii;
    }


    public static PrivateIonTextAppender
    forFastAppendable(PrivateFastAppendable out, Charset charset)
    {
        boolean escapeNonAscii = charset.equals(PrivateUtils.ASCII_CHARSET);
        return new PrivateIonTextAppender(out, escapeNonAscii);
    }


    /**
     * @param charset must be either {@link IonTextWriterBuilder#ASCII} or
     * {@link IonTextWriterBuilder#UTF8}. When ASCII is used, all non-ASCII
     * characters will be escaped. Otherwise, only select code points will be
     * escaped.
     */
    public static PrivateIonTextAppender forAppendable(Appendable out,
                                                         Charset charset)
    {
        PrivateFastAppendable fast = new AppendableFastAppendable(out);
        return forFastAppendable(fast, charset);
    }


    /**
     * Doesn't escape non-ASCII characters.
     */
    public static PrivateIonTextAppender forAppendable(Appendable out)
    {
        PrivateFastAppendable fast = new AppendableFastAppendable(out);
        boolean escapeNonAscii = false;
        return new PrivateIonTextAppender(fast, escapeNonAscii);
    }


    /**
     * @param charset must be either {@link IonTextWriterBuilder#ASCII} or
     * {@link IonTextWriterBuilder#UTF8}. When ASCII is used, all non-ASCII
     * characters will be escaped. Otherwise, only select code points will be
     * escaped.
     */
    public static PrivateIonTextAppender forOutputStream(OutputStream out,
                                                           Charset charset)
    {
        PrivateFastAppendable fast = new OutputStreamFastAppendable(out);
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
            // Find a span of non-escaped ASCII code points so we can write
            // them as quickly as possible.
            char c = 0;
            int j;
            for (j = i; j < len; ++j) {
                c = text.charAt(j);
                // The escapes array always includes U+80 through U+FF.
                if (c >= 0x100 || escapes[c] != null)
                {
                    // c is escaped and/or outside ASCII range.
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

            // We've found a code point that's escaped and/or non-ASCII.

            if (c < 0x80)
            {
                // An escaped ASCII character.
                assert escapes[c] != null;
                appendAscii(escapes[c]);
            }
            else if (c < 0x100)
            {
                // Non-ASCII LATIN-1; we will have an escape sequence but may
                // not use it.
                assert escapes[c] != null;

                // Always escape the C1 control codes U+80 through U+9F.
                if (escapeNonAscii || c <= 0x9F) {
                    appendAscii(escapes[c]);
                } else {
                    appendUtf16(c);
                }
            }
            else if (c < 0xD800 || c >= 0xE000)
            {
                // Not LATIN-1, but still in the BMP.
                String s = Integer.toHexString(c);
                if (escapeNonAscii) {
                    appendAscii(HEX_4_PREFIX);
                    appendAscii(ZERO_PADDING[4 - s.length()]);
                    appendAscii(s);
                } else {
                    appendUtf16(c);
                }
            }
            else if (isHighSurrogate(c))
            {
                // Outside the BMP! High surrogate must be followed by low.
                char c2;
                if (++i == len || !isLowSurrogate(c2 = text.charAt(i))) {
                    String message =
                        "text is invalid UTF-16. It contains an unmatched " +
                        "leading surrogate 0x" + Integer.toHexString(c) +
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
            }
            else
            {
                // unmatched low surrogate
                assert isLowSurrogate(c);

                String message =
                    "text is invalid UTF-16. It contains an unmatched " +
                    "trailing surrogate 0x" + Integer.toHexString(c) +
                    " at index " + i;
                throw new IllegalArgumentException(message);
            }
        }
    }


    //=========================================================================
    // Numeric scalars


    /** ONLY FOR USE BY {@link #printInt(long)}. */
    private final char[] _fixedIntBuffer = new char[MAX_LONG_TEXT_SIZE];

    public void printInt(long value)
        throws IOException
    {
        int j = _fixedIntBuffer.length;
        if (value == 0) {
            _fixedIntBuffer[--j] = '0';
        } else {
            if (value < 0) {
                while (value != 0) {
                    _fixedIntBuffer[--j] = (char)(0x30 - value % 10);
                    value /= 10;
                }
                _fixedIntBuffer[--j] = '-';
            } else {
                while (value != 0) {
                    _fixedIntBuffer[--j] = (char)(0x30 + value % 10);
                    value /= 10;
                }
            }
        }

        // Using CharBuffer avoids copying the _fixedIntBuffer into a String
        appendAscii(CharBuffer.wrap(_fixedIntBuffer),
                    j,
                    _fixedIntBuffer.length);
    }


    public void printInt(BigInteger value)
        throws IOException
    {
        if (value == null)
        {
            appendAscii("null.int");
            return;
        }

        appendAscii(value.toString());
    }


    public void printDecimal(PrivateIonTextWriterBuilder _options,
                             BigDecimal                    value)
        throws IOException
    {
        if (value == null)
        {
            appendAscii("null.decimal");
            return;
        }

        BigInteger unscaled = value.unscaledValue();

        int signum = value.signum();
        if (signum < 0)
        {
            appendAscii('-');
            unscaled = unscaled.negate();
        }
        else if (value instanceof Decimal
             && ((Decimal)value).isNegativeZero())
        {
            // for the various forms of negative zero we have to
            // write the sign ourselves, since neither BigInteger
            // nor BigDecimal recognize negative zero, but Ion does.
            appendAscii('-');
        }

        final String unscaledText = unscaled.toString();
        final int significantDigits = unscaledText.length();

        final int scale = value.scale();
        final int exponent = -scale;

        if (_options._decimal_as_float)
        {
            appendAscii(unscaledText);
            appendAscii('e');
            appendAscii(Integer.toString(exponent));
        }
        else if (exponent == 0)
        {
            appendAscii(unscaledText);
            appendAscii('.');
        }
        else if (exponent < 0)
        {
            // Avoid printing small negative exponents using a heuristic
            // adapted from http://speleotrove.com/decimal/daconvs.html

            final int adjustedExponent = significantDigits - 1 - scale;
            if (adjustedExponent >= 0)
            {
                int wholeDigits = significantDigits - scale;
                appendAscii(unscaledText, 0, wholeDigits);
                appendAscii('.');
                appendAscii(unscaledText, wholeDigits,
                                    significantDigits);
            }
            else if (adjustedExponent >= -6)
            {
                appendAscii("0.");
                appendAscii("00000", 0, scale - significantDigits);
                appendAscii(unscaledText);
            }
            else
            {
                appendAscii(unscaledText);
                appendAscii("d-");
                appendAscii(Integer.toString(scale));
            }
        }
        else // (exponent > 0)
        {
            // We cannot move the decimal point to the right, adding
            // rightmost zeros, because that would alter the precision.
            appendAscii(unscaledText);
            appendAscii('d');
            appendAscii(Integer.toString(exponent));
        }
    }


    public void printFloat(double value)
        throws IOException
    {
        // shortcut zero cases
        if (value == 0.0)
        {
            if (Double.compare(value, 0d) == 0)  // Only matches positive zero
            {
                appendAscii("0e0");
            }
            else
            {
                appendAscii("-0e0");
            }
        }
        else if (Double.isNaN(value))
        {
            appendAscii("nan");
        }
        else if (value == Double.POSITIVE_INFINITY)
        {
            appendAscii("+inf");
        }
        else if (value == Double.NEGATIVE_INFINITY)
        {
            appendAscii("-inf");
        }
        else
        {
            // Double.toString() forces a digit after the decimal point.
            // Remove it when it's not meaningful.
            String str = Double.toString(value);
            if (str.endsWith(".0"))
            {
                appendAscii(str, 0, str.length() - 2);
                appendAscii("e0");
            }
            else
            {
                appendAscii(str);
                if (str.indexOf('E') == -1)
                {
                    appendAscii("e0");
                }
            }
        }
    }

    public void printFloat(Double value)
        throws IOException
    {
        if (value == null)
        {
            appendAscii("null.float");
        }
        else
        {
            printFloat(value.doubleValue());
        }
    }


    //=========================================================================
    // LOBs


    public void printBlob(PrivateIonTextWriterBuilder _options,
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


    public void printClob(PrivateIonTextWriterBuilder _options,
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
