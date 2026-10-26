// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonList;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonText;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.OffsetSpan;
import com.amazon.ion.SpanProvider;
import com.amazon.ion.TextSpan;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The text reader's bulk paths (runs of plain ASCII copied or skipped within a page, base64 groups decoded from
 * the page, field names read in one pass, unread without a page lookup) give the same results as the
 * character-at-a-time code they shortcut: wherever a page ends, whatever the input source, and when values are
 * skipped instead of read.
 */
public class IonReaderTextFastPathsTest {

    private static final IonSystem SYSTEM = IonSystemBuilder.standard().build();
    /** UnifiedInputStreamX's page size for stream input: values longer than this span pages. */
    private static final int PAGE = UnifiedInputStreamX.DEFAULT_PAGE_SIZE;

    /**
     * How the reader gets its input. Streams returning a few bytes per read end pages at arbitrary positions. One-byte
     * reads are only used with small inputs: each refill takes a new page while a value is being saved, so a value
     * longer than a page read a byte at a time would need a page per byte.
     */
    enum Source {
        BYTES, STREAM, STREAM_1_BYTE_READS, STREAM_4093_BYTE_READS, STRING, READER;

        IonReader open(String text) {
            byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
            switch (this) {
                case BYTES: return SYSTEM.newReader(bytes);
                case STREAM: return SYSTEM.newReader(new ByteArrayInputStream(bytes));
                case STREAM_1_BYTE_READS: return SYSTEM.newReader(new LimitedReads(bytes, 1));
                case STREAM_4093_BYTE_READS: return SYSTEM.newReader(new LimitedReads(bytes, 4093));
                case STRING: return SYSTEM.newReader(text);
                case READER: return SYSTEM.newReader(new StringReader(text));
                default: throw new IllegalStateException();
            }
        }
    }

    /** Returns at most {@code max} bytes per read. */
    private static final class LimitedReads extends FilterInputStream {
        private final int max;

        LimitedReads(byte[] bytes, int max) {
            super(new ByteArrayInputStream(bytes));
            this.max = max;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return super.read(b, off, Math.min(len, max));
        }
    }

    private static IonDatagram load(Source source, String text) {
        IonDatagram datagram = SYSTEM.newDatagram();
        try (IonReader reader = source.open(text)) {
            IonWriter writer = SYSTEM.newWriter(datagram);
            writer.writeValues(reader);
            writer.close();
        } catch (IOException e) {
            throw new IonException(e);
        }
        return datagram;
    }

    @ParameterizedTest
    @EnumSource(value = Source.class, names = "STREAM_1_BYTE_READS", mode = EnumSource.Mode.EXCLUDE)
    public void stringsAndSymbolsLongerThanAPage(Source source) {
        StringBuilder text = new StringBuilder();
        String plain = repeat("abcdefghijklmnopqrstuvwxyz0123456789 !#$%&()*+,-./:;<=>?@[]^_`{|}~", PAGE / 40);
        // Plain runs broken by every kind of character the bulk copy must stop at.
        text.append('"').append(plain).append("\\n\\\"\\\\\\u00e9é\\U0001F600😀").append(plain).append("\" ");
        String identifier = "_$" + repeat("abcXYZ019", PAGE / 6);
        text.append(identifier).append(' ');
        text.append("{").append(identifier).append(":\"").append(plain).append("\"}");

        IonDatagram values = load(source, text.toString());

        assertEquals(3, values.size());
        assertEquals(plain + "\n\"\\\u00e9\u00e9\uD83D\uDE00\uD83D\uDE00" + plain, stringOf(values.get(0)));
        assertEquals(identifier, stringOf(values.get(1)));
        assertEquals(plain, stringOf(((IonStruct) values.get(2)).get(identifier)));
    }

    @ParameterizedTest
    @EnumSource(value = Source.class, names = "STREAM_1_BYTE_READS", mode = EnumSource.Mode.EXCLUDE)
    public void blobsDecodeTheSameWithWhitespaceAndPadding(Source source) {
        Random random = new Random(1);
        List<byte[]> blobs = new ArrayList<>();
        StringBuilder text = new StringBuilder();
        for (int length : new int[] {0, 1, 2, 3, 4, 5, 57, 58, 59, PAGE, PAGE + 1, 3 * PAGE + 2}) {
            byte[] bytes = new byte[length];
            random.nextBytes(bytes);
            blobs.add(bytes);
            String base64 = Base64.getEncoder().encodeToString(bytes);
            // Plain, wrapped every 76 characters, and with whitespace inside a 4-character group.
            text.append("{{").append(base64).append("}} ");
            text.append("{{ ").append(Base64.getMimeEncoder().encodeToString(bytes)).append("\n}} ");
            text.append("{{").append(base64.length() > 2 ? base64.substring(0, 2) + " \t" + base64.substring(2) : base64)
                    .append("}} ");
        }

        try (IonReader reader = source.open(text.toString())) {
            for (byte[] expected : blobs) {
                for (int form = 0; form < 3; form++) {
                    assertEquals(IonType.BLOB, reader.next());
                    assertEquals(expected.length, reader.byteSize());
                    assertArrayEquals(expected, reader.newBytes());
                }
            }
            assertNull(reader.next());
        } catch (IOException e) {
            throw new IonException(e);
        }
    }

    @ParameterizedTest
    @EnumSource(value = Source.class, names = "STREAM_1_BYTE_READS", mode = EnumSource.Mode.EXCLUDE)
    public void skippedValuesLeaveTheReaderOnTheNextValue(Source source) {
        String big = repeat("x", PAGE + 3);
        // Each skipped value holds quotes, comments, escapes and lobs whose contents look like its closing bracket.
        String text = "{a:\"" + big + "\", 'b': {{" + Base64.getEncoder().encodeToString(big.getBytes(StandardCharsets.US_ASCII))
                + "}}, c:[\"]\", ']', '\\']', {{\"]}}\"}}, /* ] */ (\")\" ']') // ]\n], d:{e:" + big + ", x:'}', y:\"}\"/*}*/,"
                + "z:{{\"}\"}} // }\n}, f:\"\\\"}\", g:(')' \")\" /*)*/ // )\n), h:end} 42";

        try (IonReader reader = source.open(text)) {
            assertEquals(IonType.STRUCT, reader.next());
            reader.stepIn();
            List<String> names = new ArrayList<>();
            while (reader.next() != null) {
                names.add(reader.getFieldName()); // values skipped, never read
            }
            reader.stepOut();
            assertEquals(Arrays.asList("a", "b", "c", "d", "f", "g", "h"), names);
            assertEquals(IonType.INT, reader.next());
            assertEquals(42, reader.intValue());
            assertNull(reader.next());
        } catch (IOException e) {
            throw new IonException(e);
        }
    }

    /** Field names read in one pass leave value spans, line numbers and columns as they were. */
    @ParameterizedTest
    @EnumSource(Source.class)
    public void valueSpansAfterFieldNames(Source source) {
        String text = "{abc:1,\n  de  :  \"x\",\r\n f/*c*/:{{AQID}},'q':true,\n\tgh\n:sym}";
        // What the reader reported before the fast paths, pinned as-is: the point is that they stay the same.
        // (The line and column are those of where the reader started looking for the field, not of the value.)
        long[][] expected = {  // offset, line, column of each field's span
            {1, 1, 2},
            {10, 1, 7},
            {24, 2, 13},
            {40, 3, 17},
            {51, 3, 26},
        };
        try (IonReader reader = source.open(text)) {
            reader.next();
            reader.stepIn();
            SpanProvider spans = reader.asFacet(SpanProvider.class);
            for (long[] position : expected) {
                reader.next();
                OffsetSpan offset = spans.currentSpan().asFacet(OffsetSpan.class);
                TextSpan line = spans.currentSpan().asFacet(TextSpan.class);
                if (offset != null) {
                    assertEquals(position[0], offset.getStartOffset(), "offset of " + reader.getFieldName());
                }
                assertEquals(position[1], line.getStartLine(), "line of " + reader.getFieldName());
                assertEquals(position[2], line.getStartColumn(), "column of " + reader.getFieldName());
            }
            assertNull(reader.next());
        } catch (IOException e) {
            throw new IonException(e);
        }
    }

    /** The bulk skips stop at line ends, so line numbers after skipped multi-line values are unchanged. */
    @ParameterizedTest
    @EnumSource(Source.class)
    public void lineNumbersAfterSkippedMultiLineValues(Source source) throws IOException {
        for (String nl : new String[] {"\n", "\r\n", "\r"}) {
            // A list, a struct holding a blob and an escaped line end, a blob and a string, each over two lines.
            String text = "[1," + nl + "2]" + nl
                    + "{a:\"x\"," + nl + " b:{{" + nl + "AQID" + nl + "}}, c:\"y\\" + nl + "z\"}" + nl
                    + "{{ AQID" + nl + "BAUG }}" + nl
                    + "\"p\\" + nl + "q\"" + nl
                    + "end";
            IonType[] types = {IonType.LIST, IonType.STRUCT, IonType.BLOB, IonType.STRING, IonType.SYMBOL};
            long[] lines = {1, 3, 8, 10, 12};
            try (IonReader reader = source.open(text)) {
                SpanProvider spans = reader.asFacet(SpanProvider.class);
                for (int i = 0; i < types.length; i++) {
                    assertEquals(types[i], reader.next());
                    TextSpan span = spans.currentSpan().asFacet(TextSpan.class);
                    assertEquals(lines[i], span.getStartLine(), types[i] + " after line ends " + escape(nl));
                    assertEquals(1, span.getStartColumn(), types[i] + " after line ends " + escape(nl));
                }
                assertNull(reader.next());
            }
        }
    }

    /** A raw line end is still an error in a double-quoted string that is skipped, not read. */
    @ParameterizedTest
    @EnumSource(Source.class)
    public void skippedStringsWithRawLineEndsAreErrors(Source source) {
        for (String text : new String[] {"\"x\ny\" 1", "\"x\ry\" 1", "{a:\"x\ny\", b:1}", "[\"x\ny\"] 1"}) {
            IonException e = assertThrows(IonException.class, () -> {
                try (IonReader reader = source.open(text)) {
                    while (reader.next() != null) {
                        if (reader.getType() == IonType.STRUCT) {
                            reader.stepIn();
                            while (reader.next() != null) {
                                // field values skipped
                            }
                            reader.stepOut();
                        }
                    }
                }
            }, escape(text));
            assertTrue(e.getMessage().contains("at line 2 offset 1"), escape(text) + ": " + e.getMessage());
        }
    }

    private static String escape(String s) {
        return s.replace("\r", "\\r").replace("\n", "\\n");
    }

    @ParameterizedTest
    @EnumSource(Source.class)
    public void identifiersStopAtOperatorCharacters(Source source) {
        IonDatagram values = load(source, "(abc.def a.b x...y z+w)");
        assertEquals(SYSTEM.singleValue("(abc '.' def a '.' b x '...' y z '+' w)"), values.get(0));
    }

    /**
     * Field-name errors keep their exception and message. A keyword used as a field name is reported at the end of
     * the name. (Before the name was read in one pass, the position also included the spaces and comments after it.)
     */
    @Test
    public void fieldNameErrors() {
        String parsing = "com.amazon.ion.impl.IonReaderTextRawX$IonReaderTextParsingException";
        String token = "com.amazon.ion.impl.IonReaderTextRawTokensX$IonReaderTextTokenException";
        String[][] cases = {  // input, exception class, message (or its start, for the last one)
            {"{abc 1}", parsing, "Syntax error at line 1 offset 6: field name must be followed by a colon, not a TOKEN_UNKNOWN_NUMERIC"},
            {"{abc /*c*/ 1}", parsing, "Syntax error at line 1 offset 12: field name must be followed by a colon, not a TOKEN_UNKNOWN_NUMERIC"},
            {"{abc", parsing, "Syntax error at line 1 offset 5: field name must be followed by a colon, not a TOKEN_EOF"},
            {"{true:1}", parsing, "Syntax error at line 1 offset 6: Cannot use unquoted keyword true as a field name"},
            {"{null:1}", parsing, "Syntax error at line 1 offset 6: Cannot use unquoted keyword null as a field name"},
            {"{true :1}", parsing, "Syntax error at line 1 offset 6: Cannot use unquoted keyword true as a field name"},
            {"{nan   :1}", parsing, "Syntax error at line 1 offset 5: Cannot use unquoted keyword nan as a field name"},
            {"{x:{nan /*c*/ : 1}}", parsing, "Syntax error at line 1 offset 8: Cannot use unquoted keyword nan as a field name"},
            {"{true\n:1}", parsing, "Syntax error at line 1 offset 6: Cannot use unquoted keyword true as a field name"},
            {"{false/*c*/:1}", parsing, "Syntax error at line 1 offset 7: Cannot use unquoted keyword false as a field name"},
            {"{nan\r\n  :1}", parsing, "Syntax error at line 1 offset 5: Cannot use unquoted keyword nan as a field name"},
            {"{abc\u00e9:1}", token, "bad character ["},
        };
        for (String[] c : cases) {
            for (Source source : Source.values()) {
                IonException e = assertThrows(IonException.class, () -> load(source, c[0]), source + ": " + escape(c[0]));
                assertEquals(c[1], e.getClass().getName(), source + ": " + escape(c[0]));
                if (c[1].equals(token)) {
                    assertTrue(e.getMessage().startsWith(c[2]) && e.getMessage().endsWith("at line 1 offset 6"),
                            source + ": " + escape(c[0]) + ": " + e.getMessage());
                } else {
                    assertEquals(c[2], e.getMessage(), source + ": " + escape(c[0]));
                }
            }
        }
    }

    /** Non-ASCII characters in a blob are left to the existing decoder, which rejects them as before. */
    @ParameterizedTest
    @EnumSource(Source.class)
    public void nonAsciiInBase64IsRejectedAsBefore(Source source) {
        for (String text : new String[] {"{{AAAA\u00e9AAA}}", "{{AAAA\u4e2dAAA}}", "{{\u00e9AAA}}", "{{AAAAAAAA\u4e2d}}"}) {
            IonException e = assertThrows(IonException.class, () -> {
                try (IonReader reader = source.open(text)) {
                    reader.next();
                    reader.newBytes();
                }
            }, source + ": " + text);
            assertEquals("com.amazon.ion.impl.IonReaderTextRawTokensX$IonReaderTextTokenException", e.getClass().getName(),
                    source + ": " + text + ": " + e);
            assertTrue(e.getMessage().startsWith("invalid character ") && e.getMessage().contains(" encountered in base64 value"),
                    source + ": " + text + ": " + e.getMessage());
        }
    }

    /**
     * After byteSize(), which decodes and caches the whole blob, one getBytes() shorter than the blob returns its
     * first bytes from that cache, and the reader then moves on to the next value. This does not reach the point
     * where bulk decoding hands over to the existing decoder with decoded bytes pending: that is covered by
     * bulkBase64ReturnsWholeGroupsWithinTheRequestedLength. (Reading an uncached text blob in pieces is a separate,
     * pre-existing problem.)
     */
    @ParameterizedTest
    @EnumSource(Source.class)
    public void cachedPrefixReads(Source source) throws IOException {
        byte[] bytes = new byte[200];
        new Random(5).nextBytes(bytes);
        for (String base64 : new String[] {Base64.getEncoder().encodeToString(bytes), Base64.getMimeEncoder().encodeToString(bytes)}) {
            for (int length = 1; length <= 8; length++) {
                try (IonReader reader = source.open("{{" + base64 + "}} 7")) {
                    assertEquals(IonType.BLOB, reader.next());
                    assertEquals(bytes.length, reader.byteSize());
                    byte[] buffer = new byte[length];
                    assertEquals(length, reader.getBytes(buffer, 0, length));
                    assertArrayEquals(Arrays.copyOf(bytes, length), buffer, "length " + length);
                    assertEquals(IonType.INT, reader.next());
                    assertEquals(7, reader.intValue());
                }
            }
        }
    }

    // The stream helpers, called directly: each consumes a whole run, stops where it should, and never refills.

    private static final boolean[] SYMBOL = new boolean[128];
    private static final boolean[] QUOTE = new boolean[128];
    static {
        for (int c = 0; c < 128; c++) {
            SYMBOL[c] = IonTokenConstsX.isValidSymbolCharacter(c);
        }
        QUOTE['"'] = true;
    }

    private static UnifiedInputStreamX[] streams(String text) throws IOException {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        return new UnifiedInputStreamX[] {
            UnifiedInputStreamX.makeStream(bytes), UnifiedInputStreamX.makeStream(new ByteArrayInputStream(bytes)),
            UnifiedInputStreamX.makeStream(text), UnifiedInputStreamX.makeStream(new StringReader(text)),
        };
    }

    @Test
    public void appendAsciiRunCopiesTheAcceptedRun() throws IOException {
        for (UnifiedInputStreamX in : streams("abc123_$-rest")) {
            StringBuilder sb = new StringBuilder("x");
            in.appendAsciiRun(sb, SYMBOL);
            assertEquals("xabc123_$", sb.toString());
            assertEquals('-', in.read());
        }
        for (UnifiedInputStreamX in : streams("ab\u00e9")) {
            StringBuilder sb = new StringBuilder();
            in.appendAsciiRun(sb, SYMBOL);
            assertEquals("ab", sb.toString());
            int next = in.read();
            assertEquals(true, next == 0xC3 || next == 0xE9, "the non-ASCII character is left: " + next);
        }
        StringBuilder sb = new StringBuilder();
        UnifiedInputStreamX.makeStream(new LimitedReads("abcdefghij".getBytes(StandardCharsets.US_ASCII), 4)).appendAsciiRun(sb, SYMBOL);
        assertEquals("abcd", sb.toString(), "stops at the end of the page");
    }

    @Test
    public void skipRunIgnoresNonAscii() throws IOException {
        for (UnifiedInputStreamX in : streams("ab \u00e9{\"rest")) {
            in.skipRun(QUOTE);          // non-ASCII never stops a skipRun
            assertEquals('"', in.read());
        }
    }

    @Test
    public void decodeBase64GroupsDecodesWholeGroupsOnly() throws IOException {
        int[] table = IonReaderTextRawTokensX.BASE64_CHAR_TO_BIN;
        String[][] cases = {  // input, max groups, decoded text, next character
            {"QUJDREVG}", "9", "ABCDEF", "}"},
            {"QUJDREVG}", "1", "ABC", "R"},
            {"QUJDR0g=}", "9", "ABC", "R"},        // '=' is left to the existing decoder
            {"QUJD RE}", "9", "ABC", " "},         // so is whitespace
            {"QUJDRE", "9", "ABC", "R"},           // and a group the page doesn't hold in full
            {"QUJD\u00e9AAA", "9", "ABC", null},   // and non-ASCII
            {"QUJD\u4e2dAAA", "9", "ABC", null},
        };
        for (String[] c : cases) {
            for (UnifiedInputStreamX in : streams(c[0])) {
                byte[] dst = new byte[30];
                int groups = in.decodeBase64Groups(dst, 0, Integer.parseInt(c[1]), table);
                assertEquals(c[2], new String(dst, 0, 3 * groups, StandardCharsets.US_ASCII), escape(c[0]));
                int next = in.read();
                if (c[3] != null) {
                    assertEquals(c[3].charAt(0), next, escape(c[0]));
                } else {
                    assertEquals(true, next >= 0x80, escape(c[0]) + ": the non-ASCII character is left: " + next);
                }
            }
        }
    }

    @Test
    public void bulkBase64ReturnsWholeGroupsWithinTheRequestedLength() throws IOException {
        for (UnifiedInputStreamX in : streams("QUJDREVGR0hJ}}")) {
            IonReaderTextRawTokensX scanner = new IonReaderTextRawTokensX(in);
            byte[] dst = new byte[9];
            assertEquals(6, scanner.read_base64_bytes_bulk(dst, 0, 8));
            assertEquals("ABCDEF", new String(dst, 0, 6, StandardCharsets.US_ASCII));
            assertEquals('G', scanner.read_base64_byte());
        }
    }

    /** Random documents written by the text writer read back equal to what was written, from every source. */
    @ParameterizedTest
    @EnumSource(value = Source.class, names = "STREAM_1_BYTE_READS", mode = EnumSource.Mode.EXCLUDE)
    public void randomDocumentsRoundTrip(Source source) throws IOException {
        Random random = new Random(42);
        for (int i = 0; i < 300; i++) {
            IonDatagram written = SYSTEM.newDatagram();
            for (int n = random.nextInt(5); n >= 0; n--) {
                written.add(randomValue(random, 0));
            }
            for (IonTextWriterBuilder builder : new IonTextWriterBuilder[] {
                    IonTextWriterBuilder.standard(), IonTextWriterBuilder.pretty()}) {
                StringBuilder text = new StringBuilder();
                try (IonWriter writer = builder.build(text)) {
                    written.writeTo(writer);
                }
                assertEquals(written, load(source, text.toString()), text.toString());
            }
        }
    }

    private static IonValue randomValue(Random random, int depth) {
        switch (random.nextInt(depth > 2 ? 6 : 9)) {
            case 0: return SYSTEM.newString(randomText(random));
            case 1: return SYSTEM.newSymbol(random.nextBoolean() ? randomIdentifier(random) : randomText(random));
            case 2: return SYSTEM.newDecimal(new java.math.BigDecimal(random.nextLong()).movePointLeft(random.nextInt(8)));
            case 3: {
                byte[] bytes = new byte[random.nextInt(50) == 0 ? PAGE + random.nextInt(100) : random.nextInt(80)];
                random.nextBytes(bytes);
                return SYSTEM.newBlob(bytes);
            }
            case 4: return SYSTEM.newInt(random.nextLong());
            case 5: return SYSTEM.newBool(random.nextBoolean());
            case 6: {
                IonStruct struct = SYSTEM.newEmptyStruct();
                for (int n = random.nextInt(5); n > 0; n--) {
                    struct.add(random.nextBoolean() ? randomIdentifier(random) : randomText(random), randomValue(random, depth + 1));
                }
                return struct;
            }
            case 7: {
                IonList list = SYSTEM.newEmptyList();
                for (int n = random.nextInt(5); n > 0; n--) {
                    list.add(randomValue(random, depth + 1));
                }
                return list;
            }
            default: {
                IonSexp sexp = SYSTEM.newEmptySexp();
                for (int n = random.nextInt(5); n > 0; n--) {
                    sexp.add(randomValue(random, depth + 1));
                }
                return sexp;
            }
        }
    }

    private static String randomIdentifier(Random random) {
        String start = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_$";
        String rest = start + "0123456789";
        StringBuilder s = new StringBuilder().append(start.charAt(random.nextInt(start.length())));
        for (int n = random.nextInt(random.nextInt(50) == 0 ? PAGE + 100 : 20); n > 0; n--) {
            s.append(rest.charAt(random.nextInt(rest.length())));
        }
        String identifier = s.toString();
        return identifier.equals("true") || identifier.equals("false") || identifier.equals("null")
                || identifier.equals("nan") || identifier.startsWith("$") ? "_" + identifier : identifier;
    }

    private static String randomText(Random random) {
        StringBuilder s = new StringBuilder();
        for (int n = random.nextInt(random.nextInt(50) == 0 ? PAGE + 100 : 60); n > 0; n--) {
            int kind = random.nextInt(30);
            if (kind < 24) {
                s.append((char) (0x20 + random.nextInt(0x5F)));
            } else {
                switch (kind) {
                    case 24: s.append('\n'); break;
                    case 25: s.append('\t'); break;
                    case 26: s.append('é'); break;
                    case 27: s.append("日本"); break;
                    case 28: s.appendCodePoint(0x1F600); break;
                    default: s.append((char) random.nextInt(0x20)); break;
                }
            }
        }
        return s.toString();
    }

    private static String stringOf(IonValue value) {
        return value == null ? null : ((IonText) value).stringValue();
    }

    private static String repeat(String s, int times) {
        StringBuilder out = new StringBuilder(s.length() * times);
        for (int i = 0; i < times; i++) {
            out.append(s);
        }
        return out.toString();
    }
}
