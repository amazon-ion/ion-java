// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion;

import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks targeting the hot paths modified in CR-266045891:
 *   - {@code IonWriterSystemText.writeSymbolToken} (tryWriteAsIdentifier fast path + IDENTIFIER fallback)
 *   - {@code OutputStreamFastAppendable.appendAscii(char)} / {@code appendAscii(CharSequence)} batch path
 *   - {@code _Private_IonTextAppender.printCodePoints} (NEVER_ESCAPED_ASCII fast path)
 *   - {@code _Private_IonTextAppender.printDecimal} (small-int cache for exponent/scale)
 *   - {@code IonReaderTextRawTokensX.load_double_quoted_string} / {@code load_symbol_identifier}
 *     / {@code skip_over_blob} ASCII fast paths
 *   - {@code IonReaderTextUserX.isIonVersionMarker} (regex -> character-by-character)
 *
 * Results are comparable between the baseline (pre-CR commit 1e84f39b) and CR HEAD
 * by running {@code ./gradlew :jmh -PjmhIncludes=TextHotPathBenchmark}.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class TextHotPathBenchmark {

    private static final IonSystem SYSTEM = IonSystemBuilder.standard().build();

    // === Writer-side fixtures ===

    /** Struct with many plain-identifier field names + ASCII string values. Exercises
     *  writeFieldName -> writeSymbolToken -> tryWriteAsIdentifier, appendAscii(char),
     *  appendAscii(CharSequence), and printCodePoints fast paths. */
    private byte[] symbolHeavyBinary;

    /** Struct containing an identifier with a non-ASCII identifier-part character
     *  that passes symbolVariant() as IDENTIFIER but fails tryWriteAsIdentifier().
     *  This is the regression scenario that motivated the IDENTIFIER-case fix. */
    private byte[] nonAsciiIdentifierBinary;

    /** Big ASCII string; exercises printCodePoints NEVER_ESCAPED_ASCII fast path. */
    private byte[] longAsciiStringBinary;

    /** Decimals with small exponents/scales; exercises the small-int string cache. */
    private byte[] decimalHeavyBinary;

    // === Reader-side fixtures ===

    /** Text with many double-quoted ASCII strings; exercises load_double_quoted_string fast path. */
    private byte[] doubleQuotedTextBytes;

    /** Text with many ASCII symbol identifiers; exercises load_symbol_identifier fast path. */
    private byte[] symbolIdentifierTextBytes;

    /** Text with a large base64 blob; exercises skip_over_blob inline whitespace fast path. */
    private byte[] blobTextBytes;

    /** Text beginning with $ion_1_0 IVM many times over; exercises isIonVersionMarker. */
    private byte[] ivmTextBytes;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        symbolHeavyBinary = buildSymbolHeavyBinary();
        nonAsciiIdentifierBinary = buildNonAsciiIdentifierBinary();
        longAsciiStringBinary = buildLongAsciiStringBinary();
        decimalHeavyBinary = buildDecimalHeavyBinary();

        doubleQuotedTextBytes = buildDoubleQuotedText();
        symbolIdentifierTextBytes = buildSymbolIdentifierText();
        blobTextBytes = buildBlobText();
        ivmTextBytes = buildIvmText();
    }

    // ============ Benchmarks ============

    /** Write pre-loaded symbol-heavy datagram to text. Exercises the IDENTIFIER fast path
     *  the CR adds (and the fix this patch applies). */
    @Benchmark
    public void writeSymbolHeavyText(Blackhole bh) throws IOException {
        IonDatagram dg = SYSTEM.getLoader().load(symbolHeavyBinary);
        ByteArrayOutputStream out = new ByteArrayOutputStream(16 * 1024);
        try (IonWriter w = IonTextWriterBuilder.standard().build(out)) {
            dg.writeTo(w);
        }
        bh.consume(out.size());
    }

    /** Same but for the non-ASCII identifier struct — the scenario where the CR-266045891
     *  bug produced wrong output (quoted instead of unquoted). */
    @Benchmark
    public void writeNonAsciiIdentifierText(Blackhole bh) throws IOException {
        IonDatagram dg = SYSTEM.getLoader().load(nonAsciiIdentifierBinary);
        ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
        try (IonWriter w = IonTextWriterBuilder.standard().build(out)) {
            dg.writeTo(w);
        }
        bh.consume(out.size());
    }

    /** Exercise printCodePoints NEVER_ESCAPED_ASCII fast path. */
    @Benchmark
    public void writeLongAsciiStringText(Blackhole bh) throws IOException {
        IonDatagram dg = SYSTEM.getLoader().load(longAsciiStringBinary);
        ByteArrayOutputStream out = new ByteArrayOutputStream(16 * 1024);
        try (IonWriter w = IonTextWriterBuilder.standard().build(out)) {
            dg.writeTo(w);
        }
        bh.consume(out.size());
    }

    /** Exercise printDecimal small-int cache via many exponent/scale writes. */
    @Benchmark
    public void writeDecimalHeavyText(Blackhole bh) throws IOException {
        IonDatagram dg = SYSTEM.getLoader().load(decimalHeavyBinary);
        ByteArrayOutputStream out = new ByteArrayOutputStream(16 * 1024);
        try (IonWriter w = IonTextWriterBuilder.standard().build(out)) {
            dg.writeTo(w);
        }
        bh.consume(out.size());
    }

    /** Read lots of double-quoted ASCII strings. */
    @Benchmark
    public void readDoubleQuotedStrings(Blackhole bh) throws IOException {
        try (IonReader r = IonReaderBuilder.standard().build(doubleQuotedTextBytes)) {
            while (r.next() != null) {
                bh.consume(r.stringValue());
            }
        }
    }

    /** Read many ASCII symbol identifiers. */
    @Benchmark
    public void readSymbolIdentifiers(Blackhole bh) throws IOException {
        try (IonReader r = IonReaderBuilder.standard().build(symbolIdentifierTextBytes)) {
            while (r.next() != null) {
                bh.consume(r.stringValue());
            }
        }
    }

    /** Read a big blob; exercises skip_over_blob inline whitespace path. */
    @Benchmark
    public void readBlob(Blackhole bh) throws IOException {
        try (IonReader r = IonReaderBuilder.standard().build(blobTextBytes)) {
            while (r.next() != null) {
                bh.consume(r.newBytes());
            }
        }
    }

    /** Read text starting with repeated IVMs; exercises isIonVersionMarker. */
    @Benchmark
    public void readIvmHeavyText(Blackhole bh) throws IOException {
        try (IonReader r = IonReaderBuilder.standard().build(ivmTextBytes)) {
            while (r.next() != null) {
                bh.consume(r.getType());
            }
        }
    }

    // ============ Fixture builders ============

    private static byte[] buildSymbolHeavyBinary() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (IonWriter w = SYSTEM.newBinaryWriter(out)) {
            String[] fields = {
                "customerId", "orderId", "productSku", "quantity", "unitPrice",
                "shippingAddress", "billingAddress", "paymentMethod", "orderStatus",
                "createdAt", "updatedAt", "shippedAt", "deliveredAt", "trackingNumber",
                "carrier", "isGift", "giftMessage", "isPrimeEligible", "taxAmount",
                "totalAmount"
            };
            for (int i = 0; i < 500; i++) {
                w.stepIn(IonType.STRUCT);
                for (String f : fields) {
                    w.setFieldName(f);
                    w.writeString("value_" + i + "_" + f);
                }
                w.stepOut();
            }
        }
        return out.toByteArray();
    }

    private static byte[] buildNonAsciiIdentifierBinary() throws IOException {
        // A symbol that symbolVariant() accepts as IDENTIFIER but tryWriteAsIdentifier
        // rejects because of the > 126 char. Unicode identifier-part char: 'ä' (U+00E4).
        // NOTE: whether symbolVariant accepts this depends on IonTextUtils; if it rejects,
        // this just exercises the quoted fallback cleanly instead of a correctness bug.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (IonWriter w = SYSTEM.newBinaryWriter(out)) {
            for (int i = 0; i < 200; i++) {
                w.stepIn(IonType.STRUCT);
                w.setFieldName("customer\u00e4me");
                w.writeString("value_" + i);
                w.stepOut();
            }
        }
        return out.toByteArray();
    }

    private static byte[] buildLongAsciiStringBinary() throws IOException {
        StringBuilder sb = new StringBuilder(4096);
        for (int i = 0; i < 512; i++) {
            sb.append("The quick brown fox jumps over the lazy dog 0123456789 ");
        }
        String big = sb.toString();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (IonWriter w = SYSTEM.newBinaryWriter(out)) {
            for (int i = 0; i < 50; i++) {
                w.writeString(big);
            }
        }
        return out.toByteArray();
    }

    private static byte[] buildDecimalHeavyBinary() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (IonWriter w = SYSTEM.newBinaryWriter(out)) {
            // Exponents / scales in the 0..19 range hit the small-int cache
            for (int i = 0; i < 2000; i++) {
                w.writeDecimal(new java.math.BigDecimal(
                    java.math.BigInteger.valueOf(123456), i % 20));
            }
        }
        return out.toByteArray();
    }

    private static byte[] buildDoubleQuotedText() {
        StringBuilder sb = new StringBuilder();
        Random r = new Random(42);
        for (int i = 0; i < 2000; i++) {
            sb.append('"');
            int len = 20 + r.nextInt(60);
            for (int j = 0; j < len; j++) {
                // Printable ASCII excluding '"' and '\\'
                char c = (char) ('!' + r.nextInt(93));
                if (c == '"' || c == '\\') c = 'a';
                sb.append(c);
            }
            sb.append("\" ");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] buildSymbolIdentifierText() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 5000; i++) {
            sb.append("symbol_identifier_").append(i).append(' ');
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] buildBlobText() {
        // Big base64-encoded blob (random bytes).
        Random r = new Random(7);
        byte[] raw = new byte[32 * 1024];
        r.nextBytes(raw);
        String b64 = java.util.Base64.getEncoder().encodeToString(raw);
        StringBuilder sb = new StringBuilder(b64.length() + 16);
        // Interleave whitespace to exercise the inline whitespace-skip path.
        sb.append("{{ ");
        for (int i = 0; i < b64.length(); i += 76) {
            sb.append(b64, i, Math.min(i + 76, b64.length())).append('\n');
        }
        sb.append(" }}");
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] buildIvmText() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 500; i++) {
            sb.append("$ion_1_0 value_").append(i).append(' ');
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
}
