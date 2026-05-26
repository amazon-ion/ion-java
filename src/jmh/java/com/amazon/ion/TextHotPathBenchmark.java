// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion;

import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks targeting the text reader hot paths:
 *   - {@code IonReaderTextRawTokensX.load_double_quoted_string} / {@code load_symbol_identifier}
 *     / {@code skip_over_blob} ASCII fast paths
 *   - {@code IonReaderTextUserX.isIonVersionMarker} (regex -> character-by-character)
 *
 * Results are comparable between the baseline and CR HEAD
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

    /** Text with many double-quoted ASCII strings; exercises load_double_quoted_string fast path. */
    private byte[] doubleQuotedTextBytes;

    /** Text with many ASCII symbol identifiers; exercises load_symbol_identifier fast path. */
    private byte[] symbolIdentifierTextBytes;

    /** Text with a large base64 blob; exercises skip_over_blob inline whitespace fast path. */
    private byte[] blobTextBytes;

    /** Text beginning with $ion_1_0 IVM many times over; exercises isIonVersionMarker. */
    private byte[] ivmTextBytes;

    @Setup(Level.Trial)
    public void setup() {
        doubleQuotedTextBytes = buildDoubleQuotedText();
        symbolIdentifierTextBytes = buildSymbolIdentifierText();
        blobTextBytes = buildBlobText();
        ivmTextBytes = buildIvmText();
    }

    // ============ Benchmarks ============

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

    private static byte[] buildDoubleQuotedText() {
        StringBuilder sb = new StringBuilder();
        Random r = new Random(42);
        for (int i = 0; i < 2000; i++) {
            sb.append('"');
            int len = 20 + r.nextInt(60);
            for (int j = 0; j < len; j++) {
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
        Random r = new Random(7);
        byte[] raw = new byte[32 * 1024];
        r.nextBytes(raw);
        String b64 = java.util.Base64.getEncoder().encodeToString(raw);
        StringBuilder sb = new StringBuilder(b64.length() + 16);
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
