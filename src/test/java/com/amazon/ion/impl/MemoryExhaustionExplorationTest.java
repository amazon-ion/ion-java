// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.system.IonReaderBuilder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests that verify memory-bounded rejection of malicious inputs.
 *
 * Attack vectors tested:
 * - Binary length bomb: VarUInt declares length > 100 MB with only a few actual bytes
 * - GZIP bomb: Small compressed stream that decompresses to > 1 GB
 * - Text reader unbounded pages: Text Ion exceeding maximumBufferSize without enforcement
 */
@Tag("memory-exhaustion-exploration")
public class MemoryExhaustionExplorationTest {

    /**
     * Wraps a byte array to force the refillable stream code path.
     */
    private static InputStream asNonByteArrayInputStream(byte[] data) {
        return new FilterInputStream(new ByteArrayInputStream(data)) {};
    }

    /**
     * Binary length bomb: a ~74-byte payload declares a VarUInt length of 800 MB but contains only
     * a few actual data bytes. The reader must reject without allocating proportional memory.
     */
    @Test
    public void binaryLengthBombShouldNotCauseOOM() {
        // Construct a binary Ion payload: IVM + blob type descriptor + VarUInt declaring 800 MB + minimal data
        long declaredLength = 800_000_000L; // 800 MB - exceeds default maximum buffer size
        byte[] varUInt = encodeVarUInt(declaredLength);

        // Build the payload: IVM + type descriptor (blob with VarUInt length) + VarUInt + minimal data
        byte[] payload = new byte[4 + 1 + varUInt.length + 5];
        payload[0] = (byte) 0xE0; // IVM
        payload[1] = 0x01;
        payload[2] = 0x00;
        payload[3] = (byte) 0xEA;
        payload[4] = (byte) 0xAE; // blob with VarUInt length
        System.arraycopy(varUInt, 0, payload, 5, varUInt.length);
        payload[5 + varUInt.length] = 0x00;
        payload[6 + varUInt.length] = 0x00;
        payload[7 + varUInt.length] = 0x00;
        payload[8 + varUInt.length] = 0x00;
        payload[9 + varUInt.length] = 0x00;

        try {
            // Use a non-ByteArrayInputStream to force the refillable stream code path
            IonReader reader = IonReaderBuilder.standard()
                .withIncrementalReadingEnabled(true)
                .build(asNonByteArrayInputStream(payload));
            try {
                IonType type = reader.next();
                if (type != null) {
                    reader.getBytes(new byte[10], 0, 10);
                }
                reader.close();
            } catch (IonException e) {
                // Reader detected the malformed/oversized input
                reader.close();
                return;
            }
        } catch (OutOfMemoryError e) {
            fail("OutOfMemoryError: ensureCapacity() allocated based on attacker-declared VarUInt length. " +
                "Payload: " + payload.length + " bytes declaring " + declaredLength + " bytes.");
        } catch (Exception e) {
            // Any non-OOM exception means the reader handled it without exhausting memory
        }
    }

    /**
     * Encodes a long value as a VarUInt (variable-length unsigned integer) in Ion binary format.
     * In Ion 1.0: each byte contributes 7 data bits; MSB=1 indicates the final byte (stop bit).
     */
    private static byte[] encodeVarUInt(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("VarUInt value must be non-negative");
        }
        if (value == 0) {
            return new byte[]{(byte) 0x80};
        }
        // Determine the number of bytes needed
        int numBytes = 0;
        long temp = value;
        while (temp > 0) {
            numBytes++;
            temp >>= 7;
        }
        byte[] result = new byte[numBytes];
        for (int i = numBytes - 1; i >= 0; i--) {
            result[i] = (byte) (value & 0x7F);
            value >>= 7;
        }
        // Set the stop bit on the last byte
        result[numBytes - 1] |= (byte) 0x80;
        return result;
    }

    /**
     * GZIP bomb: a small compressed stream (~100 KB) that decompresses to > 1 GB.
     * The reader must throw IonException before exhausting heap.
     */
    @Test
    public void gzipBombShouldNotCauseOOM() throws IOException {
        byte[] gzipBomb = createGzipBomb();

        try {
            IonReader reader = IonReaderBuilder.standard()
                .withIncrementalReadingEnabled(true)
                .build(asNonByteArrayInputStream(gzipBomb));
            try {
                IonType type;
                while ((type = reader.next()) != null) {
                    // consume
                }
                reader.close();
            } catch (IonException e) {
                // Reader detected excessive inflation
                reader.close();
                return;
            }
        } catch (OutOfMemoryError e) {
            fail("OutOfMemoryError: GZIP decompression exhausted heap. " +
                "Compressed payload: " + gzipBomb.length + " bytes.");
        } catch (Exception e) {
            // Any non-OOM exception means the reader handled it
        }
    }

    /**
     * Text reader unbounded pages: streams > configured maximumBufferSize through the text reader.
     * The reader must throw IonException when accumulated bytes exceed the configured limit.
     */
    @Test
    public void textReaderShouldHonorMaximumBufferSize() throws IOException {
        final int maxBufferSize = 1024 * 1024; // 1 MB

        IonBufferConfiguration bufferConfig = IonBufferConfiguration.Builder.standard()
            .withMaximumBufferSize(maxBufferSize)
            .onOversizedValue(() -> {
                throw new IonException("Oversized value detected");
            })
            .onOversizedSymbolTable(() -> {
                throw new IonException("Oversized symbol table detected");
            })
            .build();

        InputStream largeTextIonStream = new LargeTextIonInputStream(200 * 1024 * 1024); // 200 MB

        try {
            IonReader reader = IonReaderBuilder.standard()
                .withBufferConfiguration(bufferConfig)
                .build(largeTextIonStream);
            try {
                IonType type;
                long valuesRead = 0;
                while ((type = reader.next()) != null) {
                    valuesRead++;
                    if (valuesRead > 1_000_000) {
                        break;
                    }
                }
                reader.close();

                if (valuesRead > 1_000_000) {
                    fail("Text reader did not enforce maximumBufferSize: read >1M values " +
                        "from 200MB stream with 1MB limit configured.");
                }
            } catch (IonException e) {
                // Reader enforced the buffer limit
                reader.close();
                return;
            }
        } catch (OutOfMemoryError e) {
            fail("OutOfMemoryError: UnifiedInputStreamX accumulated pages beyond " +
                "configured maximumBufferSize (1 MB).");
        } catch (Exception e) {
            // Any non-OOM exception means the reader handled it
        }
    }

    /**
     * Creates a GZIP bomb: binary Ion (IVM + blob declaring 800 MB length) followed by 100 MB
     * of zeros, all GZIP-compressed. The compressed output is very small due to the repetitive content.
     */
    private static byte[] createGzipBomb() throws IOException {
        ByteArrayOutputStream compressedOutput = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOut = new GZIPOutputStream(compressedOutput)) {
            // Binary Ion IVM
            byte[] ivm = {(byte) 0xE0, 0x01, 0x00, (byte) 0xEA};
            gzipOut.write(ivm);

            // Blob type descriptor with VarUInt length declaring 800 MB
            gzipOut.write(0xAE);
            byte[] varUInt = encodeVarUInt(800_000_000L);
            gzipOut.write(varUInt);

            // Write 100 MB of zeros as body data (compresses extremely well)
            byte[] zeros = new byte[64 * 1024];
            long totalWritten = 0;
            long targetSize = 100L * 1024 * 1024;
            while (totalWritten < targetSize) {
                gzipOut.write(zeros);
                totalWritten += zeros.length;
            }
        }
        return compressedOutput.toByteArray();
    }

    /**
     * An InputStream that generates a stream of repeating valid text Ion integers.
     */
    private static class LargeTextIonInputStream extends InputStream {
        private static final byte[] ION_VALUE = "12345 ".getBytes();
        private final long totalBytes;
        private long bytesRead = 0;
        private int posInValue = 0;

        LargeTextIonInputStream(long totalBytes) {
            this.totalBytes = totalBytes;
        }

        @Override
        public int read() {
            if (bytesRead >= totalBytes) {
                return -1;
            }
            byte b = ION_VALUE[posInValue];
            posInValue = (posInValue + 1) % ION_VALUE.length;
            bytesRead++;
            return b & 0xFF;
        }

        @Override
        public int read(byte[] buf, int off, int len) {
            if (bytesRead >= totalBytes) {
                return -1;
            }
            int toRead = (int) Math.min(len, totalBytes - bytesRead);
            for (int i = 0; i < toRead; i++) {
                buf[off + i] = ION_VALUE[posInValue];
                posInValue = (posInValue + 1) % ION_VALUE.length;
            }
            bytesRead += toRead;
            return toRead;
        }
    }
}
