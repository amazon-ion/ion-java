// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.system;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.StreamInterceptor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Demonstrates how a StreamInterceptor that recognizes Zstd streams can be plugged into the IonReaderBuilder and
 * IonSystem.
 */
public class ZstdStreamInterceptorTest {

    enum ZstdStreamInterceptor implements StreamInterceptor {
        INSTANCE;

        private static final byte[] ZSTD_HEADER = {(byte) 0x28, (byte) 0xB5, (byte) 0x2F, (byte) 0xFD};

        @Override
        public String formatName() {
            return "Zstd";
        }

        @Override
        public int headerLength() {
            return ZSTD_HEADER.length;
        }

        @Override
        public boolean matchesHeader(byte[] candidate, int offset, int length) {
            if (candidate == null || length < ZSTD_HEADER.length) {
                return false;
            }

            for (int i = 0; i < ZSTD_HEADER.length; i++) {
                if (ZSTD_HEADER[i] != candidate[offset + i]) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public InputStream newInputStream(InputStream interceptedStream) throws IOException {
            return new ZstdInputStream(interceptedStream).setContinuous(true);
        }
    }

    public enum ZstdStream {
        BINARY_STREAM_READER {
            @Override
            IonReader newReader(IonReaderBuilder builder) {
                return builder.build(new ByteArrayInputStream(BINARY_BYTES));
            }
        },
        TEXT_STREAM_READER {
            @Override
            IonReader newReader(IonReaderBuilder builder) {
                return builder.build(new ByteArrayInputStream(TEXT_BYTES));
            }
        },
        BINARY_BYTES_READER {
            @Override
            IonReader newReader(IonReaderBuilder builder) {
                return builder.build(BINARY_BYTES);
            }
        },
        TEXT_BYTES_READER {
            @Override
            IonReader newReader(IonReaderBuilder builder) {
                return builder.build(TEXT_BYTES);
            }
        },
        BINARY_STREAM_SYSTEM {
            @Override
            IonReader newReader(IonReaderBuilder builder) {
                return IonSystemBuilder.standard()
                    .withReaderBuilder(builder)
                    .build()
                    .newReader(new ByteArrayInputStream(BINARY_BYTES));
            }
        },
        TEXT_STREAM_SYSTEM {
            @Override
            IonReader newReader(IonReaderBuilder builder) {
                return IonSystemBuilder.standard()
                    .withReaderBuilder(builder)
                    .build()
                    .newReader(new ByteArrayInputStream(TEXT_BYTES));
            }
        },
        BINARY_BYTES_SYSTEM {
            @Override
            IonReader newReader(IonReaderBuilder builder) {
                return IonSystemBuilder.standard()
                    .withReaderBuilder(builder)
                    .build()
                    .newReader(BINARY_BYTES);
            }
        },
        TEXT_BYTES_SYSTEM {
            @Override
            IonReader newReader(IonReaderBuilder builder) {
                return IonSystemBuilder.standard()
                    .withReaderBuilder(builder)
                    .build()
                    .newReader(TEXT_BYTES);
            }
        };

        abstract IonReader newReader(IonReaderBuilder builder);

        private static byte[] writeCompressedStream(boolean isText) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (IonWriter writer = isText
                ? IonTextWriterBuilder.standard().build(new ZstdOutputStream(bytes))
                : IonBinaryWriterBuilder.standard().build(new ZstdOutputStream(bytes))
            ) {
                writer.writeInt(123);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            return bytes.toByteArray();
        }

        private static final byte[] TEXT_BYTES = writeCompressedStream(true);
        private static final byte[] BINARY_BYTES = writeCompressedStream(false);
    }

    @ParameterizedTest
    @EnumSource(ZstdStream.class)
    public void interceptedViaIonReader(ZstdStream stream) throws IOException {
        IonReaderBuilder builder = IonReaderBuilder.standard().addStreamInterceptor(ZstdStreamInterceptor.INSTANCE);
        try (IonReader reader = stream.newReader(builder)) {
            assertEquals(IonType.INT, reader.next());
            assertEquals(123, reader.intValue());
        }
    }
}
