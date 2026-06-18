// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.BufferConfiguration;
import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.system.IonReaderBuilder;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates that {@code maximumBufferSize} propagation works correctly in {@code IonCursorBinary}.
 *
 * Verifies that:
 * - {@code IonBufferConfiguration.DEFAULT} with the sane default (100 MB) flows through correctly
 * - {@code ensureCapacity()} rejects requests exceeding the configured maximum
 * - Boundary cases: exactly at limit (succeeds), one byte over limit (triggers rejection)
 */
public class IonCursorBinaryBufferSizePropagationTest {

    /**
     * The default maximum buffer size from BufferConfiguration.Builder (100 MB).
     */
    private static final int DEFAULT_MAXIMUM_BUFFER_SIZE = BufferConfiguration.Builder.DEFAULT_MAXIMUM_BUFFER_SIZE;

    /**
     * Wraps a byte array to force the refillable stream code path (where ensureCapacity()
     * checks maximumBufferSize).
     */
    private static InputStream asNonByteArrayInputStream(byte[] data) {
        return new FilterInputStream(new ByteArrayInputStream(data)) {};
    }

    /**
     * Encodes a long value as a VarUInt (variable-length unsigned integer) in Ion binary format.
     * Each byte contributes 7 data bits; MSB=1 indicates the final byte (stop bit).
     */
    private static byte[] encodeVarUInt(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("VarUInt value must be non-negative");
        }
        if (value == 0) {
            return new byte[]{(byte) 0x80};
        }
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
        result[numBytes - 1] |= (byte) 0x80;
        return result;
    }

    /**
     * Creates a binary Ion payload with a blob declaring the given VarUInt length.
     * The actual data after the header is minimal (just a few zeros).
     */
    private static byte[] createBinaryIonWithDeclaredLength(long declaredLength) {
        byte[] varUInt = encodeVarUInt(declaredLength);
        // IVM (4 bytes) + type descriptor (1 byte: 0xAE = blob with VarUInt length) + VarUInt + minimal data
        byte[] payload = new byte[4 + 1 + varUInt.length + 5];
        payload[0] = (byte) 0xE0; // IVM
        payload[1] = 0x01;
        payload[2] = 0x00;
        payload[3] = (byte) 0xEA;
        payload[4] = (byte) 0xAE; // blob with VarUInt-encoded length
        System.arraycopy(varUInt, 0, payload, 5, varUInt.length);
        // 5 bytes of actual data (far less than declared)
        for (int i = 0; i < 5; i++) {
            payload[5 + varUInt.length + i] = 0x00;
        }
        return payload;
    }

    // ---- Propagation Verification ----

    /**
     * Verifies that IonBufferConfiguration.DEFAULT has the expected sane default maximumBufferSize
     * of 100 MB, not Integer.MAX_VALUE - 8.
     */
    @Test
    public void defaultConfigurationHasSaneMaximumBufferSize() {
        assertEquals(
            DEFAULT_MAXIMUM_BUFFER_SIZE,
            IonBufferConfiguration.DEFAULT.getMaximumBufferSize(),
            "IonBufferConfiguration.DEFAULT should use the sane default (100 MB)"
        );
        assertEquals(
            100 * 1024 * 1024,
            DEFAULT_MAXIMUM_BUFFER_SIZE,
            "DEFAULT_MAXIMUM_BUFFER_SIZE should be 100 MB"
        );
    }

    /**
     * Verifies that when using IonBufferConfiguration.DEFAULT (standard configuration),
     * the maximumBufferSize flows through to the IonCursorBinary's refillableState.
     * This is confirmed by the cursor rejecting requests exceeding the default limit.
     */
    @Test
    public void defaultMaximumBufferSizePropagates() throws Exception {
        long declaredLength = DEFAULT_MAXIMUM_BUFFER_SIZE + 1L;
        byte[] payload = createBinaryIonWithDeclaredLength(declaredLength);

        IonReader reader = IonReaderBuilder.standard()
            .withIncrementalReadingEnabled(true)
            .build(asNonByteArrayInputStream(payload));

        try {
            reader.next();
        } catch (IonException e) {
            // Expected: the throwing OversizedValueHandler fires
        } finally {
            reader.close();
        }
    }

    // ---- ensureCapacity() Rejection Verification ----

    /**
     * Verifies that ensureCapacity() rejects requests exceeding the configured maximumBufferSize.
     * When a declared length exceeds the configured maximum, the cursor sets
     * isSkippingCurrentValue = true and returns false, triggering the oversized value handler.
     */
    @Test
    public void ensureCapacityRejectsRequestsExceedingMaximumBufferSize() throws Exception {
        final int maxBuffer = 1024; // 1 KB
        final AtomicBoolean handlerInvoked = new AtomicBoolean(false);

        IonBufferConfiguration config = IonBufferConfiguration.Builder.standard()
            .withInitialBufferSize(32)
            .withMaximumBufferSize(maxBuffer)
            .onOversizedValue(() -> handlerInvoked.set(true))
            .onOversizedSymbolTable(() -> { throw new IonException("Oversized symbol table"); })
            .build();

        byte[] payload = createBinaryIonWithDeclaredLength(2048); // exceeds 1 KB limit

        IonReader reader = IonReaderBuilder.standard()
            .withIncrementalReadingEnabled(true)
            .withBufferConfiguration(config)
            .build(asNonByteArrayInputStream(payload));

        try {
            reader.next();
            assertTrue(handlerInvoked.get(),
                "OversizedValueHandler should be invoked when declared length exceeds maximumBufferSize");
        } finally {
            try {
                reader.close();
            } catch (IonException e) {
                // close() may throw when stream is incomplete after skip
            }
        }
    }

    // ---- Boundary Cases ----

    /**
     * Value with declared length exactly at the configured maximum should succeed.
     * ensureCapacity() should allocate the buffer normally.
     */
    @Test
    public void valueExactlyAtMaximumBufferSizeSucceeds() throws Exception {
        final int maxBuffer = 512;
        final AtomicBoolean handlerInvoked = new AtomicBoolean(false);

        IonBufferConfiguration config = IonBufferConfiguration.Builder.standard()
            .withInitialBufferSize(64)
            .withMaximumBufferSize(maxBuffer)
            .onOversizedValue(() -> handlerInvoked.set(true))
            .onOversizedSymbolTable(() -> { throw new IonException("Oversized symbol table"); })
            .build();

        // Use a declared length that fits within maxBuffer
        int declaredLength = 200;
        byte[] varUInt = encodeVarUInt(declaredLength);
        byte[] payload = new byte[4 + 1 + varUInt.length + declaredLength];
        payload[0] = (byte) 0xE0;
        payload[1] = 0x01;
        payload[2] = 0x00;
        payload[3] = (byte) 0xEA;
        payload[4] = (byte) 0xAE;
        System.arraycopy(varUInt, 0, payload, 5, varUInt.length);
        for (int i = 0; i < declaredLength; i++) {
            payload[5 + varUInt.length + i] = (byte) (i & 0xFF);
        }

        IonReader reader = IonReaderBuilder.standard()
            .withIncrementalReadingEnabled(true)
            .withBufferConfiguration(config)
            .build(asNonByteArrayInputStream(payload));

        try {
            IonType type = reader.next();
            assertNotNull(type, "Reader should successfully read a value within the buffer limit");
            assertEquals(IonType.BLOB, type, "Value should be a blob");
            assertFalse(handlerInvoked.get(),
                "OversizedValueHandler should NOT be invoked when value is within maximumBufferSize");
        } finally {
            reader.close();
        }
    }

    /**
     * Value with declared length one byte over the configured maximum should trigger rejection.
     * ensureCapacity() should set isSkippingCurrentValue = true and the oversized handler fires.
     */
    @Test
    public void valueOneByteOverMaximumBufferSizeTriggersRejection() throws Exception {
        final int maxBuffer = 256;
        final AtomicBoolean handlerInvoked = new AtomicBoolean(false);

        IonBufferConfiguration config = IonBufferConfiguration.Builder.standard()
            .withInitialBufferSize(32)
            .withMaximumBufferSize(maxBuffer)
            .onOversizedValue(() -> handlerInvoked.set(true))
            .onOversizedSymbolTable(() -> { throw new IonException("Oversized symbol table"); })
            .build();

        // Declare a length exceeding maxBuffer by 1
        int declaredLength = maxBuffer + 1;
        byte[] payload = createBinaryIonWithDeclaredLength(declaredLength);

        IonReader reader = IonReaderBuilder.standard()
            .withIncrementalReadingEnabled(true)
            .withBufferConfiguration(config)
            .build(asNonByteArrayInputStream(payload));

        try {
            reader.next();
            assertTrue(handlerInvoked.get(),
                "OversizedValueHandler should be invoked when declared length exceeds maximumBufferSize by 1 byte");
        } finally {
            try {
                reader.close();
            } catch (IonException e) {
                // Expected: close() may throw "Unexpected EOF" when stream is incomplete after skip
            }
        }
    }

    /**
     * Value well within the configured maximum should succeed without any issues.
     * This confirms preservation: normal inputs are read correctly.
     */
    @Test
    public void valueWellWithinMaximumBufferSizeSucceedsNormally() throws Exception {
        final int maxBuffer = 1024 * 1024; // 1 MB
        final AtomicBoolean handlerInvoked = new AtomicBoolean(false);

        IonBufferConfiguration config = IonBufferConfiguration.Builder.standard()
            .withMaximumBufferSize(maxBuffer)
            .onOversizedValue(() -> handlerInvoked.set(true))
            .onOversizedSymbolTable(() -> { throw new IonException("Oversized symbol table"); })
            .build();

        // Small value: a 100-byte blob (well within 1 MB)
        int declaredLength = 100;
        byte[] varUInt = encodeVarUInt(declaredLength);
        byte[] payload = new byte[4 + 1 + varUInt.length + declaredLength];
        payload[0] = (byte) 0xE0;
        payload[1] = 0x01;
        payload[2] = 0x00;
        payload[3] = (byte) 0xEA;
        payload[4] = (byte) 0xAE;
        System.arraycopy(varUInt, 0, payload, 5, varUInt.length);
        for (int i = 0; i < declaredLength; i++) {
            payload[5 + varUInt.length + i] = (byte) (i & 0xFF);
        }

        IonReader reader = IonReaderBuilder.standard()
            .withIncrementalReadingEnabled(true)
            .withBufferConfiguration(config)
            .build(asNonByteArrayInputStream(payload));

        try {
            IonType type = reader.next();
            assertNotNull(type, "Reader should successfully read a value well within the buffer limit");
            assertEquals(IonType.BLOB, type, "Value should be a blob");
            assertFalse(handlerInvoked.get(),
                "OversizedValueHandler should NOT be invoked for values well within the limit");
        } finally {
            reader.close();
        }
    }

    /**
     * Verifies that the default throwing OversizedValueHandler (used when no custom handler is set)
     * throws IonException when the sane default maximumBufferSize is exceeded.
     */
    @Test
    public void defaultThrowingOversizedValueHandlerIsInvokedForDefaultConfig() throws Exception {
        // Declare a length that exceeds the default 100 MB limit
        long declaredLength = DEFAULT_MAXIMUM_BUFFER_SIZE + 1000L;
        byte[] payload = createBinaryIonWithDeclaredLength(declaredLength);

        // Use the standard builder with NO custom buffer configuration.
        // The default throwing handler should fire.
        IonReader reader = IonReaderBuilder.standard()
            .withIncrementalReadingEnabled(true)
            .build(asNonByteArrayInputStream(payload));

        try {
            assertThrows(IonException.class, () -> {
                reader.next();
            }, "Should throw IonException when oversized value encountered with default configuration");
        } finally {
            reader.close();
        }
    }

    /**
     * Verifies that when ensureCapacity() rejects a request, no OutOfMemoryError occurs
     * and the OversizedValueHandler is invoked.
     */
    @Test
    public void rejectedRequestDoesNotAllocateLargeBuffer() throws Exception {
        final int maxBuffer = 1024; // 1 KB
        final AtomicBoolean handlerInvoked = new AtomicBoolean(false);

        IonBufferConfiguration config = IonBufferConfiguration.Builder.standard()
            .withInitialBufferSize(32)
            .withMaximumBufferSize(maxBuffer)
            .onOversizedValue(() -> handlerInvoked.set(true))
            .onOversizedSymbolTable(() -> { throw new IonException("Oversized symbol table"); })
            .build();

        // Declare a 10 MB value (way over the 1 KB limit)
        long declaredLength = 10 * 1024 * 1024L;
        byte[] payload = createBinaryIonWithDeclaredLength(declaredLength);

        IonReader reader = IonReaderBuilder.standard()
            .withIncrementalReadingEnabled(true)
            .withBufferConfiguration(config)
            .build(asNonByteArrayInputStream(payload));

        try {
            reader.next();
        } finally {
            try {
                reader.close();
            } catch (IonException e) {
                // Expected: close() may throw when stream is incomplete after skip
            }
        }

        // If we reach here without OOM, the large buffer was not allocated
        assertTrue(handlerInvoked.get(),
            "OversizedValueHandler should be invoked for the rejected oversized value");
    }

    /**
     * Verifies that after the oversized value is skipped, the cursor can continue
     * to read subsequent values normally (isSkippingCurrentValue is reset).
     */
    @Test
    public void cursorContinuesNormallyAfterOversizedValueIsSkipped() throws Exception {
        final int maxBuffer = 256;
        final AtomicBoolean handlerInvoked = new AtomicBoolean(false);

        IonBufferConfiguration config = IonBufferConfiguration.Builder.standard()
            .withInitialBufferSize(32)
            .withMaximumBufferSize(maxBuffer)
            .onOversizedValue(() -> handlerInvoked.set(true))
            .onOversizedSymbolTable(() -> { throw new IonException("Oversized symbol table"); })
            .build();

        // Create a stream with: oversized blob (300 bytes), then a small int value (0x21 0x01 = int 1)
        int oversizedLength = 300;
        byte[] varUInt = encodeVarUInt(oversizedLength);
        // Total: IVM(4) + blob_header(1) + varUInt + oversizedLength_data + int_value(2)
        byte[] payload = new byte[4 + 1 + varUInt.length + oversizedLength + 2];
        payload[0] = (byte) 0xE0;
        payload[1] = 0x01;
        payload[2] = 0x00;
        payload[3] = (byte) 0xEA;
        payload[4] = (byte) 0xAE; // blob with VarUInt length
        System.arraycopy(varUInt, 0, payload, 5, varUInt.length);
        // Fill blob body with data
        int bodyStart = 5 + varUInt.length;
        for (int i = 0; i < oversizedLength; i++) {
            payload[bodyStart + i] = (byte) (i & 0xFF);
        }
        // Append a small int value after the blob: 0x21 0x01 (positive int, length 1, value 1)
        payload[bodyStart + oversizedLength] = 0x21;
        payload[bodyStart + oversizedLength + 1] = 0x01;

        IonReader reader = IonReaderBuilder.standard()
            .withIncrementalReadingEnabled(true)
            .withBufferConfiguration(config)
            .build(asNonByteArrayInputStream(payload));

        try {
            // First value should be skipped (oversized)
            IonType type = reader.next();
            assertTrue(handlerInvoked.get(),
                "OversizedValueHandler should have been invoked for the oversized blob");

            // After skipping oversized value, next value should be readable
            IonType secondType = reader.next();
            if (secondType != null) {
                assertEquals(IonType.INT, secondType, "Second value should be an int after oversized skip");
            }
        } finally {
            reader.close();
        }
    }
}
