// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.SystemSymbols;
import com.amazon.ion.Timestamp;
import com.amazon.ion.impl.bin.AbstractIonWriter.WriteValueOptimization;
import com.amazon.ion.impl.bin.IonRawBinaryWriter.PreallocationMode;
import com.amazon.ion.impl.bin.IonRawBinaryWriter.StreamCloseMode;
import com.amazon.ion.impl.bin.IonRawBinaryWriter.StreamFlushMode;
import com.amazon.ion.system.IonReaderBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Regression test for a bug where stale PatchPoint data from a previous stream segment
 * could corrupt output in subsequent segments when the writer is reused.
 */
class IonRawBinaryWriterStalePatchPointTest {

    /**
     * Creates a raw binary writer with the given preallocation mode.
     */
    private IonRawBinaryWriter createWriter(ByteArrayOutputStream out, PreallocationMode mode) throws IOException {
        return new IonRawBinaryWriter(
            BlockAllocatorProviders.basicProvider(),
            11, // Arbitrary (does not affect the test)
            out,
            WriteValueOptimization.NONE,
            StreamCloseMode.NO_CLOSE,
            StreamFlushMode.NO_FLUSH,
            mode,
            true,
            false,
            null
        );
    }

    @ParameterizedTest
    @EnumSource(PreallocationMode.class)
    void stalePatchPointFromLargeContainerCorruptsContainerInNextSegment(PreallocationMode mode) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawBinaryWriter writer = createWriter(out, mode);

        // Segment 1: list with large list content
        // The inner list content exceeds contentMaxLength, causing addPatchPoint to be called.
        // The outer list also exceeds contentMaxLength (since it contains the inner list), so it
        // calls addPatchPoint in its own popContainer, constructing a PatchPoint at index 0.
        writer.writeIonVersionMarker();
        writer.stepIn(IonType.LIST);
        {
            writer.stepIn(IonType.LIST);
            {
                // Force the list to exceed patch thresholds for both PREALLOCATE_2 (16K) and PREALLOCATE_1 (127)
                for (int i = 0; i < (17_000 / 26); i++) {
                    writer.writeString("abcdefghijklmnopqrstuvwxyz");
                }
            }
            writer.stepOut();
        }
        writer.stepOut();
        writer.finish();

        // Segment 2: list with a decimal that has >13 bytes content
        // The decimal's encoding exceeds 13 bytes, triggering patchSingleByteTypedOptimisticValue's addPatchPoint path.
        // The list's total content fits within preallocated space and does not call addPatchPoint itself. The list's
        // patchIndex (assigned by the decimal's ancestor loop) points to the stale PatchPoint from segment 1. Without
        // the fix, finish() applies the stale data and corrupts the output.
        out.reset();
        writer.writeIonVersionMarker();
        writer.stepIn(IonType.LIST);
        {
            writer.writeDecimal(new BigDecimal(new BigInteger("123456789012345678901234567890"), 20));
        }
        writer.stepOut();
        writer.writeInt(123);
        writer.finish();

        byte[] cycle2Bytes = out.toByteArray();

        // Verify we can read the correct data from segment 2
        IonReader reader = IonReaderBuilder.standard().build(cycle2Bytes);
        assertEquals(IonType.LIST, reader.next());
        reader.stepIn();
        assertEquals(IonType.DECIMAL, reader.next());
        assertNotNull(reader.bigDecimalValue());
        assertNull(reader.next());
        reader.stepOut();
        assertEquals(IonType.INT, reader.next());
        assertEquals(123, reader.intValue());
        assertNull(reader.next());

        reader.close();
        writer.close();
    }

    private void writeLocalSymbolTable(IonRawBinaryWriter writer) throws IOException {
        writer.setTypeAnnotationSymbols(SystemSymbols.ION_SYMBOL_TABLE_SID);
        writer.stepIn(IonType.STRUCT);
        {
            writer.setFieldNameSymbol(SystemSymbols.SYMBOLS_SID);
            writer.stepIn(IonType.LIST);
            {
                writer.writeString("sym10");
                writer.writeString("sym11");
                writer.writeString("sym12");
            }
            writer.stepOut();
        }
        writer.stepOut();
    }

    @ParameterizedTest
    @EnumSource(PreallocationMode.class)
    void stalePatchPointFromLargeContainerCorruptsAnnotatedContainerInNextSegment(PreallocationMode mode) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawBinaryWriter writer = createWriter(out, mode);

        // Segment 1: Large annotated nested structure
        writer.writeIonVersionMarker();
        writeLocalSymbolTable(writer);
        writer.setTypeAnnotationSymbols(10);
        writer.stepIn(IonType.STRUCT);
        {
            writer.setFieldNameSymbol(11);
            writer.setTypeAnnotationSymbols(12);
            writer.stepIn(IonType.LIST);
            {
                // Force the list to exceed patch thresholds for both PREALLOCATE_2 (16K) and PREALLOCATE_1 (127)
                for (int i = 0; i < (17_000 / 26); i++) {
                    writer.writeString("abcdefghijklmnopqrstuvwxyz");
                }
            }
            writer.stepOut();
        }
        writer.stepOut();
        writer.finish();

        // Segment 2: Small annotated struct with timestamp >13 bytes
        out.reset();
        writer.writeIonVersionMarker();
        writeLocalSymbolTable(writer);
        writer.setTypeAnnotationSymbols(10);
        writer.stepIn(IonType.STRUCT);
        {
            writer.setFieldNameSymbol(11);
            writer.setTypeAnnotationSymbols(12);
            writer.writeTimestamp(Timestamp.valueOf("2026-01-01T00:00:00.123456789012345678901234567890Z"));
        }
        writer.stepOut();
        writer.finish();

        byte[] cycle2Bytes = out.toByteArray();

        // Verify segment 2
        IonReader reader = IonReaderBuilder.standard().build(cycle2Bytes);
        assertEquals(IonType.STRUCT, reader.next());
        SymbolToken[] annotations = reader.getTypeAnnotationSymbols();
        assertNotNull(annotations);
        assertEquals(1, annotations.length);
        assertEquals("sym10", annotations[0].getText());
        reader.stepIn();
        assertEquals(IonType.TIMESTAMP, reader.next());
        assertEquals("sym11", reader.getFieldName());
        SymbolToken[] fieldAnnotations = reader.getTypeAnnotationSymbols();
        assertEquals(1, fieldAnnotations.length);
        assertEquals("sym12", fieldAnnotations[0].getText());
        assertNotNull(reader.timestampValue());
        reader.stepOut();

        reader.close();
        writer.close();
    }
}
