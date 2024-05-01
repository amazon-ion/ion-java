// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.system;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

public class IonBinaryWriterBuilder_1_1_Test extends IonWriterBuilderTestBase<_Private_IonBinaryWriterBuilder_1_1> {

    @Override
    _Private_IonBinaryWriterBuilder_1_1 standard() {
        return _Private_IonBinaryWriterBuilder_1_1.standard();
    }

    @Test
    public void testBlockSize() {
        IonBinaryWriterBuilder_1_1 b = standard();
        assertEquals(_Private_IonBinaryWriterBuilder_1_1.DEFAULT_BLOCK_SIZE, b.getBlockSize());

        b.setBlockSize(42);
        assertEquals(42, b.getBlockSize());

        assertSame(b, b.withBlockSize(4096));
        assertEquals(4096, b.getBlockSize());

        assertThrows(IllegalArgumentException.class, () -> b.setBlockSize(-1));
        assertThrows(IllegalArgumentException.class, () -> b.withBlockSize(Integer.MAX_VALUE));
        assertEquals(4096, b.getBlockSize());

        IonBinaryWriterBuilder_1_1 immutable = b.immutable();

        assertThrows(UnsupportedOperationException.class, () -> immutable.setBlockSize(512));

        IonBinaryWriterBuilder_1_1 mutable = immutable.withBlockSize(16);
        assertNotSame(immutable, mutable);
        assertEquals(16, mutable.getBlockSize());
        assertEquals(4096, immutable.getBlockSize());
    }

    // TODO the following tests are currently skipped because the builder's build() method does not yet return
    //  non-null IonWriter instances.

    @Override
    @Test
    @Ignore
    public void testStandard() {
        // TODO remove this override.
    }

    @Override
    @Test
    @Ignore
    public void testImports() {
        // TODO remove this override.
    }

    @Override
    @Test
    @Ignore
    public void testCustomCatalog() {
        // TODO remove this override.
    }

}
