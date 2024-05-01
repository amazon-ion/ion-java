// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.system;

import com.amazon.ion.IonWriter;
import com.amazon.ion.impl._Private_IonConstants;

import java.io.OutputStream;

/**
 * NOT FOR APPLICATION USE.
 */
public class _Private_IonBinaryWriterBuilder_1_1
    extends IonWriterBuilderBase<_Private_IonBinaryWriterBuilder_1_1>
    implements IonBinaryWriterBuilder_1_1
{

    public static final int DEFAULT_BLOCK_SIZE = 32768;
    // A block must be able to hold at least the IVM and the smallest-possible value.
    public static final int MINIMUM_BLOCK_SIZE = 5;
    public static final int MAXIMUM_BLOCK_SIZE = _Private_IonConstants.ARRAY_MAXIMUM_SIZE;

    private int blockSize = DEFAULT_BLOCK_SIZE;

    /**
     * @return a new mutable builder.
     */
    public static _Private_IonBinaryWriterBuilder_1_1 standard()
    {
        return new _Private_IonBinaryWriterBuilder_1_1.Mutable();
    }

    private _Private_IonBinaryWriterBuilder_1_1() {

    }

    private _Private_IonBinaryWriterBuilder_1_1(_Private_IonBinaryWriterBuilder_1_1 that) {
        super(that);
        blockSize = that.blockSize;
    }

    @Override
    public int getBlockSize() {
        return blockSize;
    }

    @Override
    public void setBlockSize(int size) {
        mutationCheck();
        if (size < MINIMUM_BLOCK_SIZE || size > MAXIMUM_BLOCK_SIZE) {
            throw new IllegalArgumentException(
                String.format("Block size must be between %d and %d bytes.", MINIMUM_BLOCK_SIZE, MAXIMUM_BLOCK_SIZE)
            );
        }
        blockSize = size;
    }

    @Override
    public IonBinaryWriterBuilder_1_1 withBlockSize(int size) {
        _Private_IonBinaryWriterBuilder_1_1 b = mutable();
        b.setBlockSize(size);
        return b;
    }

    // Note: The IvmHandling / IvmMinimizing behavior is copied from the Ion 1.0 binary writer (IonBinaryWriterBuilder).

    /**
     * @return always {@link IonWriterBuilder.InitialIvmHandling#ENSURE}.
     */
    @Override
    public InitialIvmHandling getInitialIvmHandling()
    {
        return InitialIvmHandling.ENSURE;
    }

    /**
     * @return always null.
     */
    @Override
    public IvmMinimizing getIvmMinimizing()
    {
        return null;
    }

    @Override
    public IonWriter build(OutputStream out) {
        if (out == null) {
            throw new IllegalArgumentException("Cannot construct a writer with a null OutputStream.");
        }
        return null; // TODO
    }

    // Note: the copy/immutable/mutable pattern is copied from _Private_IonBinaryWriterBuilder.

    @Override
    public final _Private_IonBinaryWriterBuilder_1_1 copy()
    {
        return new Mutable(this);
    }

    @Override
    public _Private_IonBinaryWriterBuilder_1_1 immutable()
    {
        return this;
    }

    @Override
    public _Private_IonBinaryWriterBuilder_1_1 mutable()
    {
        return copy();
    }

    private static final class Mutable
        extends _Private_IonBinaryWriterBuilder_1_1
    {
        private Mutable() { }

        private Mutable(_Private_IonBinaryWriterBuilder_1_1 that)
        {
            super(that);
        }

        @Override
        public _Private_IonBinaryWriterBuilder_1_1 immutable()
        {
            return new _Private_IonBinaryWriterBuilder_1_1(this);
        }

        @Override
        public _Private_IonBinaryWriterBuilder_1_1 mutable()
        {
            return this;
        }

        @Override
        protected void mutationCheck()
        {
        }
    }
}
