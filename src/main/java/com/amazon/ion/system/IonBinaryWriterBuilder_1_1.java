// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.system;

import com.amazon.ion.IonWriter;
import com.amazon.ion.impl.bin.LengthPrefixStrategy;
import com.amazon.ion.impl.bin.LengthPrefixStrategy;

/**
 * The builder for creating {@link IonWriter}s emitting the 1.1 version of the Ion binary format.
 * <p>
 * Builders may be configured once and reused to construct multiple
 * objects.
 * <p>
 * <b>Instances of this class are not not safe for use by multiple threads
 * unless they are {@linkplain #immutable() immutable}.</b>
 *
 */
public interface IonBinaryWriterBuilder_1_1 extends IonWriterBuilder_1_1<IonBinaryWriterBuilder_1_1> {

    // TODO add auto-flush (see IonBinaryWriterBuilder.withAutoFlushEnabled)
    // TODO consider adding stream-copy optimization (see IonBinaryWriterBuilder withStreamCopyOptimized)
    // TODO consider adding user-configurable length prefix preallocation (see _Private_IonManagedBinaryWriterBuilder.withPaddedLengthPreallocation)
    // TODO consider allowing symbol/macro table block size to be configured separately (see _Private_IonManagedBinaryWriterBuilder.withSymbolsBlockSize)
    // TODO add Ion 1.1-specific configuration

    /**
     * Gets the size of the blocks of memory the writer will allocate to hold encoded bytes between flushes.
     *
     * @return the block size currently configured.
     *
     * @see #setBlockSize(int)
     * @see #withBlockSize(int)
     */
    int getBlockSize();

    /**
     * Sets the size of the blocks of memory the writer will allocate to hold encoded bytes between flushes.
     *
     * @param size the block size in bytes. If unset, the default block size of 32768 bytes will be used.
     *
     * @see #getBlockSize()
     * @see #withBlockSize(int)
     */
    void setBlockSize(int size);

    /**
     * Declares the size of the blocks of memory the writer will allocate to hold encoded bytes between flushes.
     *
     * @param size the block size in bytes. If unset, the default block size of 32768 bytes will be used.
     *
     * @return this instance, if mutable;
     * otherwise a mutable copy of this instance.
     *
     * @see #getBlockSize()
     * @see #setBlockSize(int)
     */
    IonBinaryWriterBuilder_1_1 withBlockSize(int size);

    /**
     * Gets the LengthPrefixStrategy that will be used to determine which containers will use a delimited encoding
     * vs a length-prefixed encoding.
     *
     * @return the LengthPrefixStrategy currently configured
     *
     * @see #setLengthPrefixStrategy(LengthPrefixStrategy)
     * @see #withLengthPrefixStrategy(LengthPrefixStrategy)
     */
    LengthPrefixStrategy getLengthPrefixStrategy();

    /**
     * Sets the LengthPrefixStrategy that will be used to determine which containers will use a delimited encoding
     * vs a length-prefixed encoding.
     *
     * @param lengthPrefixStrategy  If unset, the default strategy of {@link LengthPrefixStrategy#ALWAYS_PREFIXED}
     *                              will be used.
     *
     * @see #getLengthPrefixStrategy()
     * @see #withLengthPrefixStrategy(LengthPrefixStrategy)
     */
    void setLengthPrefixStrategy(LengthPrefixStrategy lengthPrefixStrategy);

    /**
     * Declares the LengthPrefixStrategy that will be used to determine which containers will use a delimited
     * encoding vs a length-prefixed encoding.
     *
     * @param lengthPrefixStrategy  If unset, the default strategy of {@link LengthPrefixStrategy#ALWAYS_PREFIXED}
     *                              will be used.
     *
     * @return this instance, if mutable;
     * otherwise a mutable copy of this instance.
     *
     * @see #getLengthPrefixStrategy()
     * @see #setLengthPrefixStrategy(LengthPrefixStrategy)
     */
    IonBinaryWriterBuilder_1_1 withLengthPrefixStrategy(LengthPrefixStrategy lengthPrefixStrategy);

    // NOTE: Unlike in Ion 1.0, local symbol table append is always enabled in the Ion 1.1 writers.
    // NOTE: Unlike in Ion 1.0, writing float 32 is always enabled in the Ion 1.1 writers.
}
