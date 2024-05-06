// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.system;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.bin.DelimitedContainerStrategy;
import com.amazon.ion.impl.bin.SymbolInliningStrategy;

import java.io.OutputStream;

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
public interface IonBinaryWriterBuilder_1_1 {

    /**
     * Gets the catalog to use when building an {@link IonWriter}.
     * The catalog is needed to resolve manually-written imports (not common).
     * By default, this property is null.
     *
     * @see #setCatalog(IonCatalog)
     * @see #withCatalog(IonCatalog)
     */
    IonCatalog getCatalog();

    /**
     * Sets the catalog to use when building an {@link IonWriter}.
     *
     * @param catalog the catalog to use in built writers.
     *  If null, the writer will be unable to resolve manually-written imports
     *  and may throw an exception.
     *
     * @see #getCatalog()
     * @see #withCatalog(IonCatalog)
     *
     * @throws UnsupportedOperationException if this is immutable.
     */
    void setCatalog(IonCatalog catalog);

    /**
     * Declares the catalog to use when building an {@link IonWriter},
     * returning a new mutable builder if this is immutable.
     *
     * @param catalog the catalog to use in built writers.
     *  If null, the writer will be unable to resolve manually-written imports
     *  and may throw an exception.
     *
     * @return this instance, if mutable;
     * otherwise a mutable copy of this instance.
     *
     * @see #getCatalog()
     * @see #setCatalog(IonCatalog)
     */
    IonBinaryWriterBuilder_1_1 withCatalog(IonCatalog catalog);

    /**
     * Gets the imports that will be used to construct the initial local
     * symbol table.
     *
     * @return may be null or empty.
     *
     * @see #setImports(SymbolTable...)
     * @see #withImports(SymbolTable...)
     */
    SymbolTable[] getImports();

    /**
     * Sets the shared symbol tables that will be used to construct the
     * initial local symbol table.
     * <p>
     * If the imports sequence is not null and not empty, the output stream
     * will be bootstrapped with a local symbol table that uses the given
     * {@code imports}.
     *
     * @param imports a sequence of shared symbol tables.
     * The first (and only the first) may be a system table.
     *
     * @see #getImports()
     * @see #withImports(SymbolTable...)
     *
     * @throws UnsupportedOperationException if this is immutable.
     */
    void setImports(SymbolTable... imports);

    /**
     * Declares the imports to use when building an {@link IonWriter},
     * returning a new mutable builder if this is immutable.
     * <p>
     * If the imports sequence is not null and not empty, the output stream
     * will be bootstrapped with a local symbol table that uses the given
     * {@code imports}.
     *
     * @param imports a sequence of shared symbol tables.
     * The first (and only the first) may be a system table.
     *
     * @return this instance, if mutable;
     * otherwise a mutable copy of this instance.
     *
     * @see #getImports()
     * @see #setImports(SymbolTable...)
     */
    IonBinaryWriterBuilder_1_1 withImports(SymbolTable... imports);

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
     * Gets the DelimitedContainerStrategy that will be used to determine which containers will use a delimited encoding
     * vs a length-prefixed encoding.
     *
     * @return the DelimitedContainerStrategy currently configured
     *
     * @see #setDelimitedContainerStrategy(DelimitedContainerStrategy)
     * @see #withDelimitedContainerStrategy(DelimitedContainerStrategy)
     */
    DelimitedContainerStrategy getDelimitedContainerStrategy();

    /**
     * Sets the DelimitedContainerStrategy that will be used to determine which containers will use a delimited encoding
     * vs a length-prefixed encoding.
     *
     * @param delimitedContainerStrategy  If unset, the default strategy of {@link DelimitedContainerStrategy#ALWAYS_PREFIXED}
     *                                    will be used.
     *
     * @see #getDelimitedContainerStrategy()
     * @see #withDelimitedContainerStrategy(DelimitedContainerStrategy)
     */
    void setDelimitedContainerStrategy(DelimitedContainerStrategy delimitedContainerStrategy);

    /**
     * Declares the DelimitedContainerStrategy that will be used to determine which containers will use a delimited
     * encoding vs a length-prefixed encoding.
     *
     * @param delimitedContainerStrategy  If unset, the default strategy of {@link DelimitedContainerStrategy#ALWAYS_PREFIXED}
     *                                    will be used.
     *
     * @return this instance, if mutable;
     * otherwise a mutable copy of this instance.
     *
     * @see #getDelimitedContainerStrategy()
     * @see #setDelimitedContainerStrategy(DelimitedContainerStrategy)
     */
    IonBinaryWriterBuilder_1_1 withDelimitedContainerStrategy(DelimitedContainerStrategy delimitedContainerStrategy);

    /**
     * Gets the SymbolInliningStrategy that will be used to determine which symbols will be written with inline text.
     *
     * @return the SymbolInliningStrategy currently configured
     *
     * @see #setSymbolInliningStrategy(SymbolInliningStrategy)
     * @see #withSymbolInliningStrategy(SymbolInliningStrategy)
     */
    SymbolInliningStrategy getSymbolInliningStrategy();

    /**
     * Sets the SymbolInliningStrategy that will be used to determine which symbols will be written with inline text.
     *
     * @param symbolInliningStrategy if unset, the default of {@link SymbolInliningStrategy#NEVER_INLINE} will be used.
     *
     * @see #getSymbolInliningStrategy()
     * @see #withSymbolInliningStrategy(SymbolInliningStrategy)
     */
    void setSymbolInliningStrategy(SymbolInliningStrategy symbolInliningStrategy);

    /**
     * Declares the SymbolInliningStrategy that will be used to determine which symbols will be written with inline text.
     *
     * @param symbolInliningStrategy if unset, the default of {@link SymbolInliningStrategy#NEVER_INLINE} will be used.
     *
     * @return this instance, if mutable;
     * otherwise a mutable copy of this instance.
     *
     * @see #getSymbolInliningStrategy()
     * @see #withSymbolInliningStrategy(SymbolInliningStrategy)
     */
    IonBinaryWriterBuilder_1_1 withSymbolInliningStrategy(SymbolInliningStrategy symbolInliningStrategy);


    // NOTE: Unlike in Ion 1.0, local symbol table append is always enabled in the Ion 1.1 writers.
    // NOTE: Unlike in Ion 1.0, writing float 32 is always enabled in the Ion 1.1 writers.

    /**
     * Creates a mutable copy of this builder.
     *
     * @return a new builder with the same configuration as {@code this}.
     */
    IonBinaryWriterBuilder_1_1 copy();

    /**
     * Returns an immutable builder configured exactly like this one.
     *
     * @return this instance, if immutable;
     * otherwise an immutable copy of this instance.
     */
    IonBinaryWriterBuilder_1_1 immutable();

    /**
     * Returns a mutable builder configured exactly like this one.
     *
     * @return this instance, if mutable;
     * otherwise a mutable copy of this instance.
     */
    IonBinaryWriterBuilder_1_1 mutable();

    /**
     * Builds a new writer based on this builder's configuration
     * properties.
     *
     * @param out the stream that will receive Ion data.
     * Must not be null.
     *
     * @return a new {@link IonWriter} instance; not {@code null}.
     */
    IonWriter build(OutputStream out);

    // TODO add a build() method that returns a 1.1-specific writer interface, allowing opt-in to new APIs.

}
