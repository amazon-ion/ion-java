// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.system;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.bin.SymbolInliningStrategy;

import java.io.OutputStream;


/**
 * The builder for creating {@link IonWriter}s emitting the 1.1 version of either
 * the text or binary Ion formats.
 * <p>
 * Builders may be configured once and reused to construct multiple
 * objects.
 * <p>
 * <b>Instances of this class are not not safe for use by multiple threads
 * unless they are {@linkplain #immutable() immutable}.</b>
 *
 */
public interface IonWriterBuilder_1_1<T extends IonWriterBuilder_1_1<T>> {

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
    T withCatalog(IonCatalog catalog);

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
    T withImports(SymbolTable... imports);

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
    T withSymbolInliningStrategy(SymbolInliningStrategy symbolInliningStrategy);

    /**
     * Creates a mutable copy of this builder.
     *
     * @return a new builder with the same configuration as {@code this}.
     */
    T copy();

    /**
     * Returns an immutable builder configured exactly like this one.
     *
     * @return this instance, if immutable;
     * otherwise an immutable copy of this instance.
     */
    T immutable();

    /**
     * Returns a mutable builder configured exactly like this one.
     *
     * @return this instance, if mutable;
     * otherwise a mutable copy of this instance.
     */
    T mutable();

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
