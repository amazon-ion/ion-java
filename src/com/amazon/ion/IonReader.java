/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.util.Iterator;

/**
 * Reads individual {@link IonValue}s from a stream.  Values returned by the
 * iterator have no container.
 * <p>
 * Users should always call {@link #close} when done using a reader, so it can
 * clean up and close its input source.
 * <p>
 * Implementations of this interface may not be safe for use by multiple
 * threads.
 * @deprecated
 */
@Deprecated
public interface IonReader
    extends Iterator<IonValue>
{
    /**
     * Gets the catalog used to resolve symbol table references.  If this
     * reader encounters an <code>$ion_symbol_table</code> struct,
     * it will be added to this catalog.
     *
     * @return not <code>null</code>.
     */
//    public IonCatalog getCatalog();


    /**
     * Gets the symbol table that will be used for this reader's next value.
     * <p>
     * This may change during reading, since Ion streams may include system
     * values that change the local symbol table.
     *
     * @return the local symbol table; not <code>null</code>.
     */
    public LocalSymbolTable getLocalSymbolTable();


    /**
     * Indicates whether this reader can have its local symbol table
     * dynamically changed.  This is generally true when reading text content,
     * and is false when reading binary content (which contains its own
     * symbol table data).
     *
     * @return false if {@link #setLocalSymbolTable(LocalSymbolTable)} will
     * throw {@link UnsupportedOperationException}.
     *
     * @see #setLocalSymbolTable(LocalSymbolTable)
     */
    public boolean canSetLocalSymbolTable();


    /**
     * Changes the symbol table to be used for future values.  This may not be
     * called between {@link #hasNext()} and {@link #next()}.
     *
     * @param symbols the new symbol table.
     * @throws NullPointerException if <code>symbols</code> is null.
     * @throws UnsupportedOperationException if this reader doesn't support
     * changing the symbol table.
     *
     * @see #canSetLocalSymbolTable()
     */
    public void setLocalSymbolTable(LocalSymbolTable symbols);


    /**
     * Clears the internal binary buffer before reading the next top-level
     * element.  This may be desirable when reading a stream of unknown
     * length (<em>e.g.</em>, when streaming Ion data over a network).
     */
//    public void resetByteBuffer();


    /**
     * Closes the underlying input source, if necessary.
     */
    public void close();
}
