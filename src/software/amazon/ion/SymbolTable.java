/*
 * Copyright 2007-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion;

import java.io.IOException;
import java.util.Iterator;

/**
 * A symbol table maps symbols between their textual form and an integer ID
 * used in the binary encoding.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 * <p>
 * There are two kinds of symbol tables: <em>shared</em> and <em>local</em>.
 * With that, there are two further distinctions of shared symbol tables:
 * <em>system</em> and <em>substitute</em>.
 *
 * <h2>Notes about Substitute symbol tables</h2>
 * Substitute tables are used when the relevant catalog cannot find an exact
 * match, that is, the catalog cannot find an imported shared symtab with the
 * same name, version and max_id.
 * <p>
 * In order to ensure that we retain the correct import declarations,
 * a substitute table is created, <em>substituting</em> the originally matched
 * shared symtab from the catalog. The substitute table in turns exposes the
 * correct name, version and max_id for any callers that require it, and
 * becomes a delegate of the substituted symtab's interface.
 * <p>
 * <b>Implementations of this interface are safe for use by multiple
 * threads.</b>
 *
 * @see <a href="http://amznlabs.github.io/ion-docs/symbols.html">Ion Symbols page</a>
 */
public interface SymbolTable
{
    /**
     * Indicates that a symbol's integer ID could not be determined.  That's
     * generally the case when constructing value instances that are not yet
     * contained by a datagram.
     */
    public final static int UNKNOWN_SYMBOL_ID = -1;


    /**
     * Gets the unique name of this symbol table.
     *
     * @return the unique name, or {@code null} if {@link #isLocalTable()}.
     */
    public String getName();


    /**
     * Gets the version of this symbol table.
     *
     * @return at least one, or zero if {@link #isLocalTable()}.
     */
    public int getVersion();


    /**
     * Determines whether this symbol table is local, and therefore unnamed
     * and unversioned.
     * <p>
     * If this method returns {@code true}, then both {@link #isSharedTable()}
     * and {@link #isSystemTable()} will return {@code false}.
     */
    public boolean isLocalTable();

    /**
     * Determines whether this symbol table is shared, and therefore named,
     * versioned, and {@linkplain #isReadOnly() read-only}.
     * <p>
     * If this method returns {@code true}, then {@link #isLocalTable()}
     * will return {@code false}.
     */
    public boolean isSharedTable();

    /**
     * Determines whether this instance is substituting for an imported
     * shared table for which no exact match was found in the catalog.
     * Such tables are not authoritative and may not even have any symbol text
     * at all (as is the case when no version of an imported table is found).
     * <p>
     * Substitute tables are always shared, non-system tables.
     *
     */
    public boolean isSubstitute();

    /**
     * Determines whether this symbol table is a system symbol table, and
     * therefore shared, named, versioned, and
     * {@linkplain #isReadOnly() read-only}.
     * <p>
     * If this method returns {@code true}, then {@link #isLocalTable()}
     * will return {@code false} and {@link #isSharedTable()} will return
     * {@code true}.
     */
    public boolean isSystemTable();


    /**
     * Determines whether this symbol table can have symbols added to it.
     * Shared symtabs are always read-only.
     * Local symtabs can also be {@linkplain #makeReadOnly() made read-only}
     * on demand, which enables some optimizations when writing data but will
     * cause failures if new symbols are encountered.
     *
     * @return true if this table is read-only, false if symbols may
     *  be added.
     *
     * @see #makeReadOnly()
     *
     */
    public boolean isReadOnly();


    /**
     * Prevents this symbol table from accepting any more new symbols.
     * Shared symtabs are always read-only.
     * Making a local symtab read-only enables some optimizations when writing
     * data, but will cause failures if new symbols are encountered.
     *
     * @see #isReadOnly()
     *
     */
    public void makeReadOnly();


    /**
     * Gets the system symbol table being used by this local table.
     * <p>
     * If {@link #isSystemTable()} then this method returns {@code this}.
     * Otherwise, if {@link #isSharedTable()} then this method returns
     * {@code null}.
     *
     * @return not <code>null</code>, except for non-system shared tables.
     */
    public SymbolTable getSystemSymbolTable();


    /**
     * Gets the identifier for the Ion version (and thus the system symbol
     * table) used by this table.
     * The version identifier is a string of the form {@code "$ion_X_Y"}.
     *
     * @return the version identifier; or {@code null} for non-system shared
     *  tables.
     */
    public String getIonVersionId();


    /**
     * Gets the sequence of shared symbol tables imported by this (local)
     * symbol table. The result does not include a system table.
     * <p>
     * If this local table imported a shared table for which the relevant
     * {@link IonCatalog} has the same name but different version and/or max_id,
     * then that entry will be a substitute table with the
     * correct version and max_id, wrapping the original shared symbol table
     * that was found.
     * <p>
     * If this local table imported a shared table for which the relevant
     * {@link IonCatalog} has no entry with the same name, but the import
     * declaration has a max_id available, then that entry will
     * be a substitute table with max_id undefined symbols.
     *
     * @return {@code null} if this is a shared or system table, otherwise a
     * non-null but potentially zero-length array of shared tables (but no
     * system table).
     */
    public SymbolTable[] getImportedTables();


    /**
     * Gets the highest symbol id reserved by this table's imports (including
     * system symbols). Any id higher than this value is a local symbol
     * declared by this table. This value is zero for shared symbol tables,
     * since they do not utilize imports.
     */
    public int getImportedMaxId();


    /**
     * Gets the highest symbol id reserved by this table.
     *
     * @return the largest integer such that {@link #findKnownSymbol(int)} could
     * return a non-<code>null</code> result.  Note that there is no promise
     * that it <em>will</em> return a name, only that any larger id will not
     * have a name defined.
     */
    public int getMaxId();


    /**
     * Adds a new symbol to this table, or finds an existing definition of it.
     * <p>
     * The resulting {@link SymbolToken} has the same String instance that
     * was first interned. In order to reduce memory
     * footprint, callers should generally replace their copy of the text with
     * the string in the result.
     * <p>
     * This method will not necessarily return the same instance given the
     * same input.
     *
     * @param text the symbol text to intern.
     *
     * @return the interned symbol, with both text and SID defined; not null.
     *
     * @throws IonException if this symtab {@link #isReadOnly()} and
     * the text isn't already interned.
     *
     * @see #find(String)
     *
     */
    public SymbolToken intern(String text);


    /**
     * Finds a symbol already interned by this table.
     * <p>
     * This method will not necessarily return the same instance given the
     * same input.
     *
     * @param text the symbol text to find.
     *
     * @return the interned symbol, with both text and SID defined;
     *  or {@code null} if it's not already interned.
     *
     * @see #intern(String)
     *
     */
    public SymbolToken find(String text);


    /**
     * Gets the symbol ID associated with a given symbol name.
     *
     * @param name must not be null or empty.
     * @return the id of the requested symbol, or
     * {@link #UNKNOWN_SYMBOL_ID} if it's not defined.
     *
     * @throws NullPointerException if {@code name} is null.
     * @throws EmptySymbolException if {@code name} is empty.
     */
    public int findSymbol(String name);


    /**
     * Gets the interned text for a symbol ID.
     *
     * @param id the requested symbol ID.
     * @return the interned text associated with the symbol ID,
     *  or {@code null} if the text is not known.
     *
     * @throws IllegalArgumentException if {@code id < 1}.
     */
    public String findKnownSymbol(int id);


    /**
     * Creates an iterator that will return all non-imported symbol names, in
     * order of their symbol IDs. The iterator will return {@code null} where
     * there is an undefined sid.
     * <p>
     * The first string returned by the iterator has a symbol ID that is one
     * more than {@link #getImportedMaxId()}, and the last string has symbol
     * ID equals to {@link #getMaxId()}.
     *
     * @return a new iterator.
     */
    public Iterator<String> iterateDeclaredSymbolNames();


    /**
     * Writes an Ion representation of this symbol table.
     *
     * @param writer must not be null.
     * @throws IOException if thrown by the writer.
     */
    public void writeTo(IonWriter writer)
        throws IOException;
}
