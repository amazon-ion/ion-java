// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.ReadOnlyValueException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.SystemSymbols;
import com.amazon.ion.UnknownSymbolException;
import com.amazon.ion.ValueFactory;
import com.amazon.ion.impl.bin.IntList;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.SimpleCatalog;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.amazon.ion.SystemSymbols.IMPORTS_SID;
import static com.amazon.ion.SystemSymbols.ION;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE_SID;
import static com.amazon.ion.SystemSymbols.MAX_ID_SID;
import static com.amazon.ion.SystemSymbols.NAME_SID;
import static com.amazon.ion.SystemSymbols.SYMBOLS_SID;
import static com.amazon.ion.SystemSymbols.VERSION_SID;
import static com.amazon.ion.impl._Private_Utils.safeEquals;

/**
 * An IonCursor capable of application-level parsing of binary Ion streams.
 */
class IonReaderContinuableApplicationBinary extends IonReaderContinuableCoreBinary implements IonReaderContinuableApplication {

    // The UTF-8 encoded bytes representing the text `$ion_symbol_table`.
    private static final byte[] ION_SYMBOL_TABLE_UTF8;

    static {
        ION_SYMBOL_TABLE_UTF8 = SystemSymbols.ION_SYMBOL_TABLE.getBytes(StandardCharsets.UTF_8);
    }

    // An IonCatalog containing zero shared symbol tables.
    private static final IonCatalog EMPTY_CATALOG = new SimpleCatalog();

    // Initial capacity of the ArrayList used to hold the text in the current symbol table.
    private static final int SYMBOLS_LIST_INITIAL_CAPACITY = 128;

    // The imports for Ion 1.0 data with no shared user imports.
    private static final LocalSymbolTableImports ION_1_0_IMPORTS
        = new LocalSymbolTableImports(SharedSymbolTable.getSystemSymbolTable(1));

    // The text representations of the symbol table that is currently in scope, indexed by symbol ID. If the element at
    // a particular index is null, that symbol has unknown text.
    private String[] symbols;

    // The maximum offset into the 'symbols' array that points to a valid local symbol.
    private int localSymbolMaxOffset = -1;

    // The catalog used by the reader to resolve shared symbol table imports.
    private final IonCatalog catalog;

    // Uses the underlying raw reader to read the symbol tables from the stream.
    private final SymbolTableReader symbolTableReader;

    // The shared symbol tables imported by the local symbol table that is currently in scope.
    private LocalSymbolTableImports imports = ION_1_0_IMPORTS;

    // The first lowest local symbol ID in the symbol table.
    private int firstLocalSymbolId = imports.getMaxId() + 1;

    // The cached SymbolTable representation of the current local symbol table. Invalidated whenever a local
    // symbol table is encountered in the stream.
    private SymbolTable cachedReadOnlySymbolTable = null;

    // The cached SymbolTable that was determined to be a superset of the reader's current symbol table during a call
    // to 'isSymbolTableSubsetOf'. This is set to null whenever the reader encounters a new symbol table. Therefore,
    // when non-null, determining whether the reader's symbol table is a subset of a given table is as simple as
    // checking whether that table is the same as 'lastSupersetSymbolTable'.
    private SymbolTable lastSupersetSymbolTable = null;

    // The reusable annotation iterator.
    private final AnnotationSequenceIterator annotationIterator = new AnnotationSequenceIterator();

    // ------

    /**
     * Constructs a new reader from the given byte array.
     * @param builder the builder containing the configuration for the new reader.
     * @param bytes the byte array containing the bytes to read.
     * @param offset the offset into the byte array at which the first byte of Ion data begins.
     * @param length the number of bytes to be read from the byte array.
     */
    IonReaderContinuableApplicationBinary(IonReaderBuilder builder, byte[] bytes, int offset, int length) {
        super(builder.getBufferConfiguration(), bytes, offset, length);
        this.catalog = builder.getCatalog() == null ? EMPTY_CATALOG : builder.getCatalog();
        symbols = new String[SYMBOLS_LIST_INITIAL_CAPACITY];
        symbolTableReader = new SymbolTableReader();
        resetImports();
        registerIvmNotificationConsumer((x, y) -> {
            // Note: for Ion 1.1 support, use the versions to set the proper system symbol table and local symbol table
            // processing logic.
            resetSymbolTable();
            resetImports();
        });
    }

    /**
     * Constructs a new reader from the given input stream.
     * @param builder the builder containing the configuration for the new reader.
     * @param alreadyRead the byte array containing the bytes already read (often the IVM).
     * @param alreadyReadOff the offset into `alreadyRead` at which the first byte that was already read exists.
     * @param alreadyReadLen the number of bytes already read from `alreadyRead`.
     */
    IonReaderContinuableApplicationBinary(final IonReaderBuilder builder, final InputStream inputStream, byte[] alreadyRead, int alreadyReadOff, int alreadyReadLen) {
        super(builder.getBufferConfiguration(), inputStream, alreadyRead, alreadyReadOff, alreadyReadLen);
        this.catalog = builder.getCatalog() == null ? EMPTY_CATALOG : builder.getCatalog();
        symbols = new String[SYMBOLS_LIST_INITIAL_CAPACITY];
        symbolTableReader = new SymbolTableReader();
        resetImports();
        registerIvmNotificationConsumer((x, y) -> {
            // Note: for Ion 1.1 support, use the versions to set the proper system symbol table and local symbol table
            // processing logic.
            resetSymbolTable();
            resetImports();
        });
        registerOversizedValueHandler(
            () -> {
                boolean mightBeSymbolTable = true;
                if (state == State.READING_VALUE) {
                    // The reader is not currently processing a symbol table.
                    if (parent != null || !hasAnnotations) {
                        // Only top-level annotated values can be symbol tables.
                        mightBeSymbolTable = false;
                    } else if (annotationSequenceMarker.startIndex >= 0 && annotationSequenceMarker.endIndex <= limit) {
                        // The annotations on the value are available.
                        if (startsWithIonSymbolTable()) {
                            // The first annotation on the value is $ion_symbol_table. It may be a symbol table if
                            // its type is not yet known (null); it is definitely a symbol table if its type is STRUCT.
                            IonType type = super.getType();
                            mightBeSymbolTable = type == null || type == IonType.STRUCT;
                        } else {
                            // The first annotation on the value is not $ion_symbol_table, so it cannot be a symbol table.
                            mightBeSymbolTable = false;
                        }
                    }
                }
                if (mightBeSymbolTable) {
                    builder.getBufferConfiguration().getOversizedSymbolTableHandler().onOversizedSymbolTable();
                    terminate();
                } else {
                    builder.getBufferConfiguration().getOversizedValueHandler().onOversizedValue();
                }
            }
        );
    }

    /**
     * Reusable iterator over the annotations on the current value.
     */
    private class AnnotationSequenceIterator implements Iterator<String> {

        // All of the annotation SIDs on the current value.
        private IntList annotationSids;
        // The index into `annotationSids` containing the next annotation to be returned.
        private int index = 0;

        void reset() {
            index = 0;
            annotationSids = getAnnotationSidList();
        }

        @Override
        public boolean hasNext() {
            return index < annotationSids.size();
        }

        @Override
        public String next() {
            int sid = annotationSids.get(index);
            String annotation = getSymbol(sid);
            if (annotation == null) {
                throw new UnknownSymbolException(sid);
            }
            index++;
            return annotation;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("This iterator does not support element removal.");
        }
    }

    /**
     * Gets the system symbol table for the Ion version currently active.
     * @return a system SymbolTable.
     */
    private SymbolTable getSystemSymbolTable() {
        // Note: Ion 1.1 currently proposes changes to the system symbol table. If this is finalized, then
        // 'majorVersion' cannot be used to look up the system symbol table; both 'majorVersion' and 'minorVersion'
        // will need to be used.
        return SharedSymbolTable.getSystemSymbolTable(getIonMajorVersion());
    }

    boolean compareSymbolTableImportsArrayToList(SymbolTable[] arr, int arrayLength, List<SymbolTable> list) {
        // Note: the array variant must begin with a system symbol table, while the list variant must not.
        if (arrayLength - 1 != list.size()) {
            return false;
        }
        for (int i = 1; i < arrayLength; i++) {
            // TODO amazon-ion/ion-java/issues/18 Currently, we check imports by their references, which
            //  is overly strict; imports that have different references but the same symbols should pass the check.
            //  However, this is a cheaper check and is compatible with how common Catalog implementations handle
            //  shared tables.
            if (list.get(i - 1) != arr[i]) {
                return false;
            }
        }
        return true;
    }

    boolean compareSymbolsArrayToCollection(String[] arr, int arrayLength, Collection<String> collection) {
        // Precondition: the collection contains at least as many elements as the array.
        Iterator<String> collectionIterator = collection.iterator();
        for (int i = 0; i < arrayLength; i++) {
            if (!safeEquals(arr[i], collectionIterator.next())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Determines whether the symbol table active at the reader's current position is a subset of another symbol table,
     * meaning that every symbol in the reader's symbol table is present and has the same symbol ID in the other
     * table.
     * @param other another symbol table.
     * @return true if the reader's symbol table is a subset of the other table; otherwise, false.
     */
    boolean isSymbolTableSubsetOf(SymbolTable other)
    {
        if (lastSupersetSymbolTable != null) {
            // lastSupersetSymbolTable is reset when the reader's symbol table changes, so we know the reader's symbol
            // table is the same as it was when last compared. This is an optimization that avoids the more expensive
            // comparisons when this method is called repetitively within the same symbol table contexts. This
            // commonly happens during repetitive calls to IonWriter.writeValue(IonReader), which is used directly
            // by users and by the looping wrapper IonWriter.writeValues(IonReader).
            return other == lastSupersetSymbolTable && other.getMaxId() == lastSupersetSymbolTable.getMaxId();
        }

        int numberOfLocalSymbols = localSymbolMaxOffset + 1;
        int maxId = imports.getMaxId() + numberOfLocalSymbols;

        // Note: the first imported table is always the system symbol table.
        boolean isSystemSymbolTable = numberOfLocalSymbols == 0 && imports.getImportedTablesNoCopy().length == 1;
        boolean otherHasPrivateAttributes = other instanceof _Private_LocalSymbolTable;
        _Private_LocalSymbolTable otherLocal = otherHasPrivateAttributes ? (_Private_LocalSymbolTable) other : null;
        if (isSystemSymbolTable) {
            if (other.isSystemTable() && maxId == other.getMaxId()) {
                // Both represent the same system table.
                lastSupersetSymbolTable = other;
                return true;
            }
            // The other symbol table might not literally be the system symbol table, but if it's a local symbol table
            // with zero local symbols and zero imports, that counts.
            if (otherHasPrivateAttributes && otherLocal.getNumberOfLocalSymbols() == 0 && otherLocal.getImportedTablesAsList().isEmpty()) {
                lastSupersetSymbolTable = other;
                return true;
            }
            return false;
        }
        if (!otherHasPrivateAttributes) {
            // The reader's symbol table is not a system symbol table, but the other is. Other cannot be a superset.
            return false;
        }
        if (maxId > otherLocal.getMaxId()) return false;

        // NOTE: the following uses of _Private_LocalSymbolTable utilize knowledge of the implementation used by
        // the binary writer, which has the only known use case for this method. Specifically, we call the interface
        // method variants that return lists instead of arrays because we know this matches the binary writer's symbol
        // table's internal representation and therefore does not require copying. If this method ends up being used
        // for other symbol table implementations, which is unlikely, we should add logic to choose the most efficient
        // variant to call for the particular implementation (such as by adding something like a `boolean usesArrays()`
        // method to the interface).

        SymbolTable[] readerImports = imports.getImportedTablesNoCopy();
        if (!compareSymbolTableImportsArrayToList(readerImports, readerImports.length, otherLocal.getImportedTablesAsList())) {
            return false;
        }

        // Superset extends subset if subset doesn't have any declared symbols.
        if (numberOfLocalSymbols == 0) {
            lastSupersetSymbolTable = other;
            return true;
        }

        // Superset must have same/more declared (local) symbols than subset.
        if (numberOfLocalSymbols > otherLocal.getNumberOfLocalSymbols()) return false;

        Collection<String> otherSymbols = otherLocal.getLocalSymbolsNoCopy();
        if (!compareSymbolsArrayToCollection(symbols, numberOfLocalSymbols, otherSymbols)) {
            return false;
        }

        lastSupersetSymbolTable = other;
        return true;
    }

    /**
     * Read-only snapshot of the local symbol table at the reader's current position.
     */
    private class LocalSymbolTableSnapshot implements _Private_LocalSymbolTable, SymbolTableAsStruct {

        // The system symbol table.
        private final SymbolTable system = IonReaderContinuableApplicationBinary.this.getSystemSymbolTable();

        // The max ID of this local symbol table.
        private final int maxId;

        // The shared symbol tables imported by this local symbol table.
        private final LocalSymbolTableImports importedTables;

        // Map representation of this symbol table. Keys are symbol text; values are the lowest symbol ID that maps
        // to that text.
        final Map<String, Integer> textToId;

        // List representation of this symbol table, indexed by symbol ID.
        final String[] idToText;

        private SymbolTableStructCache structCache = null;

        LocalSymbolTableSnapshot() {
            int importsMaxId = imports.getMaxId();
            int numberOfLocalSymbols = localSymbolMaxOffset + 1;
            // Note: 'imports' is immutable, so a clone is not needed.
            importedTables = imports;
            maxId = importsMaxId + numberOfLocalSymbols;
            idToText = new String[numberOfLocalSymbols];
            System.arraycopy(symbols, 0, idToText, 0, numberOfLocalSymbols);
            // Map with initial size and load factor set so that it will not grow unconditionally when it is filled.
            // Note: using the default load factor of 0.75 results in better lookup performance than using 1.0 and
            // filling the map to capacity.
            textToId = new HashMap<>((int) Math.ceil(numberOfLocalSymbols / 0.75), 0.75f);
            for (int i = 0; i < numberOfLocalSymbols; i++) {
                String symbol = idToText[i];
                if (symbol != null) {
                    textToId.put(symbol, i + importsMaxId + 1);
                }
            }
        }

        @Override
        public String getName() {
            return null;
        }

        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public boolean isLocalTable() {
            return true;
        }

        @Override
        public boolean isSharedTable() {
            return false;
        }

        @Override
        public boolean isSubstitute() {
            return false;
        }

        @Override
        public boolean isSystemTable() {
            return false;
        }

        @Override
        public SymbolTable getSystemSymbolTable() {
            return system;
        }

        @Override
        public String getIonVersionId() {
            return system.getIonVersionId();
        }

        @Override
        public SymbolTable[] getImportedTables() {
            return importedTables.getImportedTables();
        }

        @Override
        public int getImportedMaxId() {
            return importedTables.getMaxId();
        }

        @Override
        public SymbolToken find(String text) {
            SymbolToken token = importedTables.find(text);
            if (token != null) {
                return token;
            }
            Integer sid = textToId.get(text);
            if (sid == null) {
                return null;
            }
            // The following per-call allocation is intentional. When weighed against the alternative of making
            // 'mapView' a 'Map<String, SymbolToken>` instead of a `Map<String, Integer>`, the following points should
            // be considered:
            // 1. A LocalSymbolTableSnapshot is only created when getSymbolTable() is called on the reader. The reader
            // does not use the LocalSymbolTableSnapshot internally. There are two cases when getSymbolTable() would be
            // called: a) when the user calls it, which will basically never happen, and b) when the user uses
            // IonSystem.iterate over the reader, in which case each top-level value holds a reference to the symbol
            // table that was in scope when it occurred. In case a), in addition to rarely being called at all, it
            // would be even rarer for a user to use find() to retrieve each symbol (especially more than once) from the
            // returned symbol table. Case b) may be called more frequently, but it remains equally rare that a user
            // would retrieve each symbol at least once.
            // 2. If we make mapView a Map<String, SymbolToken>, then we are guaranteeing that we will allocate at least
            // one SymbolToken per symbol (because mapView is created in the constructor of LocalSymbolTableSnapshot)
            // even though it's unlikely most will ever be needed.
            return new SymbolTokenImpl(text, sid);
        }

        @Override
        public int findSymbol(String name) {
            Integer sid = importedTables.findSymbol(name);
            if (sid > UNKNOWN_SYMBOL_ID) {
                return sid;
            }
            sid = textToId.get(name);
            if (sid == null) {
                return UNKNOWN_SYMBOL_ID;
            }
            return sid;
        }

        @Override
        public String findKnownSymbol(int id) {
            if (id < 0) {
                throw new IllegalArgumentException("Symbol IDs must be at least 0.");
            }
            if (id > getMaxId()) {
                return null;
            }
            return IonReaderContinuableApplicationBinary.this.getSymbolString(id, importedTables, idToText);
        }

        @Override
        public Iterator<String> iterateDeclaredSymbolNames() {
            return new Iterator<String>() {

                private int index = 0;

                @Override
                public boolean hasNext() {
                    return index < idToText.length;
                }

                @Override
                public String next() {
                    if (index >= idToText.length) {
                        throw new NoSuchElementException();
                    }
                    String symbol = idToText[index];
                    index++;
                    return symbol;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("This iterator does not support element removal.");
                }
            };
        }

        @Override
        public SymbolToken intern(String text) {
            SymbolToken token = find(text);
            if (token != null) {
                return token;
            }
            throw new ReadOnlyValueException();
        }

        @Override
        public int getMaxId() {
            return maxId;
        }

        @Override
        public boolean isReadOnly() {
            return true;
        }

        @Override
        public void makeReadOnly() {
            // The symbol table is already read-only.
        }

        @Override
        public void writeTo(IonWriter writer) throws IOException {
            IonReader reader = new com.amazon.ion.impl.SymbolTableReader(this);
            writer.writeValues(reader);
        }

        @Override
        public String toString() {
            return "(LocalSymbolTable max_id:" + getMaxId() + ')';
        }

        @Override
        public IonStruct getIonRepresentation(ValueFactory valueFactory) {
            if (structCache == null) {
                structCache = new SymbolTableStructCache(this, getImportedTables(), null);
            }
            return structCache.getIonRepresentation(valueFactory);
        }

        @Override
        public _Private_LocalSymbolTable makeCopy() {
            // This is a mutable copy. LocalSymbolTable handles the mutability concerns.
            return new LocalSymbolTable(importedTables, Arrays.asList(idToText));
        }

        @Override
        public SymbolTable[] getImportedTablesNoCopy() {
            return importedTables.getImportedTablesNoCopy();
        }

        @Override
        public List<SymbolTable> getImportedTablesAsList() {
            throw new UnsupportedOperationException("Call getImportedTablesNoCopy() instead.");
        }

        @Override
        public List<String> getLocalSymbolsNoCopy() {
            throw new UnsupportedOperationException("If this is needed, add a no-copy variant that returns an array.");
        }

        @Override
        public int getNumberOfLocalSymbols() {
            return idToText.length;
        }
    }

    /**
     * Reset the local symbol table to the system symbol table.
     */
    private void resetSymbolTable() {
        // The following line is not required for correctness, but it frees the references to the old symbols,
        // potentially allowing them to be garbage collected.
        Arrays.fill(symbols, 0, localSymbolMaxOffset + 1, null);
        localSymbolMaxOffset = -1;
        cachedReadOnlySymbolTable = null;
        lastSupersetSymbolTable = null;
    }

    /**
     * Reset the list of imported shared symbol tables.
     */
    private void resetImports() {
        // Note: when support for the next version of Ion is added, conditionals on 'majorVersion' and 'minorVersion'
        // must be added here.
        imports = ION_1_0_IMPORTS;
        firstLocalSymbolId = imports.getMaxId() + 1;
    }

    /**
     * Restore a symbol table from a previous point in the stream.
     * @param symbolTable the symbol table to restore.
     */
    protected void restoreSymbolTable(SymbolTable symbolTable) {
        if (cachedReadOnlySymbolTable == symbolTable) {
            return;
        }
        if (symbolTable instanceof LocalSymbolTableSnapshot) {
            LocalSymbolTableSnapshot snapshot = (LocalSymbolTableSnapshot) symbolTable;
            cachedReadOnlySymbolTable = snapshot;
            imports = snapshot.importedTables;
            firstLocalSymbolId = imports.getMaxId() + 1;
            // 'symbols' may be smaller than 'idToText' if the span was created from a different reader.
            int shortfall = snapshot.idToText.length - symbols.length;
            if (shortfall > 0) {
                growSymbolsArray(shortfall);
            }
            localSymbolMaxOffset = snapshot.maxId - firstLocalSymbolId;
            System.arraycopy(snapshot.idToText, 0, symbols, 0, snapshot.idToText.length);
            lastSupersetSymbolTable = null;
        } else {
            // Note: this will only happen when `symbolTable` is the system symbol table.
            resetSymbolTable();
            cachedReadOnlySymbolTable = symbolTable;
            resetImports();
            localSymbolMaxOffset = -1;
        }
    }

    /**
     * Creates a shared symbol table import, resolving it from the catalog if possible.
     * @param name the name of the shared symbol table.
     * @param version the version of the shared symbol table.
     * @param maxId the max_id of the shared symbol table. This value takes precedence over the actual max_id for the
     *              shared symbol table at the requested version.
     */
    private SymbolTable createImport(String name, int version, int maxId) {
        SymbolTable shared = catalog.getTable(name, version);
        if (maxId < 0) {
            if (shared == null || version != shared.getVersion()) {
                String message =
                    "Import of shared table "
                        + name
                        + " lacks a valid max_id field, but an exact match was not"
                        + " found in the catalog";
                if (shared != null) {
                    message += " (found version " + shared.getVersion() + ")";
                }
                throw new IonException(message);
            }

            // Exact match is found, but max_id is undefined in import declaration. Set max_id to the largest SID of
            // the matching symbol table.
            maxId = shared.getMaxId();
        }
        if (shared == null) {
            // No match. All symbol IDs that fall within this shared symbol table's range will have unknown text.
            return new SubstituteSymbolTable(name, version, maxId);
        } else if (shared.getMaxId() != maxId || shared.getVersion() != version) {
            // Partial match. If the requested max_id exceeds the actual max_id of the resolved shared symbol table,
            // symbol IDs that exceed the max_id of the resolved shared symbol table will have unknown text.
            return new SubstituteSymbolTable(shared, version, maxId);
        } else {
            // Exact match; the resolved shared symbol table may be used as-is.
            return shared;
        }
    }

    /**
     * Gets the String representation of the given symbol ID. It is the caller's responsibility to ensure that the
     * given symbol ID is within the max ID of the symbol table.
     * @param sid the symbol ID.
     * @param importedSymbols the symbol table's shared symbol table imports.
     * @param localSymbols the symbol table's local symbols.
     * @return a String, which will be null if the requested symbol ID has undefined text.
     */
    private String getSymbolString(int sid, LocalSymbolTableImports importedSymbols, String[] localSymbols) {
        if (sid <= importedSymbols.getMaxId()) {
            return importedSymbols.findKnownSymbol(sid);
        }
        return localSymbols[sid - (importedSymbols.getMaxId() + 1)];
    }

    /**
     * Retrieves the String text for the given symbol ID.
     * @param sid a symbol ID.
     * @return a String.
     */
    String getSymbol(int sid) {
        if (sid < firstLocalSymbolId) {
            return imports.findKnownSymbol(sid);
        }
        int localSymbolOffset = sid - firstLocalSymbolId;
        if (localSymbolOffset > localSymbolMaxOffset) {
            throw new UnknownSymbolException(sid);
        }
        return symbols[localSymbolOffset];
    }

    /**
     * Creates a SymbolToken representation of the given symbol ID.
     * @param sid a symbol ID.
     * @return a SymbolToken.
     */
    private SymbolToken getSymbolToken(int sid) {
        int symbolTableSize = localSymbolMaxOffset + firstLocalSymbolId + 1; // +1 because the max ID is 0-indexed.
        if (sid >= symbolTableSize) {
            throw new UnknownSymbolException(sid);
        }
        String text = getSymbolString(sid, imports, symbols);
        if (text == null && sid >= firstLocalSymbolId) {
            // All symbols with unknown text in the local symbol range are equivalent to symbol zero.
            sid = 0;
        }
        return new SymbolTokenImpl(text, sid);
    }

    private void growSymbolsArray(int shortfall) {
        int newSize = nextPowerOfTwo(symbols.length + shortfall);
        String[] resized = new String[newSize];
        System.arraycopy(symbols, 0, resized, 0, localSymbolMaxOffset + 1);
        symbols = resized;
    }

    /**
     * Uses the underlying raw reader to read the symbol tables from the stream. Capable of resuming if not enough
     * data is currently available to complete the symbol table.
     */
    private class SymbolTableReader {

        private boolean hasSeenImports;
        private boolean hasSeenSymbols;
        private String name = null;
        private int version = -1;
        private int maxId = -1;
        private List<SymbolTable> newImports = null;
        private List<String> newSymbols = null;

        private void resetState() {
            hasSeenImports = false;
            hasSeenSymbols = false;
            newImports = null;
            newSymbols = null;
            name = null;
            version = -1;
            maxId = -1;
        }

        private boolean valueUnavailable() {
            Event event = fillValue();
            return event == Event.NEEDS_DATA || event == Event.NEEDS_INSTRUCTION;
        }

        private void finishReadingSymbolTableStruct() {
            stepOutOfContainer();
            if (!hasSeenImports) {
                resetSymbolTable();
                resetImports();
            }
            if (newSymbols != null) {
                int numberOfNewSymbols = newSymbols.size();
                int numberOfAvailableSlots = symbols.length - (localSymbolMaxOffset + 1);
                int shortfall = numberOfNewSymbols - numberOfAvailableSlots;
                if (shortfall > 0) {
                    growSymbolsArray(shortfall);
                }
                int i = localSymbolMaxOffset;
                for (String newSymbol : newSymbols) {
                    symbols[++i] = newSymbol;
                }
                localSymbolMaxOffset += newSymbols.size();
            }
            // Note: last superset table is reset even if new symbols were simply appended because there's no
            // guarantee those symbols are reflected in the superset table.
            lastSupersetSymbolTable = null;
            state = State.READING_VALUE;
        }

        private void readSymbolTableStructField() {
            if (fieldSid == SYMBOLS_SID) {
                state = State.ON_SYMBOL_TABLE_SYMBOLS;
                if (hasSeenSymbols) {
                    throw new IonException("Symbol table contained multiple symbols fields.");
                }
                hasSeenSymbols = true;
            } else if (fieldSid == IMPORTS_SID) {
                state = State.ON_SYMBOL_TABLE_IMPORTS;
                if (hasSeenImports) {
                    throw new IonException("Symbol table contained multiple imports fields.");
                }
                hasSeenImports = true;
            }
        }

        private void startReadingImportsList() {
            resetImports();
            resetSymbolTable();
            newImports = new ArrayList<>(3);
            newImports.add(getSystemSymbolTable());
            state = State.READING_SYMBOL_TABLE_IMPORTS_LIST;
        }

        private void preparePossibleAppend() {
            if (symbolValueId() != ION_SYMBOL_TABLE_SID) {
                resetSymbolTable();
            }
            state = State.ON_SYMBOL_TABLE_FIELD;
        }

        private void finishReadingImportsList() {
            stepOutOfContainer();
            imports = new LocalSymbolTableImports(newImports);
            firstLocalSymbolId = imports.getMaxId() + 1;
            state = State.ON_SYMBOL_TABLE_FIELD;
        }

        private void startReadingSymbolsList() {
            newSymbols = new ArrayList<>(8);
            state = State.READING_SYMBOL_TABLE_SYMBOLS_LIST;
        }

        private void startReadingSymbol() {
            if (IonReaderContinuableApplicationBinary.super.getType() == IonType.STRING) {
                state = State.READING_SYMBOL_TABLE_SYMBOL;
            } else {
                newSymbols.add(null);
            }
        }

        private void finishReadingSymbol() {
            newSymbols.add(stringValue());
            state = State.READING_SYMBOL_TABLE_SYMBOLS_LIST;
        }

        private void finishReadingSymbolsList() {
            stepOutOfContainer();
            state = State.ON_SYMBOL_TABLE_FIELD;
        }

        private void startReadingImportStruct() {
            name = null;
            version = 1;
            maxId = -1;
            if (IonReaderContinuableApplicationBinary.super.getType() == IonType.STRUCT) {
                stepIntoContainer();
                state = State.READING_SYMBOL_TABLE_IMPORT_STRUCT;
            }
        }

        private void finishReadingImportStruct() {
            stepOutOfContainer();
            state = State.READING_SYMBOL_TABLE_IMPORTS_LIST;
            // Ignore import clauses with malformed name field.
            if (name == null || name.length() == 0 || name.equals(ION)) {
                return;
            }
            newImports.add(createImport(name, version, maxId));
        }

        private void startReadingImportStructField() {
            int fieldId = getFieldId();
            if (fieldId == NAME_SID) {
                state = State.READING_SYMBOL_TABLE_IMPORT_NAME;
            } else if (fieldId == VERSION_SID) {
                state = State.READING_SYMBOL_TABLE_IMPORT_VERSION;
            } else if (fieldId == MAX_ID_SID) {
                state = State.READING_SYMBOL_TABLE_IMPORT_MAX_ID;
            }
        }

        private void readImportName() {
            if (IonReaderContinuableApplicationBinary.super.getType() == IonType.STRING) {
                name = stringValue();
            }
            state = State.READING_SYMBOL_TABLE_IMPORT_STRUCT;
        }

        private void readImportVersion() {
            if (IonReaderContinuableApplicationBinary.super.getType() == IonType.INT && !IonReaderContinuableApplicationBinary.super.isNullValue()) {
                version = Math.max(1, intValue());
            }
            state = State.READING_SYMBOL_TABLE_IMPORT_STRUCT;
        }

        private void readImportMaxId() {
            if (IonReaderContinuableApplicationBinary.super.getType() == IonType.INT && !IonReaderContinuableApplicationBinary.super.isNullValue()) {
                maxId = intValue();
            }
            state = State.READING_SYMBOL_TABLE_IMPORT_STRUCT;
        }

        void readSymbolTable() {
            Event event;
            while (true) {
                switch (state) {
                    case ON_SYMBOL_TABLE_STRUCT:
                        if (Event.NEEDS_DATA == stepIntoContainer()) {
                            return;
                        }
                        state = State.ON_SYMBOL_TABLE_FIELD;
                        break;
                    case ON_SYMBOL_TABLE_FIELD:
                        event = IonReaderContinuableApplicationBinary.super.nextValue();
                        if (Event.NEEDS_DATA == event) {
                            return;
                        }
                        if (event == Event.END_CONTAINER) {
                            finishReadingSymbolTableStruct();
                            return;
                        }
                        readSymbolTableStructField();
                        break;
                    case ON_SYMBOL_TABLE_SYMBOLS:
                        if (IonReaderContinuableApplicationBinary.super.getType() == IonType.LIST) {
                            if (Event.NEEDS_DATA == stepIntoContainer()) {
                                return;
                            }
                            startReadingSymbolsList();
                        } else {
                            state = State.ON_SYMBOL_TABLE_FIELD;
                        }
                        break;
                    case ON_SYMBOL_TABLE_IMPORTS:
                        if (IonReaderContinuableApplicationBinary.super.getType() == IonType.LIST) {
                            if (Event.NEEDS_DATA == stepIntoContainer()) {
                                return;
                            }
                            startReadingImportsList();
                        } else if (IonReaderContinuableApplicationBinary.super.getType() == IonType.SYMBOL) {
                            if (valueUnavailable()) {
                                return;
                            }
                            preparePossibleAppend();
                        } else {
                            state = State.ON_SYMBOL_TABLE_FIELD;
                        }
                        break;
                    case READING_SYMBOL_TABLE_SYMBOLS_LIST:
                        event = IonReaderContinuableApplicationBinary.super.nextValue();
                        if (event == Event.NEEDS_DATA) {
                            return;
                        }
                        if (event == Event.END_CONTAINER) {
                            finishReadingSymbolsList();
                            break;
                        }
                        startReadingSymbol();
                        break;
                    case READING_SYMBOL_TABLE_SYMBOL:
                        if (valueUnavailable()) {
                            return;
                        }
                        finishReadingSymbol();
                        break;
                    case READING_SYMBOL_TABLE_IMPORTS_LIST:
                        event = IonReaderContinuableApplicationBinary.super.nextValue();
                        if (event == Event.NEEDS_DATA) {
                            return;
                        }
                        if (event == Event.END_CONTAINER) {
                            finishReadingImportsList();
                            break;
                        }
                        startReadingImportStruct();
                        break;
                    case READING_SYMBOL_TABLE_IMPORT_STRUCT:
                        event = IonReaderContinuableApplicationBinary.super.nextValue();
                        if (event == Event.NEEDS_DATA) {
                            return;
                        }
                        if (event == Event.END_CONTAINER) {
                            finishReadingImportStruct();
                            break;
                        } else if (event != Event.START_SCALAR) {
                            break;
                        }
                        startReadingImportStructField();
                        break;
                    case READING_SYMBOL_TABLE_IMPORT_NAME:
                        if (valueUnavailable()) {
                            return;
                        }
                        readImportName();
                        break;
                    case READING_SYMBOL_TABLE_IMPORT_VERSION:
                        if (valueUnavailable()) {
                            return;
                        }
                        readImportVersion();
                        break;
                    case READING_SYMBOL_TABLE_IMPORT_MAX_ID:
                        if (valueUnavailable()) {
                            return;
                        }
                        readImportMaxId();
                        break;
                    default: throw new IllegalStateException();
                }
            }
        }
    }

    /**
     * The reader's state. `READING_VALUE` indicates that the reader is reading a user-level value; all other states
     * indicate that the reader is in the middle of reading a symbol table.
     */
    private enum State {
        ON_SYMBOL_TABLE_STRUCT,
        ON_SYMBOL_TABLE_FIELD,
        ON_SYMBOL_TABLE_SYMBOLS,
        READING_SYMBOL_TABLE_SYMBOLS_LIST,
        READING_SYMBOL_TABLE_SYMBOL,
        ON_SYMBOL_TABLE_IMPORTS,
        READING_SYMBOL_TABLE_IMPORTS_LIST,
        READING_SYMBOL_TABLE_IMPORT_STRUCT,
        READING_SYMBOL_TABLE_IMPORT_NAME,
        READING_SYMBOL_TABLE_IMPORT_VERSION,
        READING_SYMBOL_TABLE_IMPORT_MAX_ID,
        READING_VALUE
    }

    // The current state.
    private State state = State.READING_VALUE;

    /**
     * @return true if current value has a sequence of annotations that begins with `$ion_symbol_table`; otherwise,
     *  false.
     */
    boolean startsWithIonSymbolTable() {
        long savedPeekIndex = peekIndex;
        peekIndex = annotationSequenceMarker.startIndex;
        int sid = minorVersion == 0 ? readVarUInt_1_0() : readVarUInt_1_1();
        peekIndex = savedPeekIndex;
        return ION_SYMBOL_TABLE_SID == sid;
    }

    /**
     * @return true if the reader is positioned on a symbol table; otherwise, false.
     */
    private boolean isPositionedOnSymbolTable() {
        return hasAnnotations &&
            super.getType() == IonType.STRUCT &&
            startsWithIonSymbolTable();
    }

    @Override
    public Event nextValue() {
        Event event;
        if (parent == null || state != State.READING_VALUE) {
            while (true) {
                if (state != State.READING_VALUE) {
                    symbolTableReader.readSymbolTable();
                    if (state != State.READING_VALUE) {
                        event = Event.NEEDS_DATA;
                        break;
                    }
                }
                event = super.nextValue();
                if (parent == null && isPositionedOnSymbolTable()) {
                    cachedReadOnlySymbolTable = null;
                    symbolTableReader.resetState();
                    state = State.ON_SYMBOL_TABLE_STRUCT;
                    continue;
                }
                break;
            }
        } else {
            event = super.nextValue();
        }
        return event;
    }

    @Override
    public SymbolTable getSymbolTable() {
        if (cachedReadOnlySymbolTable == null) {
            if (localSymbolMaxOffset < 0 && imports == ION_1_0_IMPORTS) {
                cachedReadOnlySymbolTable = imports.getSystemSymbolTable();
            } else {
                cachedReadOnlySymbolTable = new LocalSymbolTableSnapshot();
            }
        }
        return cachedReadOnlySymbolTable;
    }

    @Override
    public String stringValue() {
        String value;
        IonType type = super.getType();
        if (type == IonType.STRING) {
            value = super.stringValue();
        } else if (type == IonType.SYMBOL) {
            int sid = symbolValueId();
            if (sid < 0) {
                // The raw reader uses this to denote null.symbol.
                return null;
            }
            value = getSymbol(sid);
            if (value == null) {
                throw new UnknownSymbolException(sid);
            }
        } else {
            throw new IllegalStateException("Invalid type requested.");
        }
        return value;
    }

    @Override
    public SymbolToken symbolValue() {
        int sid = symbolValueId();
        if (sid < 0) {
            // The raw reader uses this to denote null.symbol.
            return null;
        }
        return getSymbolToken(sid);
    }

    @Override
    public String[] getTypeAnnotations() {
        if (!hasAnnotations) {
            return _Private_Utils.EMPTY_STRING_ARRAY;
        }
        IntList annotationSids = getAnnotationSidList();
        String[] annotationArray = new String[annotationSids.size()];
        for (int i = 0; i < annotationArray.length; i++) {
            String symbol = getSymbol(annotationSids.get(i));
            if (symbol == null) {
                throw new UnknownSymbolException(annotationSids.get(i));
            }
            annotationArray[i] = symbol;
        }
        return annotationArray;
    }

    @Override
    public SymbolToken[] getTypeAnnotationSymbols() {
        if (!hasAnnotations) {
            return SymbolToken.EMPTY_ARRAY;
        }
        IntList annotationSids = getAnnotationSidList();
        SymbolToken[] annotationArray = new SymbolToken[annotationSids.size()];
        for (int i = 0; i < annotationArray.length; i++) {
            annotationArray[i] = getSymbolToken(annotationSids.get(i));
        }
        return annotationArray;
    }

    private static final Iterator<String> EMPTY_ITERATOR = new Iterator<String>() {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public String next() {
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Cannot remove from an empty iterator.");
        }
    };

    @Override
    public Iterator<String> iterateTypeAnnotations() {
        if (!hasAnnotations) {
            return EMPTY_ITERATOR;
        }
        annotationIterator.reset();
        return annotationIterator;
    }

    @Override
    public String getFieldName() {
        if (fieldSid < 0) {
            return null;
        }
        String fieldName = getSymbol(fieldSid);
        if (fieldName == null) {
            throw new UnknownSymbolException(fieldSid);
        }
        return fieldName;
    }

    @Override
    public SymbolToken getFieldNameSymbol() {
        if (fieldSid < 0) {
            return null;
        }
        return getSymbolToken(fieldSid);
    }

}
