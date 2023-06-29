// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.SystemSymbols;
import com.amazon.ion.UnknownSymbolException;
import com.amazon.ion.impl.bin.IntList;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.SimpleCatalog;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.amazon.ion.SystemSymbols.IMPORTS_SID;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE_SID;
import static com.amazon.ion.SystemSymbols.MAX_ID_SID;
import static com.amazon.ion.SystemSymbols.NAME_SID;
import static com.amazon.ion.SystemSymbols.SYMBOLS_SID;
import static com.amazon.ion.SystemSymbols.VERSION_SID;

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

    // The max ID of the local symbol table.
    private int localSymbolMaxId = -1;

    // The catalog used by the reader to resolve shared symbol table imports.
    private final IonCatalog catalog;

    // Uses the underlying raw reader to read the symbol tables from the stream.
    private final SymbolTableReader symbolTableReader;

    // The shared symbol tables imported by the local symbol table that is currently in scope.
    private LocalSymbolTableImports imports = ION_1_0_IMPORTS;

    // The first lowest local symbol ID in the symbol table.
    private int firstLocalSymbolId = imports.getMaxId() + 1;

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
                if (
                    state != State.READING_VALUE || // The reader is currently processing a symbol table.
                    (
                        parent == null && hasAnnotations && ( // The reader is on an annotated top-level value.
                            // The value's type is not yet known; it might be a symbol table.
                            super.getType() == null ||
                            // The value's type is known. It can be determined whether it is a symbol table.
                            isPositionedOnSymbolTable()
                        )
                    )
                ) {
                    builder.getBufferConfiguration().getOversizedSymbolTableHandler().onOversizedSymbolTable();
                    terminate();
                } else {
                    builder.getBufferConfiguration().getOversizedValueHandler().onOversizedValue();
                }
            }
        );
    }

    /**
     * Non-reusable iterator over the annotations on the current value. May be iterated even if the reader advances
     * past the current value.
     */
    private class SingleUseAnnotationSequenceIterator implements Iterator<String> {

        // All of the annotation SIDs on the current value.
        private final IntList annotationSids;
        // The index into `annotationSids` containing the next annotation to be returned.
        private int index = 0;

        SingleUseAnnotationSequenceIterator() {
            annotationSids = new IntList(getAnnotationSidList());
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

    /**
     * Reset the local symbol table to the system symbol table.
     */
    private void resetSymbolTable() {
        // The following line is not required for correctness, but it frees the references to the old symbols,
        // potentially allowing them to be garbage collected.
        Arrays.fill(symbols, 0, localSymbolMaxId + 1, null);
        localSymbolMaxId = -1;
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
     * Creates a shared symbol table import, resolving it from the catalog if possible.
     * @param name the name of the shared symbol table.
     * @param version the version of the shared symbol table.
     * @param maxId the max_id of the shared symbol table. This value takes precedence over the actual max_id for the
     *              shared symbol table at the requested version.
     */
    private SymbolTable createImport(String name, int version, int maxId) {
        SymbolTable shared = catalog.getTable(name, version);
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
     * Retrieves the String text for the given symbol ID.
     * @param sid a symbol ID.
     * @return a String.
     */
    String getSymbol(int sid) {
        if (sid < firstLocalSymbolId) {
            return imports.findKnownSymbol(sid);
        }
        int localSid = sid - firstLocalSymbolId;
        if (localSid > localSymbolMaxId) {
            throw new IonException("Symbol ID exceeds the max ID of the symbol table.");
        }
        return symbols[localSid];
    }

    /**
     * Uses the underlying raw reader to read the symbol tables from the stream. Capable of resuming if not enough
     * data is currently available to complete the symbol table.
     */
    private class SymbolTableReader {

        private boolean hasSeenImports;
        private boolean hasSeenSymbols;
        private boolean isAppend;
        private String name = null;
        private int version = -1;
        private int maxId = -1;
        private List<SymbolTable> newImports = null;
        private List<String> newSymbols = null;

        private void resetState() {
            hasSeenImports = false;
            hasSeenSymbols = false;
            isAppend = false;
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

        private void growSymbolsArray(int shortfall) {
            int newSize = nextPowerOfTwo(symbols.length + shortfall);
            String[] resized = new String[newSize];
            System.arraycopy(symbols, 0, resized, 0, localSymbolMaxId + 1);
            symbols = resized;
        }

        private void finishReadingSymbolTableStruct() {
            stepOutOfContainer();
            if (!hasSeenImports) {
                resetSymbolTable();
                resetImports();
            }
            if (newSymbols != null) {
                int numberOfNewSymbols = newSymbols.size();
                int numberOfAvailableSlots = symbols.length - (localSymbolMaxId + 1);
                int shortfall = numberOfNewSymbols - numberOfAvailableSlots;
                if (shortfall > 0) {
                    growSymbolsArray(shortfall);
                }
                int i = localSymbolMaxId;
                for (String newSymbol : newSymbols) {
                    symbols[++i] = newSymbol;
                }
                localSymbolMaxId += newSymbols.size();
            }
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
            if (symbolValueId() == ION_SYMBOL_TABLE_SID) {
                isAppend = true;
            } else {
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
            version = -1;
            maxId = -1;
            if (IonReaderContinuableApplicationBinary.super.getType() == IonType.STRUCT) {
                stepIntoContainer();
                state = State.READING_SYMBOL_TABLE_IMPORT_STRUCT;
            }
        }

        private void finishReadingImportStruct() {
            stepOutOfContainer();
            newImports.add(createImport(name, version, maxId));
            state = State.READING_SYMBOL_TABLE_IMPORTS_LIST;
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
            if (IonReaderContinuableApplicationBinary.super.getType() == IonType.INT) {
                version = intValue();
            }
            state = State.READING_SYMBOL_TABLE_IMPORT_STRUCT;
        }

        private void readImportMaxId() {
            if (IonReaderContinuableApplicationBinary.super.getType() == IonType.INT) {
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
        throw new UnsupportedOperationException("Not yet implemented");
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
        throw new UnsupportedOperationException("Not yet implemented");
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
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private static final Iterator<String> EMPTY_ITERATOR = new Iterator<String>() {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public String next() {
            return null;
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
        return new SingleUseAnnotationSequenceIterator();
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
        throw new UnsupportedOperationException("Not yet implemented");
    }

}
